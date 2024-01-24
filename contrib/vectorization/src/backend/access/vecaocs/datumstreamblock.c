/*-------------------------------------------------------------------------
 *
 * datumstreamblock.c
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 *
 * IDENTIFICATION
 *	    src/backend/access/vecaocs/datumstreamblock.h
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "cdb/cdbaocsam.h"
#include "utils/builtins.h"
#include "utils/datumstreamblock.h"
#include "access/detoast.h"

#include "utils/datumstream_vec.h"

#define WORDNUM(x)	((x) / BITS_PER_BITMAPWORD)
#define BITNUM(x)	((x) % BITS_PER_BITMAPWORD)

static inline bool
is_visible(int offset, const List *visimaps);
static inline void
CopyNextVarlena(DatumStreamBlockRead *dsr, uint8 *datump, VecDesc vecdesc, AttrNumber attno, bool isnull,
		int offset, bool isSnapshotAny);

/*
 * Copy single ctid to the last coldesc if necessary.
 *
 * offset: offset of current tuple from the first row of this time block reading. Reused with visimap.
 */
static inline void
CopyCtid(VecDesc vecdesc, int offset)
{
	ColDesc *ctiddesc = NULL;
	if (!vecdesc->scan_ctid)
		return;

	ctiddesc = vecdesc->coldescs[vecdesc->natts - 1];

	*(((Datum *)ctiddesc->values) + ctiddesc->currows) = offset + vecdesc->cur_ctid;
	ctiddesc->currows++;
}

/*
 * Copy single fix-length Datum to the coldesc.
 *
 * offset: offset of current tuple from the first row of this time block reading.
 */
static inline void
CopyDatum(DatumStreamBlockRead *dsr, VecDesc vecdesc,
		 int attno, bool isnull, int offset, bool isSnapshotAny)
{
	ColDesc *coldesc = vecdesc->coldescs[attno];
	List *visimaps = vecdesc->visbitmaps;

	/* skip invisible tuple */
	if (isSnapshotAny || is_visible(offset, visimaps))
	{
		/* bit is 1 for Null */
		if(!isnull)
		{
			AppendDatumToColDesc(coldesc, dsr->datump);
		}
		else
		{
			AppendNullToColDesc(coldesc);
		}
		CopyCtid(vecdesc, offset);
	}

	if(!isnull)
		dsr->datump += dsr->typeInfo.datumlen;
}

/*
 * In acos '1' in nullbitmap means the datum is null.
 * In arrow the  bitmap is validbitmp, '1' means the datum is valid.
 */
static void inline
CopyWithBitmap(DatumStreamBlockRead *dsr, int32 row_count,VecDesc vecdesc,
		 AttrNumber attno, bool isSnapshotAny)
{
	int spos = dsr->nth + 1;
	uint8 *sbit = dsr->null_bitmap_beginp + spos /8;
	int64 visioffset = vecdesc->visbitmapoffset;
	int i;

	for (i = 0; i < row_count; i++)
	{
		uint8 bit = 1;
		bit <<= spos % 8;
		bit &= *sbit;

		/* bit is 1 for Null */
		CopyDatum(dsr, vecdesc, attno, bit, i + visioffset, isSnapshotAny);

		spos++;
		if (spos % 8 == 0)
			sbit++;
	}

	return;
}

static inline int
CopyVarlena(uint8 *datump, ColDesc *coldesc)
{
	struct varlena *s = (struct varlena *) datump;
	text	   *tunpacked = NULL;

	/* must cast away the const, unfortunately */
	if (VARATT_IS_COMPRESSED(s)
		|| VARATT_IS_EXTERNAL_ONDISK(s)
		|| VARATT_IS_EXTERNAL_INDIRECT(s))
		tunpacked = detoast_attr(s);
	else
		tunpacked = s;

	int		len = VARSIZE_ANY_EXHDR(tunpacked);
	char   *str = VARDATA_ANY(tunpacked);

	/* clear null bitmap for first append. */
	if ((coldesc->currows == 0) && (coldesc->hasnull))
	{
		memset(coldesc->bitmap, 0xff, (max_batch_size + 7) / 8);
		coldesc->hasnull = false;
	}

	if (coldesc->isbpchar)
		len = bpchartruelen(str, len);

	/*  start copy data */
	uint32 *offsets = (uint32*)coldesc->offsets;
	uint32 offset = offsets[coldesc->currows];

	if ((offset + len) > coldesc->values_len)
	{
		g_autoptr(GArrowBuffer) valuesbuffer = NULL;
		g_autoptr(GBytes)       valuebytes = NULL;
		GError                  *error = NULL;

		gsize to_size = 0;
		char *ptmp = NULL;
		gsize size = 0;

		to_size = (offset + len) > (coldesc->values_len * 2) ?
				(offset + len) : (coldesc->values_len * 2);

		valuesbuffer = garrow_buffer_new_default_mem(to_size, &error);

		if (error)
			elog(ERROR, "create new array buffer fail caused by %s", error->message);

		valuebytes = garrow_buffer_get_data(valuesbuffer);
		ptmp = (void *)g_bytes_get_data(valuebytes, &size);
		memcpy(ptmp, (char*)coldesc->values, offset);
		coldesc->values = ptmp;
		coldesc->values_len = to_size;
		garrow_store_ptr(coldesc->value_buffer, valuesbuffer);
	}

	char *dst = (char*)coldesc->values + offset;
	memcpy(dst, (char*)str, len);
	offsets[coldesc->currows + 1] = offset + len;
	coldesc->currows++;

	/* end copy data */
	return len;
}

static void inline
CopyWithBitmapVarlena(DatumStreamBlockRead *dsr, int32 row_count,  VecDesc vecdesc, AttrNumber attno,
		bool isSnapshotAny)
{
	int i;
	int spos = dsr->nth + 1;
	uint8 *sbit = dsr->null_bitmap_beginp + spos /8;
	int64 visioffset = vecdesc->visbitmapoffset;

	for (i = 0; i < row_count; i++)
	{
		uint8 bit = 1;

		bit <<= spos % 8;
		bit &= *sbit;

		/* bit is 1 for Null */
		if(!bit)
		{
			CopyNextVarlena(dsr, dsr->datump, vecdesc, attno, false, i + visioffset, isSnapshotAny);
		}
		else
		{
			CopyNextVarlena(dsr, dsr->datump, vecdesc, attno, true, i + visioffset, isSnapshotAny);
		}

		bit >>= spos % 8;

		spos++;
		if (spos % 8 == 0)
			sbit++;
	}

	return;
}

/*
 * bms_is_member - is X a member of A?
 */
static inline bool
is_visible(int offset, const List *visimaps)
{
	int			wordnum,
				bitnum,
				nth = offset / APPENDONLY_VISIMAP_MAX_RANGE;
	/* visimap is index by rowNum of TupelId which start from 1.
	 * So the offset = 0 is reserved, offset = 1 is the first tuple. */

	if (!visimaps)
		return true;

	Bitmapset *a = list_nth(visimaps, nth);

	/* null visimap, all visible */
	if(!a)
		return true;

	offset -= nth * APPENDONLY_VISIMAP_MAX_RANGE;
	wordnum = WORDNUM(offset);
	bitnum = BITNUM(offset);
	if (wordnum >= a->nwords)
		return true;
	if ((a->words[wordnum] & ((bitmapword) 1 << bitnum)) != 0)
		return false;
	return true;
}

/*
 * Copy single var-lengnth Datum to the coldesc.
 *
 * offset: offset of current tuple from the first row of this time block reading.
 */
static inline void
CopyVarlenaIfVisible(uint8 *datump, VecDesc vecdesc, AttrNumber attno, bool isnull, int offset, bool isSnapshotAny)
{
	ColDesc *coldesc = vecdesc->coldescs[attno];
	List *visimaps = vecdesc->visbitmaps;

	/* skip invisible tuple */
	if (isSnapshotAny || is_visible(offset, visimaps))
	{
		if (!isnull)
			CopyVarlena(datump, coldesc);
		else
			AppendNullToColDesc(coldesc);
		CopyCtid(vecdesc, offset);
	}

}

static inline void
CopyNextVarlena(DatumStreamBlockRead *dsr, uint8 *datump, VecDesc vecdesc, AttrNumber attno, bool isnull,
		int offset, bool isSnapshotAny)
{
	CopyVarlenaIfVisible(datump, vecdesc, attno, isnull, offset, isSnapshotAny);

	if (!isnull)
	{
		int alllen;

		alllen = VARSIZE_ANY((struct varlena *) datump);
		if (dsr->datump != NULL)
		{
			dsr->datump += alllen;
			if (*dsr->datump == 0)
			{
				dsr->datump = (uint8 *) att_align_nominal(dsr->datump, dsr->typeInfo.align);
			}
		}
	}
}

static int
ReadToBuffer(DatumStreamBlockRead *dsr, int32 row_count, VecDesc vecdesc,
		 AttrNumber attno, bool isSnapshotAny)
{
	ColDesc *coldesc = vecdesc->coldescs[attno];
	List *visimaps = vecdesc->visbitmaps;
	int64 visioffset = vecdesc->visbitmapoffset;
	coldesc->block_row_count = row_count;
	if (dsr->typeInfo.datumlen == -1)
	{
		Assert(coldesc->currows + row_count <= max_batch_size);
		if (dsr->has_null)
		{
			CopyWithBitmapVarlena(dsr, row_count, vecdesc, attno, isSnapshotAny);
		}
		else
		{
			int i;

			for (i = 0; i < row_count; i++)
			{
				CopyNextVarlena(dsr, dsr->datump, vecdesc, attno, false, i + visioffset, isSnapshotAny);
			}
		}
	}
	else if (!dsr->typeInfo.byval)
	{
		// not implemented yet.
		Assert(false);
	}
	else
	{
		if (dsr->has_null)
		{
			/* copy data and bitmap */
			Assert(coldesc->currows + row_count <= max_batch_size);
			CopyWithBitmap(dsr, row_count, vecdesc, attno, isSnapshotAny);
			coldesc->hasnull = true;
		}
		/*coldesc->hasnull = false;*/
		else
		{
			int read_size = dsr->typeInfo.datumlen * row_count;

			if ((visimaps) || coldesc->type == GARROW_TYPE_BOOLEAN)
			{
				int i;

				for (i = 0; i < row_count; i++)
					CopyDatum(dsr, vecdesc, attno, false, i + visioffset, isSnapshotAny);
			}
			/* visimaps are null, all visible */
			else
			{
				int i;
				uint8 *cur_read = (uint8 *)(coldesc->values) + coldesc->currows * coldesc->datalen;

				/* copy data*/
				memcpy(cur_read, dsr->datump, read_size);
				coldesc->currows += row_count;
				for (i = 0; i < row_count; i++)
					CopyCtid(vecdesc, i + visioffset);
				dsr->datump += read_size;
			}
		}

	}

	coldesc->rowstoread -= row_count;

	/* buffer if filled, make array */
	if (coldesc->rowstoread == 0)
	{
		if (!dsr->delta_item)
			return 1;
		else
		{
			// not implemented yet.
			Assert(false);
		}
	}
	else if (coldesc->rowstoread < 0)
	{
		Assert(false);
		return -1;
	}
	return 0;
}

/*
 * Get An ArrowArray from DatumStreamBlock by selective positions.
 *
 * Return  0: All Datum is read, need to get new block.
 *         1: This block is not finished yet.
 *         -1: error.
 */
int
DatumStreamBlockRead_Get_By_Pos_Vec(DatumStreamBlockRead *dsr,
									ColDesc *coldesc,
									void *pos,
									int64 pos_base, bool isSnapshotAny)
{
	int ret = 0;

	if (dsr->has_null)
	{
		ret = DatumStreamBlockRead_Get_By_Pos_with_bitmap_vec(
				dsr, coldesc, pos, pos_base, isSnapshotAny);
	}
	else
	{
		ret = DatumStreamBlockRead_Get_By_Pos_without_bitmap_vec(
				dsr, coldesc, pos, pos_base);
	}

	return ret;
}

int
DatumStreamBlockRead_Get_By_Pos_with_bitmap_vec(DatumStreamBlockRead *dsr,
												ColDesc *coldesc,
												void *pos,
												int64 pos_base, bool isSnapshotAny)
{
	int64 low_limit = 0;
	int64 up_limit = 0;
	int64 len = 0;
	int64 i = 0;
	int64 step = 0;
	int64 start = coldesc->lm_row_index;
	const int64 *vals = NULL;
	int spos = dsr->nth + 1;
	uint8 *sbit = dsr->null_bitmap_beginp + (spos / 8);

	/* get up limit and low limit [low_limit, up_limit) */
	low_limit = (int64)dsr->nth + pos_base;
	up_limit = (int64)dsr->logical_row_count + pos_base - 1;

	vals = garrow_int64_array_get_values(GARROW_INT64_ARRAY(pos), &len);

	for (i = start; i < len; i++)
	{
		if (vals[i] <= up_limit)
		{
			uint8 bit = 1;

			/* move to the position */
			step = vals[i] - low_limit;

			while(step > 0)
			{
				bit <<= spos % 8;
				bit &= *sbit;

				if (!bit)
				{
					struct varlena *s = (struct varlena *) dsr->datump;
					int alllen = VARSIZE_ANY(s);

					dsr->datump += alllen;

					if (*dsr->datump == 0)
					{
						dsr->datump = (uint8 *) att_align_nominal(dsr->datump, dsr->typeInfo.align);
					}
				}

				spos += 1;
				step -= 1;

				dsr->nth += 1;
			}

			bit <<= spos % 8;
			bit &= *sbit;

			if (!bit)
			{
				CopyVarlena(dsr->datump, coldesc);
			}
			else
			{
				AppendNullToColDesc(coldesc);
			}

			dsr->nth += 1;
			coldesc->lm_row_index += 1;
		}
		else
		{
			break;
		}
	}

	return (dsr->nth < (dsr->logical_row_count - 1)) ? 1 : 0;
}


int
DatumStreamBlockRead_Get_By_Pos_without_bitmap_vec(DatumStreamBlockRead *dsr,
												   ColDesc *coldesc,
												   void *pos,
												   int64 pos_base)
{
	int64 low_limit = 0;
	int64 up_limit = 0;
	int64 len = 0;
	int64 i = 0;
	int64 step = 0;
	int64 start = coldesc->lm_row_index;
	const int64 *vals = NULL;

	/* get up limit and low limit [low_limit, up_limit) */
	low_limit = (int64)dsr->nth + pos_base;
	up_limit = (int64)dsr->logical_row_count + pos_base - 1;

	vals = garrow_int64_array_get_values(GARROW_INT64_ARRAY(pos), &len);

	for (i = start; i < len; i++)
	{
		if (vals[i] < up_limit)
		{
			if (dsr->typeInfo.datumlen == -1)
			{
				/* move to the position */
				step = vals[i] - ((int64)dsr->nth + pos_base);
				while(step > 0)
				{
					struct varlena *s = (struct varlena *) dsr->datump;
					int alllen = VARSIZE_ANY(s);

					dsr->datump += alllen;

					if (*dsr->datump == 0)
					{
						dsr->datump = (uint8 *) att_align_nominal(dsr->datump, dsr->typeInfo.align);
					}

					dsr->nth += 1;
					step--;
				}

				/* moved and get value */
				CopyVarlena(dsr->datump, coldesc);
				low_limit = vals[i] + 1;
			}
			else if (!dsr->typeInfo.byval)
			{
				// not implemented yet.
				Assert(false);
			}
			else
			{
				step = vals[i] - (-1 + pos_base);
				/* move to the exact position */
				int move_size = step * coldesc->datalen;
				uint8 *p = dsr->datump + move_size;
				memcpy((uint8*)coldesc->values, p, coldesc->datalen);
				coldesc->values = (char*)coldesc->values + coldesc->datalen;
				coldesc->currows++;
			}

			dsr->nth += 1;
			coldesc->lm_row_index += 1;

		}
		else
		{
			dsr->nth = dsr->logical_row_count - 1;
			break;
		}
	}

	return (dsr->nth < (dsr->logical_row_count - 1)) ? 1 : 0;
}

/* [start, end) */
void
DatumStreamBlockRead_fill_pos(ColDesc *coldesc, int start_idx, int64 start_pos, int64 end_pos)
{
	int64 *p = coldesc->pos_values;
	int i = 0;
	int idx = start_idx;

	for (i = start_pos; i < end_pos; i++)
	{
		p[idx] = i;
		idx += 1;
	}

	return;
}

/*
 * Get An ArrowArray from DatumStreamBlock.
 *
 * Return  0: All Datum is read, need to get new block.
 *         1: This block is not finished yet.
 *         -1: error.
 */
int
DatumStreamBlockRead_Get_Vec(DatumStreamBlockRead *dsr, VecDesc vecdesc,
							 AttrNumber attno, int64 pos_base, bool isSnapshotAny)
{
	ColDesc *coldesc = vecdesc->coldescs[attno];
	/* dsr->nth start from -1. */
	int   last = dsr->nth + coldesc->rowstoread;
	int   idx = coldesc->currows;
	int   ret = 0;

	int64 start = dsr->nth + pos_base;
	int64 end = 0;

	/* exactly read to block end*/
	if (last == dsr->logical_row_count - 1)
	{
		if(ReadToBuffer(dsr, coldesc->rowstoread, vecdesc, attno, isSnapshotAny) >= 0)
		{
			dsr->nth = last;
			end = dsr->logical_row_count - 1 + pos_base;
			ret = 0;
		}
		else
		{
			ret = -1;
		}
	}
	/* read in block */
	else if (last < dsr->logical_row_count - 1)
	{
		if(ReadToBuffer(dsr, coldesc->rowstoread, vecdesc, attno, isSnapshotAny) >= 0)
		{
			dsr->nth = last;
			end = dsr->nth + pos_base;
			ret = 1;
		}
		else
		{
			ret = -1;
		}
	}
	/* read out bound of block */
	else
	{
		if(ReadToBuffer(dsr, dsr->logical_row_count - dsr->nth - 1,  vecdesc, attno, isSnapshotAny) >= 0)
		{
			dsr->nth = dsr->logical_row_count - 1;
			end = dsr->nth + pos_base;
			ret = 0;
		}
		else
		{
			ret = -1;
		}
	}

	if (coldesc->is_pos_recorder
		&& (ret >= 0))
	{
		DatumStreamBlockRead_fill_pos(coldesc, idx, start, end);
	}

	return ret;
}

/*
 * Large content block is always in s single block.
 *
 * For vector scan, we read a sing block (as a single datum) for
 * each datumstreamread_getlarge_vec. large context is read from
 * buffer_beginp and no need to move dsr->datump.
 *
 */
int
datumstreamread_getlarge_vec(DatumStreamRead *acc, VecDesc vecdesc, AttrNumber attno, bool isSnapshotAny)
{
	int64 visioffset = vecdesc->visbitmapoffset;

	CopyVarlenaIfVisible(acc->buffer_beginp, vecdesc, attno, false, visioffset, isSnapshotAny);
	return 0;
}
