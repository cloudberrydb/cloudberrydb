/*--------------------------------------------------------------------
 * aocs.c
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
*	  src/backend/access/vecaocs/aocs.c
 *
 *--------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/appendonlytid.h"
#include "cdb/cdbaocsam.h"
#include "catalog/gp_fastsequence.h"
#include "utils/lsyscache.h"
#include "access/aomd.h"
#include "pgstat.h"
#include "catalog/pg_attribute_encoding.h"
#include "nodes/nodeFuncs.h"

#include "utils/am_vec.h"
#include "utils/datumstream_vec.h"
#include "utils/aocs_vec.h"
#include "vecexecutor/execnodes.h"
#include "vecexecutor/execslot.h"
#include "vecexecutor/executor.h"
#include "utils/datumstream_vec.h"

static AOCSScanDesc
aocs_beginscan_internal_vec(Relation relation,
							AOCSFileSegInfo **seginfo,
							int total_seg,
							Snapshot snapshot,
							Snapshot appendOnlyMetaDataSnapshot,
							bool *proj,
							uint32 flags,
							bool has_ctid);
static int
open_next_scan_seg_vec(AOCSScanDesc scan);
static void
initscan_with_colinfo(AOCSScanDesc scan);
static void open_ds_read(Relation rel, DatumStreamRead **ds, TupleDesc relationTupleDesc,
						 AttrNumber *proj_atts, AttrNumber num_proj_atts, bool checksum);
bool extractcolumns_from_node(Node *expr, bool *cols, AttrNumber natts);
static void init_visimap_info_vec(VecAOCSScanDescData* vscan, AttrNumber attno, int64 rowNum);

/*
 * begin the scan over the given relation.
 */
static AOCSScanDesc
aocs_beginscan_internal_vec(Relation relation,
							AOCSFileSegInfo **seginfo,
							int total_seg,
							Snapshot snapshot,
							Snapshot appendOnlyMetaDataSnapshot,
							bool *proj,
							uint32 flags,
							bool has_ctid)
{
	VecAOCSScanDescData* vscan;
	AOCSScanDesc	scan;
	AttrNumber		natts;
	Oid				visimaprelid;
	Oid				visimapidxid;

	vscan = (VecAOCSScanDescData*) palloc0(sizeof(VecAOCSScanDescData));
	vscan->islast = false;
	scan = (AOCSScanDesc) vscan;
	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_flags = flags;
	scan->appendOnlyMetaDataSnapshot = appendOnlyMetaDataSnapshot;
	scan->seginfo = seginfo;
	scan->total_seg = total_seg;
	scan->columnScanInfo.scanCtx = CurrentMemoryContext;
	vscan->has_ctid = has_ctid;

	/* relationTupleDesc will be inited by the slot when needed */
	scan->columnScanInfo.relationTupleDesc = NULL;

	/*
	 * We get an array of booleans to indicate which columns are needed. But
	 * if you have a very wide table, and you only select a few columns from
	 * it, just scanning the boolean array to figure out which columns are
	 * needed can incur a noticeable overhead in aocs_getnext. So convert it
	 * into an array of the attribute numbers of the required columns.
	 *
	 * However, if no array is given, then let it get lazily initialized when
	 * needed since all the attributes will be fetched.
	 */
	if (proj)
	{
		natts = RelationGetNumberOfAttributes(relation);
		scan->columnScanInfo.proj_atts = (AttrNumber *)
										 palloc0(natts * sizeof(AttrNumber));
		scan->columnScanInfo.num_proj_atts = 0;

		for (AttrNumber i = 0; i < natts; i++)
		{
			if (proj[i])
				scan->columnScanInfo.proj_atts[scan->columnScanInfo.num_proj_atts++] = i;
		}

		vscan->proj_atts_full = palloc0(natts * sizeof(int));
		vscan->num_proj_atts_full = 0;

		for (AttrNumber i = 0; i < natts; i++)
		{
			if (proj[i])
				vscan->proj_atts_full[vscan->num_proj_atts_full++] = i;
		}

		/* for late materialize , not in those columns which applied to filter and
		 * the data type is varlen */
		vscan->proj_atts_lm = palloc0(natts * sizeof(int));
		vscan->num_proj_atts_lm = 0;

		for (AttrNumber i = 0; i < natts; i++)
		{
			if (!proj[i + natts] && proj[i])
			{
				vscan->proj_atts_lm[vscan->num_proj_atts_lm++] = i;
				proj[i] = false;
			}
		}

		vscan->proj_atts_filter = palloc0(natts * sizeof(int));
		vscan->num_proj_atts_filter = 0;

		for (AttrNumber i = 0; i < natts; i++)
		{
			if (proj[i])
				vscan->proj_atts_filter[vscan->num_proj_atts_filter++] = i;
		}
	}

	scan->columnScanInfo.ds = NULL;

	GetAppendOnlyEntryAttributes(RelationGetRelid(relation),
								 NULL,
								 NULL,
								 NULL,
								 &scan->checksum,
								 NULL);

	GetAppendOnlyEntryAuxOids(RelationGetRelid(relation),
							  scan->appendOnlyMetaDataSnapshot,
							  NULL, NULL, NULL,
							  &visimaprelid, &visimapidxid);

	if (scan->total_seg != 0)
		AppendOnlyVisimap_Init(&scan->visibilityMap,
							   visimaprelid,
							   visimapidxid,
							   AccessShareLock,
							   appendOnlyMetaDataSnapshot);
	return scan;
}

/*
 * Initialise data streams for every column used in this query. For writes, this
 * means all columns.
 */
static void
open_ds_read(Relation rel, DatumStreamRead **ds, TupleDesc relationTupleDesc,
			 AttrNumber *proj_atts, AttrNumber num_proj_atts, bool checksum)
{
	/*
	 *  RelationGetAttributeOptions does not always success return opts. e.g.
	 *  `ALTER TABLE ADD COLUMN` with an illegal option.
	 *
	 *  In this situation, the transaction will abort, and the Relation will be
	 *  free. Upstream have sanity check to promise we must have a worked TupleDesc
	 *  attached the Relation during memory recycle. Otherwise, the query will crash.
	 *
	 *  For some reason, we can not put the option validation check into "perp"
	 *  phase for AOCO table ALTER command.
	 *  (commit: e707c19c885fadffe50095cc699e52af1ee64f4b)
	 *
	 *  So, a fake TupleDesc temporary replace into Relation.
	 */

	TupleDesc orig_att = rel->rd_att;
	if (orig_att->tdrefcount == -1)
	{
		rel->rd_att = CreateTemplateTupleDesc(relationTupleDesc->natts);
		rel->rd_att->tdrefcount = 1;
	}

	StdRdOptions **opts = RelationGetAttributeOptions(rel);

	if (orig_att->tdrefcount == -1)
	{
		pfree(rel->rd_att);
		rel->rd_att = orig_att;
	}

	/*
	 * Clear all the entries to NULL first, as the NULL value is used during
	 * closing
	 */
	for (AttrNumber attno = 0; attno < relationTupleDesc->natts; attno++)
		ds[attno] = NULL;

	/* And then initialize the data streams for those columns we need */
	for (AttrNumber i = 0; i < num_proj_atts; i++)
	{
		AttrNumber			attno = proj_atts[i];
		Form_pg_attribute	attr;
		char			   *ct;
		int32				clvl;
		int32				blksz;
		StringInfoData titleBuf;

		Assert(attno <= relationTupleDesc->natts);
		attr = TupleDescAttr(relationTupleDesc, attno);

		/*
		 * We always record all the three column specific attributes for each
		 * column of a column oriented table.  Note: checksum is a table level
		 * attribute.
		 */
		if (opts[attno] == NULL || opts[attno]->blocksize == 0)
			elog(ERROR, "could not find blocksize option for AOCO column in pg_attribute_encoding");
		ct = opts[attno]->compresstype;
		clvl = opts[attno]->compresslevel;
		blksz = opts[attno]->blocksize;

		/* UNDONE: Need to track and dispose of this storage... */
		initStringInfo(&titleBuf);
		appendStringInfo(&titleBuf, "Scan of Append-Only Column-Oriented relation '%s', column #%d '%s'",
						 RelationGetRelationName(rel),
						 attno + 1,
						 NameStr(attr->attname));

		ds[attno] = create_datumstreamread(ct,
										   clvl,
										   checksum,
										    /* safeFSWriteSize */ false,	/* UNDONE:Need to wire
																			 * down pg_appendonly
																			 * column */
										   blksz,
										   attr,
										   RelationGetRelationName(rel),
										    /* title */ titleBuf.data,
										   &rel->rd_node);
	}
}

/*
 * Open the segment file for a specified column associated with the datum
 * stream.
 */
static void
open_datumstreamread_segfile(
							 char *basepath, RelFileNode node,
							 AOCSFileSegInfo *segInfo,
							 DatumStreamRead *ds,
							 int colNo)
{
	int			segNo = segInfo->segno;
	char		fn[MAXPGPATH];
	int32		fileSegNo;

	AOCSVPInfoEntry *e = getAOCSVPEntry(segInfo, colNo);

	FormatAOSegmentFileName(basepath, segNo, colNo, &fileSegNo, fn);
	Assert(strlen(fn) + 1 <= MAXPGPATH);

	Assert(ds);
	datumstreamread_open_file(ds, fn, e->eof, e->eof_uncompressed, node,
							  fileSegNo, segInfo->formatversion);
}

/*
 * Open all segment files associted with the datum stream.
 *
 * Currently, there is one segment file for each column. This function
 * only opens files for those columns which are in the projection.
 *
 * If blockDirectory is not NULL, the first block info is written to
 * the block directory.
 */
static void
open_all_datumstreamread_segfiles(Relation rel,
								  AOCSFileSegInfo *segInfo,
								  DatumStreamRead **ds,
								  AttrNumber *proj_atts,
								  AttrNumber num_proj_atts,
								  AppendOnlyBlockDirectory *blockDirectory)
{
	char	   *basepath = relpathbackend(rel->rd_node, rel->rd_backend, MAIN_FORKNUM);

	Assert(proj_atts);
	for (AttrNumber i = 0; i < num_proj_atts; i++)
	{
		AttrNumber	attno = proj_atts[i];

		open_datumstreamread_segfile(basepath, rel->rd_node, segInfo, ds[attno], attno);
		datumstreamread_block(ds[attno], blockDirectory, attno);
	}

	pfree(basepath);
}

static int
open_next_scan_seg_vec(AOCSScanDesc scan)
{
	while (++scan->cur_seg < scan->total_seg)
	{
		AOCSFileSegInfo *curSegInfo = scan->seginfo[scan->cur_seg];

		if (curSegInfo->total_tupcount > 0)
		{
			bool		emptySeg = false;

			/*
			 * If the segment is entirely empty, nothing to do.
			 *
			 * We assume the corresponding segments for every column to be in
			 * the same state. So somewhat arbitrarily, we check the state of
			 * the first column we'll be accessing.
			 */

			/*
			 * subtle: we must check for AWAITING_DROP before calling getAOCSVPEntry().
			 * ALTER TABLE ADD COLUMN does not update vpinfos on AWAITING_DROP segments.
			 */
			if (curSegInfo->state == AOSEG_STATE_AWAITING_DROP)
				emptySeg = true;
			else
			{
				AOCSVPInfoEntry *e;

				e = getAOCSVPEntry(curSegInfo, scan->columnScanInfo.proj_atts[0]);
				if (e->eof == 0)
					elog(ERROR, "inconsistent segment state for relation %s, segment %d, tuple count " INT64_FORMAT,
						 RelationGetRelationName(scan->rs_base.rs_rd),
						 curSegInfo->segno,
						 curSegInfo->total_tupcount);
			}

			if (!emptySeg)
			{

				/*
				 * If the scan also builds the block directory, initialize it
				 * here.
				 */
				if (scan->blockDirectory)
				{
					/*
					 * if building the block directory, we need to make sure
					 * the sequence starts higher than our highest tuple's
					 * rownum.  In the case of upgraded blocks, the highest
					 * tuple will have tupCount as its row num for non-upgrade
					 * cases, which use the sequence, it will be enough to
					 * start off the end of the sequence; note that this is
					 * not ideal -- if we are at least curSegInfo->tupcount +
					 * 1 then we don't even need to update the sequence value
					 */

					AppendOnlyBlockDirectory_Init_forInsert(scan->blockDirectory,
															scan->appendOnlyMetaDataSnapshot,
															(FileSegInfo *) curSegInfo,
															0 /* lastSequence */ ,
															scan->rs_base.rs_rd,
															curSegInfo->segno,
															scan->columnScanInfo.relationTupleDesc->natts,
															true);

				}

				open_all_datumstreamread_segfiles(scan->rs_base.rs_rd,
												  curSegInfo,
												  scan->columnScanInfo.ds,
												  scan->columnScanInfo.proj_atts,
												  scan->columnScanInfo.num_proj_atts,
												  scan->blockDirectory);

				return scan->cur_seg;
			}
		}
	}

	return -1;
}

static void
close_cur_scan_seg_vec(AOCSScanDesc scan)
{
	if (scan->cur_seg < 0)
		return;

	/*
	 * If rescan is called before we lazily initialized then there is nothing to
	 * do
	 */
	if (scan->columnScanInfo.relationTupleDesc == NULL)
		return;

	for (AttrNumber i = 0;
		 i < scan->columnScanInfo.num_proj_atts;
		 i++)
	{
		int attno = scan->columnScanInfo.proj_atts[i];
		if (scan->columnScanInfo.ds[attno])
			datumstreamread_close_file(scan->columnScanInfo.ds[attno]);
	}

	if (scan->blockDirectory)
		AppendOnlyBlockDirectory_End_forInsert(scan->blockDirectory);
}

bool
aoco_getnextslot_vec(TableScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
{
	AOCSScanDesc aoscan = (AOCSScanDesc)scan;

	ExecClearTuple(slot);
	if (aocs_getnext_vec(aoscan, direction, slot))
	{
		// ExecStoreVirtualTuple(slot);
		// pgstat_count_heap_getnext(aoscan->rs_base.rs_rd);

		return true;
	}

	return false;
}

static int64
GetRowNum(VecAOCSScanDescData* vscan, AttrNumber attno)
{
	AOCSScanDescData *scan = &vscan->base_scan;
	DatumStreamRead *acc = scan->columnScanInfo.ds[attno];
	int64		rowNum = INT64CONST(-1);

	/* get aoTupeId for current row */
	if (acc->blockFirstRowNum != INT64CONST(-1))
	{
		int nth;

		Assert(acc->blockFirstRowNum > 0);

		if (acc->largeObjectState == DatumStreamLargeObjectState_None)
			nth = acc->blockRead.nth;
		else
			nth = 0;
		rowNum = acc->blockFirstRowNum + nth;
	}
	if (rowNum == INT64CONST(-1))
	{
		rowNum = scan->cur_seg_row + 1;
	}
	return rowNum;
}

static void
UpdateOffset(VecDesc vecdesc, int64 rowNum)
{
	vecdesc->visbitmapoffset = rowNum - vecdesc->firstrow + 1;
}

static void
UpdateVisimap(VecDesc vecdesc, int64 rowNum, AppendOnlyVisimap *visiMap)
{
	Bitmapset *bitmap = visiMap->visimapEntry.bitmap;
	/* set visbitmapoffset as offset of the first row*/
	if (vecdesc->firstrow == -1)
	{
		vecdesc->firstrow = visiMap->visimapEntry.firstRowNum;
		/* row num start from 0, bitmap start from 1 */
		vecdesc->visbitmapoffset = rowNum - vecdesc->firstrow + 1;
	}

	/* set bitmap */
	if (!bms_is_empty(visiMap->visimapEntry.bitmap))
	{
		Bitmapset *newbitmap;
		int size;

		size = sizeof(Bitmapset) + bitmap->nwords * sizeof(bitmapword);
		newbitmap = (Bitmapset *) palloc(size);
		memcpy(newbitmap, bitmap, size);
		vecdesc->visbitmaps =
				lappend(vecdesc->visbitmaps, newbitmap);
	}
	else {
		vecdesc->visbitmaps = vecdesc->visbitmaps ? 
					lappend(vecdesc->visbitmaps, NULL) :
					NULL;
	}


}

/* Get visimaps for this DatumStreamRead. */
void
init_visimap_info_vec(VecAOCSScanDescData* vscan, AttrNumber attno, int64 rowNum)
{
	AOCSScanDescData *scan = &vscan->base_scan;
	AppendOnlyVisimap *visiMap = &scan->visibilityMap;
	VecDesc vecdesc = vscan->vecdes;
	int32 segno = scan->seginfo[scan->cur_seg]->segno;
	AOTupleId	aoTupleId;
	DatumStreamRead *read = scan->columnScanInfo.ds[attno];
	int64		rowCount = read->blockRowCount;
	int64		blockbound = read->blockFirstRowNum + rowCount;

	/* cover current rowNum and current block? */
	if ((rowNum < vecdesc->boundrow) && ((blockbound - 1) < vecdesc->boundrow) )
	{
		return;
	}

	while (rowNum < ((read->blockFirstRowNum + rowCount) / APPENDONLY_VISIMAP_MAX_RANGE + 1)
			* APPENDONLY_VISIMAP_MAX_RANGE)
	{
		AOTupleIdInit(&aoTupleId, segno, rowNum);
		vecdesc->cur_ctid =
				(((uint64)ItemPointerGetBlockNumber((ItemPointer)&aoTupleId)) << 16)
				+ ((uint64)ItemPointerGetOffsetNumber((ItemPointer)&aoTupleId));

		/* find vismapEntry for current row */
		if (!AppendOnlyVisimapEntry_CoversTuple(&visiMap->visimapEntry, &aoTupleId))
		{
			/* if necessary persist the current entry before moving. */
			if (AppendOnlyVisimapEntry_HasChanged(&visiMap->visimapEntry))
			{
				/* Only SELECT is supported for vectorizaion now,
				 * so visimap is read-only.*/
				Assert(false);
			}

			if(!AppendOnlyVisimapStore_Find(&visiMap->visimapStore, segno,
					(rowNum / APPENDONLY_VISIMAP_MAX_RANGE) * APPENDONLY_VISIMAP_MAX_RANGE,
					&visiMap->visimapEntry))
			{
				/*
				 * There is no entry that covers the given tuple id.
				 */
				AppendOnlyVisimapEntry_New(&visiMap->visimapEntry, &aoTupleId);
			}

			UpdateVisimap(vecdesc, rowNum, visiMap);
		}
		/* cover current rows and visimap is freed */
		else if (!vecdesc->visbitmaps)

		{
			UpdateVisimap(vecdesc, rowNum, visiMap);
		}
		/* visiMap covers APPENDONLY_VISIMAP_MAX_RANGE at most. */
		rowNum += APPENDONLY_VISIMAP_MAX_RANGE;
		vecdesc->boundrow = visiMap->visimapEntry.firstRowNum + APPENDONLY_VISIMAP_MAX_RANGE;
	}
}

bool
aocs_getnext_vec(AOCSScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
{
	VecAOCSScanDescData* vscan = (VecAOCSScanDescData*)scan;
	int			err = 0;
	AttrNumber	i;
	bool		isSnapshotAny = (scan->rs_base.rs_snapshot == SnapshotAny);

	if (vscan->islast)
	{
		ExecClearTuple(slot);
		return false;
	}

	Assert(ScanDirectionIsForward(direction));

	if (scan->columnScanInfo.relationTupleDesc == NULL)
	{
		scan->columnScanInfo.relationTupleDesc = RelationGetDescr(scan->rs_base.rs_rd);
		/* Pin it! ... and of course release it upon destruction / rescan */
		PinTupleDesc(scan->columnScanInfo.relationTupleDesc);
		initscan_with_colinfo(scan);
	}

	while (1)
	{
		AOCSFileSegInfo *curseginfo;

		/* If necessary, open next seg */
		if (scan->cur_seg < 0)
		{
			err = open_next_scan_seg_vec(scan);
			if (err < 0)
			{
				/* No more seg, we are at the end */
				ExecClearTuple(slot);
				scan->cur_seg = -1;
				vscan->islast = true;
				return false;
			}
			scan->cur_seg_row = 0;
		}

		Assert(scan->cur_seg >= 0);
		curseginfo = scan->seginfo[scan->cur_seg];

		/* Read from cur_seg */
		for (i = 0; i < scan->columnScanInfo.num_proj_atts; i++)
		{
			bool  isnull;
			AttrNumber	attno = scan->columnScanInfo.proj_atts[i];
			VecDesc vecdes = vscan->vecdes;
			ColDesc *ctiddesc = NULL;

			ResetColDesc(vecdes->coldescs[i]);

			if (vscan->has_ctid && i == scan->columnScanInfo.num_proj_atts - 1)
			{
				ctiddesc = vecdes->coldescs[vecdes->natts - 1];
				ResetColDesc(ctiddesc);
			}
			while (true)
			{
				/* init all visimaps for this read*/
				int64 rowNum = GetRowNum(vscan, attno);

				if (i == 0)
				{
					init_visimap_info_vec(vscan, attno, rowNum);
					vecdes->scan_ctid = false;
				}
				UpdateOffset(vecdes, rowNum);

				if (vscan->has_ctid && i == scan->columnScanInfo.num_proj_atts - 1)
					vecdes->scan_ctid = true;
				err = datumstreamread_get_vec(scan->columnScanInfo.ds[attno],
											  scan->cur_seg,
											  vecdes,
											  i,
											  isSnapshotAny);
				Assert(err >= 0);

				/* Read array, array is always not null. */
				isnull = false;

				if (err == 0)
				{
					/*
					 * err == 0 means that need read next block for more data
					 */
					err = datumstreamread_block(scan->columnScanInfo.ds[attno], scan->blockDirectory, attno);
					if (err < 0)
					{
						if (i >= scan->columnScanInfo.num_proj_atts - 1)
						{
							/* close seg after read all column. */
							close_cur_scan_seg_vec(scan);

							/* open next seg for next time read. */
							err = open_next_scan_seg_vec(scan);
							if (err < 0)
							{
								/* No more seg, we are at the end */
								scan->cur_seg = -1;
								vscan->islast = true;
								break;
							}
							scan->cur_seg_row = 0;

							if (scan->cur_seg > 0)
							{/*  reset vector description about visual map while open new seg file*/
								resetVecDescBitmap(vscan->vecdes);
							}
						}
						/* no more block, continue read next column. */
						break;
					}
				}
				else
				{
					/* err > 0 means we got a complete array. */
					if (i == 0 && isSnapshotAny)
						continue;
					break;
				}
			}
		}
		GenerateVecValue(vscan->vecdes, slot);
		/* Got an empty slot this time, continue to next scan.
		 * Only when islast is set, the scan is end.*/
		if (!TupIsNull(slot))
		{
			vscan->cur_seg_filter = scan->cur_seg;
			return true;
		}
		else if (vscan->islast)
			return false;
	}

	Assert(!"Never here");
	return false;
}

/*
 * late materialize and no-lm use common logic to init VecAOCSScanDescData
 */
AOCSScanDesc
aocs_beginscan_vec(Relation relation,
				  Snapshot snapshot,
				  bool *proj,
				  uint32 flags,
				  bool has_ctid)
{
	AOCSFileSegInfo	  **seginfo;
	Snapshot			aocsMetaDataSnapshot;
	int32				total_seg;

	RelationIncrementReferenceCount(relation);

	/*
	 * the append-only meta data should never be fetched with
	 * SnapshotAny as bogus results are returned.
	 */
	if (snapshot != SnapshotAny)
		aocsMetaDataSnapshot = snapshot;
	else
		aocsMetaDataSnapshot = GetTransactionSnapshot();

	seginfo = GetAllAOCSFileSegInfo(relation, aocsMetaDataSnapshot, &total_seg, NULL);
	return aocs_beginscan_internal_vec(relation,
									  seginfo,
									  total_seg,
									  snapshot,
									  aocsMetaDataSnapshot,
									  proj,
									  flags,
									  has_ctid);
}

TableScanDesc
aoco_beginscan_vec(Relation rel, Snapshot snapshot,
				   List *targetlist, List *qual,
				   uint32 flags)
{
	AOCSScanDesc	aoscan;
	AttrNumber		natts = RelationGetNumberOfAttributes(rel);
	bool		   *cols;
	bool			found = false;
	bool			ctid = false;

	cols = palloc0(natts * sizeof(*cols));

	found |= extractcolumns_from_node((Node *)targetlist, cols, natts);
	found |= extractcolumns_from_node((Node *)qual, cols, natts);

	/*
	 * In some cases (for example, count(*)), targetlist and qual may be null,
	 * extractcolumns_walker will return immediately, so no columns are specified.
	 * We always scan the first column.
	 */
	if (!found)
		cols[0] = true;

	if (has_ctid((Expr *)targetlist, NULL) || has_ctid((Expr *)qual, NULL))
		ctid = true;

	aoscan = aocs_beginscan_vec(rel,
							snapshot,
							cols,
							flags,
							ctid);

	pfree(cols);

	return (TableScanDesc)aoscan;
}


static void
initscan_with_colinfo(AOCSScanDesc scan)
{
	VecAOCSScanDescData *vscan;
	MemoryContext	oldCtx;
	AttrNumber		natts;

	Assert(scan->columnScanInfo.relationTupleDesc);
	natts = scan->columnScanInfo.relationTupleDesc->natts;

	oldCtx = MemoryContextSwitchTo(scan->columnScanInfo.scanCtx);
	vscan = (VecAOCSScanDescData*)scan;

	if (scan->columnScanInfo.ds == NULL)
		scan->columnScanInfo.ds = (DatumStreamRead **)
								  palloc0(natts * sizeof(DatumStreamRead *));

	if (scan->columnScanInfo.proj_atts == NULL)
	{
		scan->columnScanInfo.num_proj_atts = natts;
		scan->columnScanInfo.proj_atts = (AttrNumber *)
										 palloc0(natts * sizeof(AttrNumber));

		for (AttrNumber attno = 0; attno < natts; attno++)
			scan->columnScanInfo.proj_atts[attno] = attno;
	}

	open_ds_read(scan->rs_base.rs_rd, scan->columnScanInfo.ds,
				 scan->columnScanInfo.relationTupleDesc,
				 scan->columnScanInfo.proj_atts, scan->columnScanInfo.num_proj_atts,
				 scan->checksum);

	MemoryContextSwitchTo(oldCtx);

	scan->cur_seg = -1;
	scan->cur_seg_row = 0;
	vscan->cur_seg_lm = -1;
	vscan->cur_seg_filter = -1;
	scan->columnScanInfo.proj_atts = vscan->proj_atts_full;
	scan->columnScanInfo.num_proj_atts = vscan->num_proj_atts_full;

	ItemPointerSet(&scan->cdb_fake_ctid, 0, 0);

	pgstat_count_heap_scan(scan->rs_base.rs_rd);
}


struct ExtractcolumnContext
{
	bool	   *cols;
	AttrNumber	natts;
	bool		found;
};



static bool
extractcolumns_walker(Node *node, struct ExtractcolumnContext *ecCtx)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var *var = (Var *)node;

		if (IS_SPECIAL_VARNO(var->varno))
			return false;

		if (var->varattno > 0 && var->varattno <= ecCtx->natts)
		{
			ecCtx->cols[var->varattno -1] = true;
			ecCtx->found = true;
		}
		/*
		 * If all attributes are included,
		 * set all entries in mask to true.
		 */
		else if (var->varattno == 0)
		{
			for (AttrNumber attno = 0; attno < ecCtx->natts; attno++)
				ecCtx->cols[attno] = true;
			ecCtx->found = true;

			return true;
		}

		return false;
	}

	return expression_tree_walker(node, extractcolumns_walker, (void *)ecCtx);
}

bool
extractcolumns_from_node(Node *expr, bool *cols, AttrNumber natts)
{
	struct ExtractcolumnContext	ecCtx;

	ecCtx.cols	= cols;
	ecCtx.natts = natts;
	ecCtx.found = false;

	extractcolumns_walker(expr, &ecCtx);

	return  ecCtx.found;
}


static void
close_ds_read_vec(DatumStreamRead **ds, AttrNumber natts)
{
	for (AttrNumber attno = 0; attno < natts; attno++)
	{
		if (ds[attno])
		{
			destroy_datumstreamread(ds[attno]);
			ds[attno] = NULL;
		}
	}
}


void
aoco_endscan_vec(TableScanDesc scan)
{
	AOCSScanDesc	aocsScanDesc;

	aocsScanDesc = (AOCSScanDesc) scan;

	if (aocsScanDesc->descIdentifier == AOCSSCANDESCDATA)
	{
		aocs_endscan_vec(aocsScanDesc);
		return;
	}
}


void
aocs_endscan_vec(AOCSScanDesc scan)
{
	VecAOCSScanDescData *vscan;

	vscan = (VecAOCSScanDescData*) scan;

	close_cur_scan_seg_vec(scan);

	if (vscan->proj_atts_filter)
		pfree(vscan->proj_atts_filter);

	if (vscan->proj_atts_lm)
		pfree(vscan->proj_atts_lm);

	if (vscan->proj_atts_full)
		pfree(vscan->proj_atts_full);

	if (scan->columnScanInfo.ds)
	{
		Assert(scan->columnScanInfo.proj_atts);

		close_ds_read_vec(scan->columnScanInfo.ds, scan->columnScanInfo.relationTupleDesc->natts);
		pfree(scan->columnScanInfo.ds);
	}

	if (scan->columnScanInfo.relationTupleDesc)
	{
		Assert(scan->columnScanInfo.proj_atts);

		ReleaseTupleDesc(scan->columnScanInfo.relationTupleDesc);
		scan->columnScanInfo.relationTupleDesc = NULL;
	}

	if (scan->columnScanInfo.proj_atts)
		pfree(scan->columnScanInfo.proj_atts);

	for (int i = 0; i < scan->total_seg; ++i)
	{
		if (scan->seginfo[i])
		{
			pfree(scan->seginfo[i]);
			scan->seginfo[i] = NULL;
		}
	}

	if (scan->seginfo)
		pfree(scan->seginfo);

	if (scan->total_seg != 0)
		AppendOnlyVisimap_Finish(&scan->visibilityMap, AccessShareLock);

	RelationDecrementReferenceCount(scan->rs_base.rs_rd);

	pfree(scan);

	pfree(vscan);
}
