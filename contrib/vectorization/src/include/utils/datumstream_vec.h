/*--------------------------------------------------------------------
 * datumstream_vec.h
 *
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/utils/datumstream_vec.h
 *
 *--------------------------------------------------------------------
 */
#ifndef DATUMSTREAM_VEC_H
#define DATUMSTREAM_VEC_H

#include "utils/datumstream.h"
#include "utils/datumstreamblock.h"

#include "utils/arrow.h"

/* Stream access method */
extern int DatumStreamBlockRead_Get_Vec(DatumStreamBlockRead *dsr,
										VecDesc vecdesc,
										AttrNumber attno,
										int64 pos_base,
										bool isSnapshotAny);
extern int datumstreamread_getlarge_vec(DatumStreamRead *acc,
										VecDesc vecdesc,
										AttrNumber attno,
										 bool isSnapshotAny);
extern int DatumStreamBlockRead_Get_By_Pos_Vec(DatumStreamBlockRead *dsr,
											   ColDesc *coldesc,
											   void *pos,
											   int64 pos_base,
											   bool isSnapshotAny);

extern int
DatumStreamBlockRead_Get_By_Pos_without_bitmap_vec(DatumStreamBlockRead *dsr,
												   ColDesc *coldesc,
												   void *pos,
												   int64 pos_base);

extern int
DatumStreamBlockRead_Get_By_Pos_with_bitmap_vec(DatumStreamBlockRead *dsr,
												ColDesc *coldesc,
												void *pos,
												int64 pos_base,
												bool isSnapshotAny);

extern void
DatumStreamBlockRead_fill_pos(ColDesc *coldesc, int start_idx, int64 start_pos, int64 end_pos);

inline static int
datumstreamread_get_vec(DatumStreamRead *acc,
						int32 cur_seg,
						VecDesc vecdesc,
						AttrNumber attno, bool isSnapshotAny)
{
	if (acc->largeObjectState == DatumStreamLargeObjectState_None)
	{
		int64 segno = cur_seg;
		int64 row = acc->blockFirstRowNum;
		int64 base = (segno << 40) + row;
		/*
		 * Small objects are handled by the DatumStreamBlockRead module.
		 */
		return DatumStreamBlockRead_Get_Vec(&acc->blockRead, vecdesc, attno, base, isSnapshotAny);
	}
	else
	{
		return datumstreamread_getlarge_vec(acc, vecdesc, attno, isSnapshotAny);
	}
}

inline static int
datumstreamread_get_by_pos_vec(DatumStreamRead *acc,
							   int32 cur_seg,
							   VecDesc vecdesc,
							   AttrNumber attno,
							   void *pos,bool isSnapshotAny)
{
	if (acc->largeObjectState == DatumStreamLargeObjectState_None)
	{
		int64 segno = cur_seg;
		int64 row = acc->blockFirstRowNum;
		// 40 is from cbdb AOTupleId_MaxRowNum
		int64 base = (segno << 40) + row;
		/*
		 * Small objects are handled by the DatumStreamBlockRead module.
		 */
		return DatumStreamBlockRead_Get_Vec(&acc->blockRead, vecdesc, attno, base, isSnapshotAny);
	}
	else
	{
		//datumstreamread_getlarge(acc, datum, null);
		return -1;
	}
}

#endif   /* DATUMSTREAM_VEC_H */
