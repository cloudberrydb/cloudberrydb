/*-------------------------------------------------------------------------
 *
 * appendonlytid.h
 *
 * Portions Copyright (c) 2007-2009, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/access/appendonlytid.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef APPENDONLYTID_H
#define APPENDONLYTID_H

#include "c.h"

/*
 * AOTupleId is a unique tuple id, specific to AO relation tuples, of the
 * form (segfile#, row#)
 *
 * *** WARNING *** Outside the appendonly AM code, AOTIDs are treated as
 * HEAPTIDs!  Therefore, AOTupleId struct must have the same size and
 * alignment as ItemPointer, and we mustn't construct AOTupleIds that would
 * look like invalid ItemPointers.
 *
 * The 48 bits available in the struct are divided as follow:
 *
 * - the 7 leftmost bits stand for which segment file the tuple is in
 *   (Limit: 128 (2^8))
 *
 * - the 16th bit in bytes_4_5 is used to avoid an all-zeros value in the
 *   field (see below)
 *
 * - the remaining 40 bits stand for the row within the segment file
 *   (Limit: 1099 trillion (2^40 - 1)).
 *
 * About that 16th bit in 'bytes_4_5': When an AOTupleId is cast to an
 * ItemPointer, 'bytes_4_5' becomes the 'ip_posid' field in the ItemPointer.
 * An invalid itempointer is represented by 0 'ip_posid', so we must avoid
 * that.  To do so, if 'bytes_4_5' would be all zeros, we set the 16th bit.
 * That occurs when the 15 lower bits of the row number are 0.
 *
 * NOTE: Before GPDB6, we *always* set the reserved bit, not only when
 * 'bytes_4_5' would otherwise be zero.  That was simpler, but caused problems
 * of its own, because very high offset numbers, which cannot occur in heap
 * tables, have a special meaning in some places.  See TUPLE_IS_INVALID in
 * gist_private.h, for example.  In other words, the 'ip_posid' field now uses
 * values in the range 1..32768, whereas before GPDB 6, it used the range
 * (32768..65535).  We must still be prepared to deal with the old TIDs, if
 * the cluster was binary-upgrade from an older version! Because of that, the
 * accessor macros simply ignore the reserved bit.
 */
typedef struct AOTupleId
{
	uint16		bytes_0_1;
	uint16		bytes_2_3;
	uint16		bytes_4_5;

} AOTupleId;

#define AOTupleIdGet_segmentFileNum(h)        ((((h)->bytes_0_1&0xFE00)>>9)) // 7 bits
#define AOTupleIdGet_makeHeapExecutorHappy(h) (((h)->bytes_4_5&0x8000)) // 1 bit
#define AOTupleIdGet_rowNum(h) \
	((((uint64)((h)->bytes_0_1&0x01FF))<<31)|(((uint64)((h)->bytes_2_3))<<15)|(((uint64)((h)->bytes_4_5&0x7FFF))))
 /* top most 25 bits */	/* 15 bits from bytes_4_5 */

static inline void
AOTupleIdInit(AOTupleId *h, uint16 segfilenum, uint64 rownum)
{
	h->bytes_0_1 = ((uint16) (0x007F & segfilenum)) << 9;
	h->bytes_0_1 |= (uint16) ((INT64CONST(0x000000FFFFFFFFFF) & rownum) >> 31);
	h->bytes_2_3 = (uint16) ((INT64CONST(0x000000007FFFFFFF) & rownum) >> 15);

	/* If 'bytes_4_5' would otherwise be all-zero, set the reserved bit. */
	if ((0x7FFF & rownum) == 0)
		h->bytes_4_5 = 0x8000;
	else
		h->bytes_4_5 = 0x7FFF & rownum;
}

/* like ItemPointerSetInvalid */
static inline void
AOTupleIdSetInvalid(AOTupleId *h)
{
	ItemPointerSetInvalid((ItemPointer) h);
}

#define AOTupleId_MaxRowNum            INT64CONST(1099511627775) 		// 40 bits, or 1099511627775 (1099 trillion).

#define AOTupleId_MaxSegmentFileNum    			127
#define AOTupleId_MultiplierSegmentFileNum    	128	// Next up power of 2 as multiplier.

extern char *AOTupleIdToString(AOTupleId *aoTupleId);

#endif							/* APPENDONLYTID_H */
