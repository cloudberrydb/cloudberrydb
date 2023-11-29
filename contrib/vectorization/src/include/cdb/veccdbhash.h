/*-------------------------------------------------------------------------
 *
 * veccdbhash.h
 *	  Implement hashing routines with arrow.
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 *
 * IDENTIFICATION
 *		src/include/cdb/veccdbhash.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_CDBHASH_H
#define VEC_CDBHASH_H
#include "cdb/cdbhash.h"

typedef struct VecCdbHash
{
	CdbHash base;
	void **segmapping_scalar;
	void *hasharray;
}VecCdbHash;

extern Datum
evalHashKeyVec(void *projector, void *recordbatch, VecCdbHash *h, int rows);

extern VecCdbHash *
makeVecCdbHash(int numsegs, int natts, Oid *hashfuncs);

extern Datum
cdbhashreduce_vec(VecCdbHash *h);

#endif							/* VEC_CDBHASH_H */