/*-------------------------------------------------------------------------
 * vectupser.h
 *	   Functions for serializing and deserializing heap tuples.
 *
 * Portions Copyright (c) 2016-2020, HashData
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/vectupser.h
 *-------------------------------------------------------------------------
 */
#ifndef VEC_TUPSER_H
#define VEC_TUPSER_H

/* Convert a tuple into chunks directly in a set of transport buffers */
extern int SerializeTupleVec(TupleTableSlot *tuple, SerTupInfo *pSerInfo,
							 struct directTransportBuffer *b, TupleChunkList tcList, int16 targetRoute);
void *DeserializeMinimalTuple(MinimalTuple tup, bool need_copy);
TupleTableSlot *ExecStoreMinimalTupleTupleVec(MinimalTuple tup, TupleTableSlot *slot, bool shouldFree);

MinimalTuple
ExecCopySlotMinimalTupleVec(TupleTableSlot *slot);

/*
 * An aligned buffer(8 bytes) for arrow group by.
 */
typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int32 		pad;         /* byte padding, so that the data start address is aligned with 8 bytes */
	char 		data[FLEXIBLE_ARRAY_MEMBER];  /* actual data */
} VarBuffer;

#define VARBUFFER(PTR)		(((VarBuffer *) (PTR))->data)
#define PADSIZE VARHDRSZ
#define DatumGetVarBufferP(X)		   ((VarBuffer *) PG_DETOAST_DATUM(X))
#define VARBUFFERFROMDATUM(PTR) (VARDATA_ANY(PTR) + PADSIZE)
#define VARBUFFERFDATASIZE(PTR) (VARSIZE_ANY_EXHDR(PTR) - PADSIZE)

#endif   /* VEC_TUPSER_H */
