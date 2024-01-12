/*-------------------------------------------------------------------------
 * tuptable_vec.h
 *	  Redefine the TupleTableSlot structure to adapt to vectorization
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 *
 * IDENTIFICATION
 *		src/include/utils/tuptable_vec.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TUPTABLE_VEC_H
#define TUPTABLE_VEC_H

#include "postgres.h"
#include "executor/tuptable.h"
#include "utils/arrow.h"

typedef struct VecSchema {
	/* build GArrowSchema based on tupdesc attr */
	void *schema;
    /* GArrowSchema, init by tupleDesc, rebuilt by actual array data type. */
	bool schema_rebuilt; /* true for rebuilt schema */
} VecSchema;

typedef struct VecTupleTableSlot
{
	TupleTableSlot base; 	/* a base class slot */

	/* tuple vectorized values */
	void *tts_recordbatch;		/* arrow batch, must visit by GetBatch */

	/* Arrow schema like tupdesc */
	VecSchema vec_schema;
} VecTupleTableSlot;

#define VECSLOT(slot) ((VecTupleTableSlot*)slot)

#define			TTS_FLAG_VECTOR 	(1 << 5)
#define			TTS_FLAG_DIRTY 		(1 << 6)

/*
 * Predefined TupleTableSlotOps for various types of TupleTableSlotOps. The
 * same are used to identify the type of a given slot.
 */
extern PGDLLIMPORT const TupleTableSlotOps TTSOpsVecTuple;

#define TTS_IS_VECTOR(slot) ((slot)->tts_ops == &TTSOpsVecTuple)

#define TTS_CLEAR_EMPTY(slot) (slot->tts_flags &= (~TTS_FLAG_EMPTY))

#define TTS_IS_DIRTY(slot) ((slot->tts_flags & TTS_FLAG_DIRTY) != 0)
#define TTS_SET_DIRTY(slot) (slot->tts_flags |= TTS_FLAG_DIRTY)
#define TTS_CLEAR_DIRTY(slot) (slot->tts_flags &= (~TTS_FLAG_DIRTY))

#define TTS_SET_SHOULDFREE(slot) (slot->tts_flags |= TTS_FLAG_SHOULDFREE)
#define TTS_CLEAR_SHOULDFREE(slot) (slot->tts_flags &= (~TTS_FLAG_SHOULDFREE))


static inline bool TupHasVectorTuple(TupleTableSlot *slot)
{
	Assert(slot);
	return TTS_IS_VECTOR(slot) && VECSLOT(slot)->tts_recordbatch != NULL;
}


static inline bool IsContainEmptyTuples(TupleTableSlot *slot)
{
	if (TupIsNull(slot) || TTS_IS_DIRTY(slot))
		return false;
	return garrow_record_batch_is_null((GArrowRecordBatch *)(VECSLOT(slot)->tts_recordbatch));
}

extern const char *GetUniqueAttrName(char *attr_name, AttrNumber attr);
extern int GetNumRows(TupleTableSlot *slot);
extern void InitSlotSchema(TupleTableSlot *slot);
extern GArrowSchema *TupDescToSchema(TupleDesc tupdesc);
extern void RebuildRuntimeSchema(TupleTableSlot *slot);
extern GArrowSchema *GetSchemaFromSlot(TupleTableSlot *slot);
extern char *BuildUniqueAttrName(const char *attr_name, int attrno);
extern TupleDesc CreateCtidTupleDesc(TupleDesc tupdesc);

#endif   /* TUPTABLE_VEC_H */
