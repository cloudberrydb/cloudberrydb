/*-------------------------------------------------------------------------
 *
 * execDML.h
 *	  Prototypes for execDML.c
 *
 * Portions Copyright (c) 2012, EMC Corp.
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/executor/execDML.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EXECDML_H
#define EXECDML_H

extern void
reconstructTupleValues(AttrMap *map,
					   Datum *oldValues, bool *oldIsnull, int oldNumAttrs,
					   Datum *newValues, bool *newIsnull, int newNumAttrs);


extern TupleTableSlot *
reconstructMatchingTupleSlot(TupleTableSlot *slot, ResultRelInfo *resultRelInfo);

#endif   /* EXECDML_H */

