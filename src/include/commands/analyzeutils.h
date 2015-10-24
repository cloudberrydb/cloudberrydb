/*-------------------------------------------------------------------------
 *
 * analyzeutils.h
 *
 *	  Header file for utils functions in analyzeutils.c
 *
 * Copyright (c) 2015, Pivotal Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ANALYZEUTILS_H
#define ANALYZEUTILS_H

/* extern functions called by commands/analyze.c */
extern void aggregate_leaf_partition_MCVs(Oid relationOid,
		AttrNumber attnum,
		unsigned int nEntries,
		ArrayType **result);
extern void aggregate_leaf_partition_histograms(Oid relationOid,
		AttrNumber attnum,
		unsigned int nEntries,
		ArrayType **result);
extern bool datumCompare(Datum d1, Datum d2, Oid opFuncOid);

#endif  /* ANALYZEUTILS_H */
