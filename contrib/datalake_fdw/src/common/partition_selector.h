#ifndef DATALAKE_PARTITION_SELECTOR_H
#define DATALAKE_PARTITION_SELECTOR_H


#include "../datalake_def.h"
#include "nodes/pg_list.h"



typedef struct PartitionConstraint
{
	List *partitionValues;
	List *constraints;
} PartitionConstraint;


extern int getAttnumber(TupleDesc tupleDesc, const char *attName);

extern List *splitString2(const char *value, char deli, char escape);

extern List *selectPartitions(List *quals, TupleDesc tupleDesc, List *keys, List *partitions, bool allParts);


void initializeConstraints(dataLakeOptions *options, List *quals, TupleDesc tupleDesc);

bool isLastPartition(void* scanstate);

int initializeDefaultMap(List *attNums,
					 List *constraints,
					 bool *proj,
					 int *defMap,
					 ExprState **defExprs);

List *transfromHMSPartitions(List *partitions);

Datum
ExecEvalConst(ExprState *exprstate, ExprContext *econtext,
			  bool *isNull, ExprDoneCond *isDone);

Datum
ExecEvalConst2(ExprState *exprstate, ExprContext *econtext,
			  ExprDoneCond *isDone);
#endif
