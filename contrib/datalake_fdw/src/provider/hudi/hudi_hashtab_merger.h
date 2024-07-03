#ifndef HUDI_HASHTAB_MERGER_H
#define HUDI_HASHTAB_MERGER_H

#include "postgres.h"
#include "src/dlproxy/datalake.h"
#include "utils/hsearch.h"
#include "src/provider/common/utils.h"

typedef struct HudiHashTableMerger
{
	MergeProvider base;
	MemoryContext mcxt;
	HASH_SEQ_STATUS hashTabSeqStatus;
	bool isRegistered;
} HudiHashTableMerger;

HudiHashTableMerger *createHudiHashTableMerger(MemoryContext mcxt,
											   List *columnDesc,
											   List *recordKeyFields,
											   char *preCombineField);

#endif // HUDI_HASHTAB_MERGER_H
