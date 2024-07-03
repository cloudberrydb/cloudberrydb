#ifndef HUDI_BTREE_MERGER_H
#define HUDI_BTREE_MERGER_H

#include "postgres.h"
#include "commands/tablespace.h"
#include "storage/fd.h"
// #include "utils/datalake.h"
#include "src/dlproxy/datalake.h"
#include "utils/hsearch.h"
#include "cdb/cdbvars.h"
#include "src/provider/common/utils.h"
#include "src/provider/common/lmdb.h"

typedef struct HudiBtreeMerger
{
	MergeProvider base;
	MemoryContext mcxt;
	MDB_env *dbEnv;
	MDB_txn *dbTxn;
	MDB_dbi  dbi;
	MDB_cursor *cursor;
	MemTupleBinding *mtBind;
	bool isRegistered;
	File file;
} HudiBtreeMerger;

HudiBtreeMerger *createHudiBtreeMerger(List *columnDesc,
									   List *recordKeyFields,
									   char *preCombineField,
									   int64 fileSize,
									   TupleDesc tupDesc);

#endif // HUDI_BTREE_MERGER_H
