/*-------------------------------------------------------------------------
 *
 * persistentutil.c
 *
 * Functions to support administrative tasks against gp_persistent_*_node
 * tables, gp_global_sequence and gp_relation_node. These tables do not have
 * normal MVCC semantics and changing them is usually part of a more complex
 * chain of events in the backend. So, we cannot modify them with INSERT,
 * UPDATE or DELETE as we might other catalog tables.
 *
 * So we provide these functions which update the tables and also update the in
 * memory persistent table / file rep data structures.
 *
 * Portions Copyright (c) 2010 Greenplum
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/utils/gp/persistentutil.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "funcapi.h"

#include "access/heapam.h"
#include "catalog/catalog.h"
#include "catalog/pg_tablespace.h"
#include "cdb/cdbglobalsequence.h"
#include "executor/executor.h"
#include "utils/builtins.h"



#include <dirent.h>


typedef struct
{
	Relation			 tablespaceRelation;
	HeapScanDesc		 scandesc;
	Oid					 tablespaceOid;
	Oid					 databaseOid;
	DIR					*tablespaceDir;
	DIR					*databaseDir;
	char                 tablespaceDirName[MAXPGPATH];
	char                 databaseDirName[MAXPGPATH];
} node_check_data;

#define MAX_STRING_LEN_RELFILENODE 10 /* strlen("2147483648") */
#define NYI elog(ERROR, "not yet implemented")

Datum
gp_add_global_sequence_entry(PG_FUNCTION_ARGS)
{
	NYI;
	PG_RETURN_BOOL(true);
}

/*
 * gp_update_global_sequence_entry(tid, bigint) => bool
 * 
 * Updates the given global sequence to the specified value:
 *   - Only allows increasing the sequence value
 *   - Only lets you set the tids '(0,1)' through '(0,4)'
 *     * these are the only tids currently used by the system 
 *       (see cdb/cdbglobalsequence.h)
 */
Datum
gp_update_global_sequence_entry(PG_FUNCTION_ARGS)
{
	ItemPointer			tid;
	int8				sequenceVal;
	GpGlobalSequence    sequence;

	/* Must be super user */
	if (!superuser())
		elog(ERROR, "permission denied");

	/* Check input arguments */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		elog(ERROR, "null input parameter");

	tid = (ItemPointer) PG_GETARG_POINTER(0);
	sequenceVal = PG_GETARG_INT64(1);

	/* Check tid */
	if (ItemPointerGetBlockNumber(tid) != 0)
		elog(ERROR, "unexpected block number in tid");
	sequence = (GpGlobalSequence) ItemPointerGetOffsetNumber(tid);
	switch (sequence)
	{
		case GpGlobalSequence_PersistentRelation:
		case GpGlobalSequence_PersistentDatabase:
		case GpGlobalSequence_PersistentTablespace:
		case GpGlobalSequence_PersistentFilespace:
			break;

		default:
			elog(ERROR, "unexpected offset number in tid");
	}

	/* Check sequence value */
	if (sequenceVal < GlobalSequence_Current(sequence))
		elog(ERROR, "sequence number too low");

	/* Everything looks good, update the value */
	GlobalSequence_Set(sequence, sequenceVal);
	PG_RETURN_BOOL(true);
}

Datum
gp_delete_global_sequence_entry(PG_FUNCTION_ARGS)
{
	NYI;
	PG_RETURN_BOOL(true);
}
