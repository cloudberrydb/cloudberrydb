/*-------------------------------------------------------------------------
 *
 * Storage manipulation for directory table.
 *
 * Copyright (c) 2016-Present Hashdata, Inc.
 *
 * src/backend/catalog/storage_directory_table.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/parallel.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/pg_directory_table.h"
#include "catalog/pg_tablespace.h"
#include "catalog/storage_directory_table.h"
#include "storage/smgr.h"
#include "storage/ufile.h"
#include "utils/acl.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "cdb/cdbvars.h"

/*
 * TODO: Redo pending delete
 *
 * We do not support deleteing files during WAL redo, this is because deleting
 * files requires a connection to object storage system. In order to establish
 * the connection to the object storage, we need to access the catalog table to
 * retrieve the connection configuration info, which is impossible during WAL
 * redo.
 */

typedef struct FileNodePendingDelete
{
	RelFileNode node;
	char  relkind;
	Oid   spcId;			/* directory table needs an extra tabpespace */
	char *relativePath;
} FileNodePendingDelete;

typedef struct PendingRelDeleteFile
{
	FileNodePendingDelete filenode;		/* relation that may need to be deleted */
	bool		atCommit;		/* T=delete at commit; F=delete at abort */
	int			nestLevel;		/* xact nesting level of request */
	struct PendingRelDeleteFile *next;		/* linked-list link */
} PendingRelDeleteFile;

static PendingRelDeleteFile *pendingDeleteFiles = NULL; /* head of linked list */

void
DirectoryTableDropStorage(Relation rel)
{
	char *filePath;
	DirectoryTable *dirTable;
	PendingRelDeleteFile *pending;
	TableScanDesc scandesc;
	Relation	spcrel;
	HeapTuple	tuple;
	Form_pg_tablespace spcform;
	ScanKeyData entry[1];
	Oid			tablespaceoid;
	char 	   *tablespace_name;

//	if (Gp_role != GP_ROLE_DISPATCH)
//		return;

	dirTable = GetDirectoryTable(RelationGetRelid(rel));

	/*
	 * Find the tablespace by spaceId
	 */
	spcrel = table_open(TableSpaceRelationId, RowExclusiveLock);

	ScanKeyInit(&entry[0],
				Anum_pg_tablespace_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(dirTable->spcId));
	scandesc = table_beginscan_catalog(spcrel, 1, entry);
	tuple = heap_getnext(scandesc, ForwardScanDirection);

	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("tablespace \"%d\" does not exist",
							dirTable->spcId)));
	}

	spcform = (Form_pg_tablespace) GETSTRUCT(tuple);
	tablespaceoid = spcform->oid;
	tablespace_name = pstrdup(NameStr(((Form_pg_tablespace) GETSTRUCT(tuple))->spcname));

	/* Must be tablespace owner */
	if (!pg_tablespace_ownercheck(tablespaceoid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLESPACE,
					   tablespace_name);

	/* Disallow drop of the standard tablespaces, even by superuser */
	if (tablespaceoid == GLOBALTABLESPACE_OID ||
		tablespaceoid == DEFAULTTABLESPACE_OID)
		aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_TABLESPACE,
					   tablespace_name);

	table_endscan(scandesc);
	table_close(spcrel, RowExclusiveLock);

	filePath = psprintf("%s", dirTable->location);

	/* Add the relation to the list of stuff to delete at commit */
	pending = (PendingRelDeleteFile *)
		MemoryContextAlloc(TopMemoryContext, sizeof(PendingRelDeleteFile));
	pending->filenode.node = rel->rd_node;
	pending->filenode.relkind = rel->rd_rel->relkind;
	pending->filenode.relativePath = MemoryContextStrdup(TopMemoryContext, filePath);
	pending->filenode.spcId = dirTable->spcId;

	pending->atCommit = true;	/* delete if commit */
	pending->nestLevel = GetCurrentTransactionNestLevel();
	pending->next = pendingDeleteFiles;

	pendingDeleteFiles = pending;

	pfree(filePath);

	/*
	 * TODO Only on DFS need do this
	 * Make sure the connection to the corresponding tablespace has
	 * been cached.
	 *
	 * FileDoDeletesActions->UfsFileUnlink is called outside of the
	 * transaction, if we don't establish a connection here. we may
	 * face the issus of accessing the catalog outside of the
	 * transaction.
	 */
	//forceCacheUfsResource(dirTable->spcId);
}

void
FileAddCreatePendingEntry(Relation rel, Oid spcId, char *relativePath)
{
	PendingRelDeleteFile *pending;

	/* Add the relation to the list of stuff to delete at abort */
	pending = (PendingRelDeleteFile *)
		MemoryContextAlloc(TopMemoryContext, sizeof(PendingRelDeleteFile));
	pending->filenode.node = rel->rd_node;
	pending->filenode.relkind = rel->rd_rel->relkind;
	pending->filenode.relativePath = MemoryContextStrdup(TopMemoryContext, relativePath);
	pending->filenode.spcId = spcId;

	pending->atCommit = false;	/* delete if abort */
	pending->nestLevel = GetCurrentTransactionNestLevel();
	pending->next = pendingDeleteFiles;

	pendingDeleteFiles = pending;


	/*
	 * TODO same as before
	 * Make sure the spccache to the corresponding tablespace has
	 * been cached.
	 */
	//forceCacheUfsResource(spcId);
}

void
FileAddDeletePendingEntry(Relation rel, Oid spcId, char *relativePath)
{
	PendingRelDeleteFile *pending;

	/* Add the relation to the list of stuff to delete at abort */
	pending = (PendingRelDeleteFile *)
		MemoryContextAlloc(TopMemoryContext, sizeof(PendingRelDeleteFile));
	pending->filenode.node = rel->rd_node;
	pending->filenode.relkind = rel->rd_rel->relkind;
	pending->filenode.relativePath = MemoryContextStrdup(TopMemoryContext, relativePath);
	pending->filenode.spcId = spcId;

	pending->atCommit = true;	/* delete if commit */
	pending->nestLevel = GetCurrentTransactionNestLevel();
	pending->next = pendingDeleteFiles;

	pendingDeleteFiles = pending;

	/*
	 * TODO same as before
	 * Make sure the spccache to the corresponding tablespace has
	 * been cached.
	 */
	//forceCacheUfsResource(spcId);
}

void
FileDoDeletesActions(bool isCommit)
{
	int nestLevel = GetCurrentTransactionNestLevel();
	PendingRelDeleteFile *pending;
	PendingRelDeleteFile *prev;
	PendingRelDeleteFile *next;

	prev = NULL;
	for (pending = pendingDeleteFiles; pending != NULL; pending = next)
	{
		next = pending->next;
		if (pending->nestLevel < nestLevel)
		{
			/* outer-level entries should not be processed yet */
			prev = pending;
		}
		else
		{
			/* unlink list entry first, so we don't retry on failure */
			if (prev)
				prev->next = next;
			else
				pendingDeleteFiles = next;

			/* do deletion if called for */
			if (pending->atCommit == isCommit)
				UFileUnlink(pending->filenode.spcId, pending->filenode.relativePath);

			/* must explicitly free the list entry */
			if (pending->filenode.relativePath)
				pfree(pending->filenode.relativePath);

			pfree(pending);
			/* prev does not change */
		}
	}
}

void
FileAtSubCommitSmgr(void)
{
	int	nestLevel = GetCurrentTransactionNestLevel();
	PendingRelDeleteFile *pending;

	for (pending = pendingDeleteFiles; pending != NULL; pending = pending->next)
	{
		if (pending->nestLevel >= nestLevel)
			pending->nestLevel = nestLevel - 1;
	}
}

void
FileAtSubAbortSmgr(void)
{
	FileDoDeletesActions(false);
}
