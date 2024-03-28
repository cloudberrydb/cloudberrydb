/*-------------------------------------------------------------------------
 *
 * pg_directory_table.c
 *		  support for directory table.
 *
 * Copyright (c) 2016-Present Hashdata, Inc.
 *
 * IDENTIFICATION
 *		  src/backend/catalog/pg_directory_table.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "parser/parser.h"
#include "catalog/pg_directory_table.h"
#include "catalog/pg_tablespace.h"
#include "storage/ufile.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

typedef struct DirTableColumnDesc
{
	const char *colName;
	const char *typName;
} DirTableColumnDesc;

static const DirTableColumnDesc dirTableColumns[] = {
	{"relative_path", "text"},
	{"size", "int8"},
	{"last_modified", "timestamptz"},
	{"md5", "text"},
	{"tag", "text"}
};

static void
GetTablespaceFileHandler(Oid spcId)
{
	HeapTuple tuple;
	Datum datum;
	bool isNull;
	Oid	fileHandlerOid;
	Form_pg_tablespace tblspcForm;

	tuple = SearchSysCache1(TABLESPACEOID, ObjectIdGetDatum(spcId));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for table space %u", spcId);


	tblspcForm = (Form_pg_tablespace) GETSTRUCT(tuple);
	datum = SysCacheGetAttr(TABLESPACEOID,
							tuple,
							Anum_pg_tablespace_spcfilehandler,
							&isNull);
	if (!isNull)
	{
		fileHandlerOid = DatumGetObjectId(datum);
		datum = OidFunctionCall0(fileHandlerOid);
		currentFileAm = (FileAm *) DatumGetPointer(datum);
		if (currentFileAm == NULL)
			elog(ERROR, "tablespace file handler %u did not return a FileAm struct",
				 		fileHandlerOid);
	}
	else
	{
		currentFileAm = &localFileAm;
	}

	ReleaseSysCache(tuple);

	Assert(currentFileAm != NULL);
	Assert(currentFileAm->close != NULL);
	Assert(currentFileAm->read != NULL);
	Assert(currentFileAm->write != NULL);
	Assert(currentFileAm->size != NULL);
	Assert(currentFileAm->name != NULL);
	Assert(currentFileAm->getLastError != NULL);

	return;
}

/*
 * GetDirectoryTable - look up the directory table definition by relId.
 */
DirectoryTable *
GetDirectoryTable(Oid relId)
{
	Form_pg_directory_table dirtableForm;
	HeapTuple tuple;
	Datum datum;
	bool isNull;
	DirectoryTable *dirTable;

	tuple = SearchSysCache1(DIRECTORYTABLEREL, ObjectIdGetDatum(relId));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for directory table %u", relId);

	dirtableForm = (Form_pg_directory_table) GETSTRUCT(tuple);

	dirTable = (DirectoryTable *) palloc(sizeof(DirectoryTable));
	dirTable->relId = relId;
	dirTable->spcId = dirtableForm->dttablespace;
	GetTablespaceFileHandler(dirtableForm->dttablespace);

	datum = SysCacheGetAttr(DIRECTORYTABLEREL,
							tuple,
							Anum_pg_directory_table_dtlocation,
							&isNull);
	Assert(!isNull);

	dirTable->location = TextDatumGetCString(datum);
	ReleaseSysCache(tuple);

	return dirTable;
}

bool
RelationIsDirectoryTable(Oid relId)
{
	HeapTuple tuple;

	tuple = SearchSysCache1(DIRECTORYTABLEREL, ObjectIdGetDatum(relId));
	if (!HeapTupleIsValid(tuple))
		return false;

	ReleaseSysCache(tuple);
	return true;
}

List *
GetDirectoryTableBuiltinColumns(void)
{
	int i;
	List *result = NIL;

	for (i = 0; i < lengthof(dirTableColumns); i++)
	{
		ColumnDef *columnDef = makeNode(ColumnDef);

		columnDef->colname = pstrdup(dirTableColumns[i].colName);
		columnDef->typeName = SystemTypeName(pstrdup(dirTableColumns[i].typName));
		columnDef->is_local = true;

		if (i == 0)
		{
			Constraint *constraint = makeNode(Constraint);
			constraint->contype = CONSTR_PRIMARY;
			constraint->location = -1;
			constraint->keys = NIL;
			constraint->options = NIL;
			constraint->indexname = NULL;
			constraint->indexspace = NULL;
			columnDef->constraints = list_make1(constraint);
		}

		result = lappend(result, columnDef);
	}

	return result;
}

void
RemoveDirectoryTableEntry(Oid relId)
{
	Relation	rel;
	HeapTuple 	tuple;

	rel = table_open(DirectoryTableRelationId, RowExclusiveLock);

	tuple = SearchSysCache1(DIRECTORYTABLEREL, ObjectIdGetDatum(relId));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for directory table %u", relId);

	CatalogTupleDelete(rel, &tuple->t_self);

	ReleaseSysCache(tuple);
	table_close(rel, RowExclusiveLock);
}
