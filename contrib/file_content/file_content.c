/*
 *      Defination of file_content function for directory table.
 *
 *      contrib/file_content.c
 *
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "access/relscan.h"
#include "access/tableam.h"
#include "catalog/pg_directory_table.h"
#include "commands/dirtablecmds.h"
#include "commands/tablespace.h"
#include "catalog/namespace.h"
#include "catalog/storage_directory_table.h"
#include "storage/ufile.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

PG_MODULE_MAGIC;

typedef struct TableFunctionContext
{
    Relation 		relation;
    TableScanDesc	scanDesc;
    TupleTableSlot	*slot;
    DirectoryTable 	*dirTable;
} TableFunctionContext;

/*
 * This is function for display file content in directory table
 */
static char *
getScopedFileUrl(DirectoryTable *dirTable, char *relativePath);

PG_FUNCTION_INFO_V1(file_content);
PG_FUNCTION_INFO_V1(directory_table_without_content);

Datum
file_content(PG_FUNCTION_ARGS)
{
	text *scopedurl = PG_GETARG_TEXT_PP(0);
	char *scopedUrl;
	char errorMessage[256];
	char buffer[4096];
	char *data;
	int curPos = 0;
	int bytesRead;
	Oid spcId;
	bytea *result;
	UFile *ufile;
	int64 fileSize;
	char *filePath;
	char *tablespace;

	/* /dir_tablespace/dir_table/animal/tab_a.bin */
    scopedUrl = text_to_cstring(scopedurl);
    filePath = strchr(scopedUrl + 1, '/');
    tablespace = pnstrdup(scopedUrl + 1, filePath - scopedUrl - 1);
    spcId = atoi(tablespace);

	ufile = UFileOpen(spcId,
			  scopedUrl,
			  O_RDONLY,
			  errorMessage,
			  sizeof(errorMessage));
	if (ufile == NULL)
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("failed to open file \"%s\": %s", scopedUrl, errorMessage)));

	fileSize = UFileSize(ufile);
	result = (bytea *) palloc(fileSize + VARHDRSZ);
	SET_VARSIZE(result, fileSize + VARHDRSZ);
	data = VARDATA(result);

	while (true)
	{
		bytesRead = UFileRead(ufile, buffer, sizeof(buffer));
		if (bytesRead == -1)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("failed to read file \"%s\": %s", scopedUrl, UFileGetLastError(ufile))));

	if (bytesRead == 0)
		break;

	memcpy(data + curPos, buffer, bytesRead);
	curPos += bytesRead;
	}

	UFileClose(ufile);

	PG_RETURN_BYTEA_P(result);
}

Datum
directory_table_without_content(PG_FUNCTION_ARGS)
{
#define DIRECTORY_TABLE_FUNCTION_COLUMNS	7
    text        *arg0 = PG_GETARG_TEXT_PP(0);
    char        *dirtable_name = text_to_cstring(arg0);
    Oid			relId;
    Datum 		values[DIRECTORY_TABLE_FUNCTION_COLUMNS];
    bool 		nulls[DIRECTORY_TABLE_FUNCTION_COLUMNS];
    HeapTuple 	tuple;
    Datum 		result;
    FuncCallContext *funcCtx;
    TableFunctionContext *context;

    relId = RelnameGetRelid(dirtable_name);
    if (!OidIsValid(relId))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("Can't find directory table \"%s\"", dirtable_name)));

    if (SRF_IS_FIRSTCALL())
    {
        Snapshot 		snapshot;
        TupleDesc 		newTupDesc;
        MemoryContext	oldContext;

        funcCtx = SRF_FIRSTCALL_INIT();

        oldContext = MemoryContextSwitchTo(funcCtx->multi_call_memory_ctx);

        newTupDesc = CreateTemplateTupleDesc(DIRECTORY_TABLE_FUNCTION_COLUMNS);
        TupleDescInitEntry(newTupDesc, (AttrNumber) 1, "scoped_file_url", TEXTOID, -1, 0);
        TupleDescInitEntry(newTupDesc, (AttrNumber) 2, "relative_path", TEXTOID, -1, 0);
        TupleDescInitEntry(newTupDesc, (AttrNumber) 3, "tag", TEXTOID, -1, 0);
        TupleDescInitEntry(newTupDesc, (AttrNumber) 4, "size", INT8OID, -1, 0);
        TupleDescInitEntry(newTupDesc, (AttrNumber) 5, "last_modified", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(newTupDesc, (AttrNumber) 6, "md5", TEXTOID, -1, 0);
        TupleDescInitEntry(newTupDesc, (AttrNumber) 7, "dirtable_oid", OIDOID, -1, 0);

        funcCtx->tuple_desc = BlessTupleDesc(newTupDesc);

        context = (TableFunctionContext *) palloc0(sizeof(TableFunctionContext));
        context->relation = table_open(relId, AccessShareLock);

        if (!RelationIsDirectoryTable(relId))
            ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("'%s' is not a directory table",
                                RelationGetRelationName(context->relation))));

        context->slot = MakeSingleTupleTableSlot(RelationGetDescr(context->relation),
                                                 table_slot_callbacks(context->relation));
        context->dirTable = GetDirectoryTable(RelationGetRelid(context->relation));

        snapshot = GetLatestSnapshot();
        context->scanDesc = table_beginscan(context->relation, snapshot, 0, NULL);

        funcCtx->user_fctx = (void *) context;

        MemoryContextSwitchTo(oldContext);
    }

    funcCtx = SRF_PERCALL_SETUP();
    context = (TableFunctionContext *) funcCtx->user_fctx;

    if (table_scan_getnextslot(context->scanDesc, ForwardScanDirection, context->slot))
    {
        Datum attrRelativePath;
        Datum attrSize;
        Datum attrLastModified;
        Datum attrMd5Sum;
        Datum attrTags;
        bool  isNull;
        char  *scopedFileUrl;

        slot_getallattrs(context->slot);

        attrRelativePath = slot_getattr(context->slot, 1, &isNull);
        Assert(isNull == false);
        attrSize = slot_getattr(context->slot, 2, &isNull);
        Assert(isNull == false);
        attrLastModified = slot_getattr(context->slot, 3, &isNull);
        Assert(isNull == false);
        attrMd5Sum = slot_getattr(context->slot, 4, &isNull);
        Assert(isNull == false);
        attrTags = slot_getattr(context->slot, 5, &isNull);

        MemSet(values, 0, sizeof(values));
        MemSet(nulls, 0, sizeof(nulls));

        scopedFileUrl = getScopedFileUrl(context->dirTable, TextDatumGetCString(attrRelativePath));

        values[0] = PointerGetDatum(cstring_to_text(scopedFileUrl));
        values[1] = attrRelativePath;
        values[2] = attrTags;
        nulls[2] = isNull;
        values[3] = attrSize;
        values[4] = attrLastModified;
        values[5] = attrMd5Sum;
        values[6] = ObjectIdGetDatum(relId);

        tuple = heap_form_tuple(funcCtx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT(funcCtx, result);
    }

    table_endscan(context->scanDesc);
    ExecDropSingleTupleTableSlot(context->slot);
    table_close(context->relation, AccessShareLock);

    pfree(context);
    funcCtx->user_fctx = NULL;

    SRF_RETURN_DONE(funcCtx);
}

static char *
getScopedFileUrl(DirectoryTable *dirTable, char *relativePath)
{
        return psprintf("%s/%s", dirTable->location, relativePath);
}
