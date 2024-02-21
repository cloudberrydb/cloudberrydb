#include "postgres.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "nodes/parsenodes.h"
#include "gopher/gopher.h"

#include "src/provider/iceberg/iceberg_task_reader.h"
#include "src/provider/hudi/hudi_task_reader.h"
#include "row_reader.h"

extern bool disableCacheFile;

static bool resownerCallbackRegistered;
static RemoteFileHandle *openRemoteHandles;

static void flatCombinedTasks(List *combinedScanTasks, List **fileScanTasks);
static bool isCacheEnabled(char *cacheEnabled);

static RemoteFileHandle *
createRemoteFileHandle(gopherFS gopherFilesystem)
{
	RemoteFileHandle *result;

	result = MemoryContextAlloc(TopMemoryContext, sizeof(RemoteFileHandle));
	result->gopherFilesystem = gopherFilesystem;
	result->reader = NULL;
	result->prev = NULL;
	result->next = openRemoteHandles;
	result->owner = CurrentResourceOwner;

	if (openRemoteHandles)
		openRemoteHandles->prev = result;

	openRemoteHandles = result;

	return result;
}

static void
destroyRemoteFileHandle(RemoteFileHandle *handle)
{
	if (handle->prev)
		handle->prev->next = handle->next;
	else
		openRemoteHandles = openRemoteHandles->next;

	if (handle->next)
		handle->next->prev = handle->prev;

	if (handle->gopherFilesystem)
		gopherDisconnect(handle->gopherFilesystem);

	if (handle->reader)
		rowReaderClose(handle->reader);

	pfree(handle);
}

static void
remoteFileAbortCallback(ResourceReleasePhase phase,
						bool isCommit,
						bool isTopLevel,
						void *arg)
{
	RemoteFileHandle *cur;
	RemoteFileHandle *next;

	if (phase != RESOURCE_RELEASE_AFTER_LOCKS)
		return;

	next = openRemoteHandles;
	while (next)
	{
		cur = next;
		next = cur->next;

		if (cur->owner == CurrentResourceOwner)
			destroyRemoteFileHandle(cur);
	}
}

static Reader icebergHandler = {
	createIcebergTaskReader,
	icebergTaskReaderNext,
	icebergTaskReaderClose
};

static Reader hudiHandler = {
	createHudiTaskReader,
	hudiTaskReaderNext,
	hudiTaskReaderClose
};

static Reader deltaLakeHandler = {
	NULL,
	NULL,
	NULL
};

RowReader *
createRowReader(MemoryContext mcxt,
				TupleDesc tupleDesc,
				bool *attrUsed,
				gopherFS gopherFilesystem,
				List *combinedScanTasks,
				char *format,
				ExternalTableMetadata *tableOptions)
{
	RowReader *reader = palloc0(sizeof(RowReader));

	flatCombinedTasks(combinedScanTasks, &reader->fileScanTasks);
	list_free(combinedScanTasks);

	reader->datafileDesc = createFieldDescription(tupleDesc);
	reader->attrUsed = attrUsed;
	reader->gopherFilesystem = gopherFilesystem;
	reader->mcxt = mcxt;
	reader->tableOptions = tableOptions;

	if (FORMAT_IS_ICEBERG(format))
		reader->handler = &icebergHandler;
	else if (FORMAT_IS_HUDI(format))
		reader->handler = &hudiHandler;
	else
		reader->handler = &deltaLakeHandler;

	reader->taskMcxt = AllocSetContextCreate(CurrentMemoryContext,
											 "RowReaderContext",
											 ALLOCSET_DEFAULT_MINSIZE,
											 ALLOCSET_DEFAULT_INITSIZE,
											 ALLOCSET_DEFAULT_MAXSIZE);
	reader->curMcxt = CurrentMemoryContext;

	return reader;
}

bool
rowReaderNext(RowReader *reader, InternalRecord *record)
{
	while (true)
	{
		if (reader->handler->Next(reader->curReader, record))
		{
			return true;
		}
		else if (list_length(reader->fileScanTasks) > 0)
		{
			ReaderInitInfo initInfo;

			reader->handler->Close(reader->curReader);
			MemoryContextSwitchTo(reader->curMcxt);

			initInfo.taskId = reader->curReaderIndex++;
			initInfo.mcxt = reader->mcxt;
			initInfo.datafileDesc = reader->datafileDesc;
			initInfo.attrUsed = reader->attrUsed;
			initInfo.gopherFilesystem = reader->gopherFilesystem;
			initInfo.fileScanTask = list_nth(reader->fileScanTasks, 0);
			initInfo.tableOptions = reader->tableOptions;

			MemoryContextReset(reader->taskMcxt);
			MemoryContextSwitchTo(reader->taskMcxt);

			reader->curReader = reader->handler->Create(&initInfo);
			reader->fileScanTasks = list_delete_first(reader->fileScanTasks);
		}
		else
		{
			reader->handler->Close(reader->curReader);
			MemoryContextSwitchTo(reader->curMcxt);
			reader->curReader = NULL;
			return false;
		}
	}

	return false;
}

void
rowReaderClose(RowReader *reader)
{
	if (reader->curReader)
		reader->handler->Close(reader->curReader);

	list_free_deep(reader->datafileDesc);
	MemoryContextDelete(reader->taskMcxt);
	pfree(reader);
}

static void
flatCombinedTasks(List *combinedScanTasks, List **fileScanTasks)
{
	ListCell *lco;
	ListCell *lci;
	List *combinedScanTask;

	foreach(lco, combinedScanTasks)
	{
		combinedScanTask = (List *) lfirst(lco);

		foreach(lci, combinedScanTask)
		{
			FileScanTask *task = (FileScanTask *) lfirst(lci);

			*fileScanTasks = lappend(*fileScanTasks, task);
		}

		list_free(combinedScanTask);
	}
}

static bool
checkInterrupt(void)
{
	if (!InterruptPending)
		return false;

	if (InterruptHoldoffCount != 0 || CritSectionCount != 0)
		return false;

	return true;
}

static void GetGopherSocketPath(char *dest)
{
	snprintf(dest, 1024, "/tmp/.s.gopher.%d", PostPortNumber);
}

ProtocolContext *
createContext(dataLakeOptions *options)
{
	ProtocolContext *context;
	HdfsConfigInfo  *hdfsConfig;
	gopherConfig    *gopherConfig;
	gopherFS        fs;
	const char      *serverName = options->server_name;

	context = (ProtocolContext *)palloc0(sizeof(ProtocolContext));

	if (!serverName)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid URI, \"server_name\" option(s) missing")));

	hdfsConfig = parseHdfsConfig("gphdfs.conf", options->hdfs_cluster_name);
	if (!hdfsConfig)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("server \"%s\" is not found in gphdfs.conf", serverName)));

	disableCacheFile = !isCacheEnabled(options->cache_enabled);

	formKrbCCName(hdfsConfig);

	char connect_path[1024] = {0};
	GetGopherSocketPath(connect_path);
	hdfsConfig->gopherPath = pstrdup(connect_path);

	gopherConfig = gopherCreateConfig(hdfsConfig);
	gopherUserCanceledCallBack(&checkInterrupt);

	fs = gopherConnect(*gopherConfig);
	if (fs == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to connect to gopher: %s", gopherGetLastError())));

	gopherConfigDestroy(gopherConfig);

	if (!resownerCallbackRegistered)
	{
		RegisterResourceReleaseCallback(remoteFileAbortCallback, NULL);
		resownerCallbackRegistered = true;
	}

	context->file = createRemoteFileHandle(fs);

	return context;
}

static bool
isCacheEnabled(char *cacheEnabled)
{
	if (cacheEnabled)
	{
		if (pg_strcasecmp(cacheEnabled, "false") == 0)
			return false;
		else if (pg_strcasecmp(cacheEnabled, "true") == 0)
			return true;
		else
			ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid value for boolean option \"cache_enabled\": %s", cacheEnabled)));
	}

	return true;
}

void
cleanupContext(ProtocolContext *context)
{
	if (context->file)
		destroyRemoteFileHandle(context->file);

	if (context->rowContext)
		MemoryContextDelete(context->rowContext);

	if (context->record)
		pfree(context->record);

	pfree(context);
}

void
protocolImportStart(dataLakeFdwScanState *scanstate, ProtocolContext *context, bool *attrUsed)
{
	int       i = 0;
	ListCell *lc;
	int       numSegments = getgpsegmentCount();
	List     *combinedScanTasks = NIL;

    ExternalTableMetadata *tableOptions = (ExternalTableMetadata *)linitial(scanstate->fragments);
	scanstate->fragments = list_delete_first(scanstate->fragments);

	foreach_with_count(lc, scanstate->fragments, i)
	{
		List *combinedScanTask = (List *) lfirst(lc);

		if (GpIdentity.segindex == (i % numSegments))
			combinedScanTasks = lappend(combinedScanTasks, combinedScanTask);
	}

	context->rowContext = AllocSetContextCreate(CurrentMemoryContext,
												"DatalakeExtensionContext",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);

	context->file->reader = createRowReader(context->rowContext,
											scanstate->rel->rd_att,
											attrUsed,
											context->file->gopherFilesystem,
											combinedScanTasks,
											scanstate->options->format,
											tableOptions);
}
