/*-------------------------------------------------------------------------
 *
 * NOTES:
 *
 * This file contains an example table am that demonstrates how to use ufs
 * api(ufs.h)
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "fmgr.h"
#include "pgstat.h"
#include "nodes/pg_list.h"
#include "access/tableam.h"
#include "access/heapam.h"
#include "access/amapi.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"
#include "utils/wait_event.h"
#include "utils/fmgroids.h"
#include "utils/builtins.h"

#include "ufs.h"
#include <vbf.h>

PG_FUNCTION_INFO_V1(ufsdemo_am_handler);

typedef struct UfsDemoScanDescData
{
	TableScanDescData  rs_base;	/* AM independent part of the descriptor */
	int				   nScanKeys;
	ScanKey			   scanKey;
	MemTupleBinding	  *mtBind;
	UfsFile			  *curDataFile;
	List			  *dataFiles;
	vbf_reader_t       reader;
	int				   filesProcessed;
} UfsDemoScanDescData;
typedef struct UfsDemoScanDescData *UfsDemoScanDesc;

typedef struct VbfRandomFile {
	vbf_file_t  base;
	UfsFile    *file;
} VbfRandomFile;

static vbf_file_t *
randomFileOpen(const char *file, int flags);
static void
randomFileClose(vbf_file_t *file);
static int
randomFileRead(vbf_file_t *file, char *buffer, int amount, off_t offset);
static int
randomFileWrite(vbf_file_t *file, char *buffer, int amount, off_t offset);
static off_t
randomFileSeek(vbf_file_t *file, off_t offset, int whence);
static int
randomFileSync(vbf_file_t *file);
static int64_t
randomFileSize(vbf_file_t *file);
static int
randomFileTruncate(vbf_file_t *file, int64_t size);
const char *
randomFileName(vbf_file_t *file);
static int
randomFileRemove(const char *file_name);
static int
randomFileGetCreateFlags(void);
static int
randomFileGetOpenFlags(bool isRead);

bool isInitialized = false;

static vbf_vfs_t vbfOssVfs = {
	"ufs",
	randomFileOpen,
	randomFileRemove,
	randomFileGetCreateFlags,
	randomFileGetOpenFlags,
	{
		randomFileClose,
		randomFileRead,
		randomFileWrite,
		randomFileSeek,
		randomFileSync,
		randomFileSize,
		randomFileTruncate,
		randomFileName
	}
};

static vbf_file_t *
randomFileOpen(const char *file, int flags)
{
	VbfRandomFile *result;

	result = (VbfRandomFile *) palloc(sizeof(VbfRandomFile));
	result->file = (UfsFile *) file;

	return (vbf_file_t *) result;
}

static void
randomFileClose(vbf_file_t *file)
{
	pfree(file);
}

static int
randomFileRead(vbf_file_t *file, char *buffer, int amount, off_t offset)
{
	int bytes;
	UfsFile *ufsFile = ((VbfRandomFile *) file)->file;

	bytes = UfsFilePread(ufsFile, buffer, amount, offset);
	if (bytes < 0)
	{
		vbf_set_error("failed to read: %s", UfsGetLastError(ufsFile));
		return -1;
	}
	return bytes;
}

static int
randomFileWrite(vbf_file_t *file, char *buffer, int amount, off_t offset)
{
	int bytes;
	UfsFile *ufsFile = ((VbfRandomFile *) file)->file;

	bytes = UfsFilePwrite(ufsFile, buffer, amount, offset);
	if (bytes < 0)
	{
		vbf_set_error("failed to write: %s", UfsGetLastError(ufsFile));
		return -1;
	}
	return bytes;
}

static off_t
randomFileSeek(vbf_file_t *file, off_t offset, int whence)
{
	off_t result;
	UfsFile *ufsFile = ((VbfRandomFile *) file)->file;

	result = UfsFileSeek(ufsFile, offset);
	if (result < 0)
	{
		vbf_set_error("failed to seek file: %s", UfsGetLastError(ufsFile));
		return -1;
	}
	return result;
}

static int
randomFileSync(vbf_file_t *file)
{
	elog(ERROR, "operation randomFileSync is not supported");
	return 0;
}

static int64_t
randomFileSize(vbf_file_t *file)
{
	int64_t result;
	UfsFile *ufsFile = ((VbfRandomFile *) file)->file;

	result = UfsFileSize(ufsFile);
	if (result < 0)
	{
		vbf_set_error("failed to get file size: %s", UfsGetLastError(ufsFile));
		return -1;
	}
	return result;
}

static int
randomFileTruncate(vbf_file_t *file, int64_t size)
{
	elog(ERROR, "operation randomFileTruncate is not supported");
	return 0;
}

const char *
randomFileName(vbf_file_t *file)
{
	UfsFile *ufsFile = ((VbfRandomFile *) file)->file;

	return UfsFileName(ufsFile);
}

static int
randomFileRemove(const char *file_name)
{
	elog(ERROR, "operation randomFileRemove is not supported");
	return 0;
}

static int
randomFileGetCreateFlags(void)
{
	return O_CREAT | O_EXCL;
}

static int
randomFileGetOpenFlags(bool isRead)
{
	return isRead ? O_RDONLY : O_RDWR;
}

static vbf_vfs_t *
randomFileGetVfs()
{
	return &vbfOssVfs;
}

static const TupleTableSlotOps *
ufsdemo_slot_callbacks(Relation relation)
{
	return &TTSOpsVirtual;
}

static List *
retrieveDataFiles(RelFileNode *relFileNode, const char *metadataFile)
{
	char c;
	int i = 0;
	int bytes;
	UfsFile *file;
	char buffer[MAXPGPATH];
	char errorMessage[4096];
	List *result = NIL;

	file = UfsFileOpenEx(relFileNode, metadataFile, O_RDONLY, errorMessage, sizeof(errorMessage));
	if (file == NULL)
		elog(ERROR, "failed to open metadata file \"%s\": %s", metadataFile, errorMessage);

	while (true)
	{
		bytes = UfsFileRead(file, &c, 1);
		if (bytes == -1)
			elog(ERROR, "failed to read metadata file \"%s\": %s", metadataFile, UfsGetLastError(file));

		if (bytes == 0)
			break;

		if (c != '\n')
		{
			buffer[i++] = c;
			continue;
		}

		buffer[i] = '\0';
		result = lappend(result, pstrdup(buffer));
		i = 0;
    }

	UfsFileClose(file);

	return result;
}

static char*
formatMetadataFileName(RelFileNode *relFileNode)
{
	return psprintf("%lu.metadata", relFileNode->relNode);
}

static void
updateMetadataFile(RelFileNode *relFileNode, char *dataFileName)
{
	int len;
	UfsFile *file;
	ListCell *lc;
	List *dataFiles;
	char errorMessage[4096];
	char *fullPathName;
	char *metadataFile = formatMetadataFileName(relFileNode);

	dataFiles = retrieveDataFiles(relFileNode, metadataFile);

	fullPathName = UfsMakeFullPathName(relFileNode, metadataFile);
	UfsFileUnlinkEx(relFileNode->spcNode, fullPathName);
	pfree(fullPathName);

	file = UfsFileOpenEx(relFileNode,
						 metadataFile,
						 O_CREAT | O_WRONLY,
						 errorMessage,
						 sizeof(errorMessage));
	if (file == NULL)
		elog(ERROR, "failed to create metadata file \"%s\": %s", metadataFile, errorMessage);

	foreach(lc, dataFiles)
	{
		char *fileName = (char *) lfirst(lc);

		len = strlen(fileName);
		fileName[len] = '\n';
		if (UfsFileWrite(file, fileName, len + 1) < 0)
			elog(ERROR, "failed to write metadata file \"%s\": %s", metadataFile, UfsGetLastError(file));
	}

	len = strlen(dataFileName);
	dataFileName[len] = '\n';
	if (UfsFileWrite(file, dataFileName, len+1) < 0)
		elog(ERROR, "failed to write metadata file \"%s\": %s", metadataFile, UfsGetLastError(file));

	UfsFileClose(file);
}

static const char*
formatDataFileFileName(void)
{
	char *result;
	Datum uuid;

	uuid = DirectFunctionCall1(gen_random_uuid, PointerGetDatum(NULL));
	result = DatumGetPointer(DirectFunctionCall1(uuid_out, uuid));

	return psprintf("%s.data", result);
}

static off_t
getFileSize(UfsFile *file)
{
	off_t result;

	result = UfsFileSize(file);
	if (result == -1)
		elog(ERROR, "failed to retrieve file size %ld", result);

	return result;
}

static TableScanDesc
ufsdemo_beginscan(Relation relation,
				  Snapshot snapshot,
				  int nkeys, struct ScanKeyData *key,
				  ParallelTableScanDesc pscan,
				  uint32 flags)
{
	char errorMessage[4096];
	UfsDemoScanDesc scan;
	RelFileNode relFileNode = relation->rd_node;

	if (!isInitialized) {
		vbf_register_vfs(randomFileGetVfs());
		isInitialized = true;
	}

	scan = (UfsDemoScanDesc) palloc0(sizeof(UfsDemoScanDescData));

	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = pscan;

	scan->nScanKeys = nkeys;

	if (nkeys > 0)
		scan->scanKey = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);

	if (key != NULL)
		memcpy(scan->scanKey, key, scan->nScanKeys * sizeof(ScanKeyData));

	scan->dataFiles = retrieveDataFiles(&relFileNode, formatMetadataFileName(&relFileNode));
	scan->filesProcessed = 0;

	if (vbf_reader_init(&scan->reader, "none", 0, 64 * 1024, 0, NULL) == -1)
		elog(ERROR, "failed to initialize vbf reader: %s", vbf_strerror());

	if (list_length(scan->dataFiles) > 0)
	{
		scan->curDataFile = UfsFileOpen(&relFileNode,
										list_nth(scan->dataFiles, scan->filesProcessed),
										O_RDONLY,
										errorMessage,
										sizeof(errorMessage));
		if (scan->curDataFile == NULL)
			elog(ERROR, "failed to open data file \"%s\": %s",
						(char *) list_nth(scan->dataFiles, scan->filesProcessed),
						errorMessage);

		if (vbf_reader_reset(&scan->reader, (char *) scan->curDataFile, getFileSize(scan->curDataFile)) == -1)
			elog(ERROR, "failed to open data file \"%s\": %s", UfsFileName(scan->curDataFile), vbf_strerror());
	}

	scan->mtBind = create_memtuple_binding(RelationGetDescr(relation));

	return (TableScanDesc) scan;
}

static void
ufsdemo_endscan(TableScanDesc scan)
{
	UfsDemoScanDesc demoScan = (UfsDemoScanDesc) scan;

	if (demoScan->scanKey)
		pfree(demoScan->scanKey);

	vbf_reader_fini(&demoScan->reader);
	destroy_memtuple_binding(demoScan->mtBind);

	list_free_deep(demoScan->dataFiles);
	pfree(demoScan);
}

static void
ufsdemo_rescan(TableScanDesc scan, ScanKey key,
			   bool set_params, bool allow_strat,
			   bool allow_sync, bool allow_pagemode)
{
	elog(ERROR, "operation ufsdemo_rescan is not supported");
}

static bool
ufsdemo_getnexttuple(UfsDemoScanDesc scan,
					 ScanDirection dir pg_attribute_unused(),
					 int nkeys,
					 ScanKey key,
					 TupleTableSlot *slot)
{
	uint8_t *data;
	bool     hasNext;
	int      dataLength;
	char     errorMessage[4096];
	RelFileNode relFileNode = scan->rs_base.rs_rd->rd_node;

	Assert(ScanDirectionIsForward(dir));

	if (scan->filesProcessed == list_length(scan->dataFiles))
		return false;

	while (true)
	{
		if (vbf_reader_next(&scan->reader, &data, &dataLength, &hasNext) == -1)
			elog(ERROR, "failed to read data file \"%s\": %s", UfsFileName(scan->curDataFile), vbf_strerror());

		if (hasNext)
		{
			ExecClearTuple(slot);
			memtuple_deform((MemTuple) data, scan->mtBind, slot->tts_values, slot->tts_isnull);
			ExecStoreVirtualTuple(slot);
			return true;
		}
		else if (scan->filesProcessed < list_length(scan->dataFiles) - 1)
		{
			UfsFileClose(scan->curDataFile);
			scan->filesProcessed++;
			scan->curDataFile = UfsFileOpen(&relFileNode,
											list_nth(scan->dataFiles, scan->filesProcessed),
											O_RDONLY,
											errorMessage,
											sizeof(errorMessage));
			if (scan->curDataFile == NULL)
				elog(ERROR, "failed to open data file \"%s\": %s", 
							(char *) list_nth(scan->dataFiles, scan->filesProcessed),
							errorMessage);

			if (vbf_reader_reset(&scan->reader, (char *) scan->curDataFile, getFileSize(scan->curDataFile)) == -1)
				elog(ERROR, "failed to open data file \"%s\": %s", UfsFileName(scan->curDataFile), vbf_strerror());
		}
		else
		{
			UfsFileClose(scan->curDataFile);
			return false;
		}
	}

	return false;
}

static bool
ufsdemo_getnextslot(TableScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
{
	UfsDemoScanDesc demoScan = (UfsDemoScanDesc) scan;

	while (ufsdemo_getnexttuple(demoScan, direction, demoScan->nScanKeys, demoScan->scanKey, slot))
	{
		pgstat_count_heap_getnext(demoScan->rs_base.rs_rd);
		return true;
	}

	if (slot)
		ExecClearTuple(slot);

	return false;
}

static Size
ufsdemo_parallelscan_estimate(Relation rel)
{
	return sizeof(ParallelBlockTableScanDescData);
}

static Size
ufsdemo_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	ParallelBlockTableScanDesc bpscan = (ParallelBlockTableScanDesc) pscan;

	bpscan->base.phs_relid = RelationGetRelid(rel);
	bpscan->phs_nblocks = 0; /* init, will be updated later by table_parallelscan_initialize */
	pg_atomic_init_u64(&bpscan->phs_nallocated, 0);
	/* we don't need phs_mutex and phs_startblock in ufsdemo, though, init them. */
	SpinLockInit(&bpscan->phs_mutex);
	bpscan->phs_startblock = InvalidBlockNumber;
	return sizeof(ParallelBlockTableScanDescData);
}

static void
ufsdemo_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	ParallelBlockTableScanDesc bpscan = (ParallelBlockTableScanDesc) pscan;

	pg_atomic_write_u64(&bpscan->phs_nallocated, 0);
}

static IndexFetchTableData *
ufsdemo_index_fetch_begin(Relation rel)
{
	elog(ERROR, "operation ufsdemo_index_fetch_begin is not supported");

	return NULL;
}

static void
ufsdemo_index_fetch_reset(IndexFetchTableData *scan)
{
	elog(ERROR, "operation ufsdemo_index_fetch_reset is not supported");
}

static void
ufsdemo_index_fetch_end(IndexFetchTableData *scan)
{
	elog(ERROR, "operation ufsdemo_index_fetch_end is not supported");
}

static bool
ufsdemo_index_fetch_tuple(struct IndexFetchTableData *scan,
						  ItemPointer tid,
						  Snapshot snapshot,
						  TupleTableSlot *slot,
						  bool *call_again, bool *all_dead)
{
	elog(ERROR, "operation ufsdemo_index_fetch_tuple is not supported");
	return false;
}

static void
ufsdemo_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
					int options, BulkInsertState bistate)
{
	int size;
	char errorMessage[4096];
	vbf_writer_t writer;
	MemTupleBinding *mtBind;
	MemTuple mtuple;
	RelFileNode relFileNode = relation->rd_node;
	const char *fileName;
	UfsFile *file;

	if (!isInitialized) {
		vbf_register_vfs(randomFileGetVfs());
		isInitialized = true;
	}

	fileName = formatDataFileFileName();
	file = UfsFileOpen(&relFileNode,
					   fileName,
					   O_CREAT | O_WRONLY,
					   errorMessage,
					   sizeof(errorMessage));
	if (file == NULL)
		elog(ERROR, "failed to create data file \"%s\": %s", fileName, errorMessage);

	if (vbf_writer_init(&writer,
						"none", /* compress_type: none, zlib, zstd */
						0, /* compress_level: 0-9 */
						64 * 1024, /* block_size: 8192-2097152 */
						(char *) file,
						true, /* is_create_file */
						0, /* file_length */
						0, /* rownum */
						0, /* dbid */
						NULL /* vbf callback function */) == -1)
		elog(ERROR, "failed to initialize vbf writer: %s", vbf_strerror());

	mtBind = create_memtuple_binding(RelationGetDescr(relation));
	mtuple = appendonly_form_memtuple(slot, mtBind);
	slot->tts_tableOid = RelationGetRelid(relation);

	size = memtuple_get_size(mtuple);

	if (vbf_writer_write(&writer, (uint8_t *) mtuple, size) == -1)
		elog(ERROR, "failed to write data file %s: %s", fileName, vbf_strerror());

	if (vbf_writer_flush(&writer) == -1)
		elog(ERROR, "failed to flush data file %s: %s", fileName, vbf_strerror());

	vbf_writer_fini(&writer);
	appendonly_free_memtuple(mtuple);

	updateMetadataFile(&relFileNode, (char *) fileName);

	pgstat_count_heap_insert(relation, 1);
	UfsFileClose(file);
}

static void
ufsdemo_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
								CommandId cid, int options,
								BulkInsertState bistate, uint32 specToken)
{
	elog(ERROR, "speculative insertion not supported on ufsdemo table");
}

static void
ufsdemo_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
								  uint32 specToken, bool succeeded)
{
	elog(ERROR, "ufsdemo_tuple_complete_speculative not supported on ufsdemo table");
}

static void
ufsdemo_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
						CommandId cid, int options, BulkInsertState bistate)
{
	elog(ERROR, "ufsdemo_multi_insert not supported on ufsdemo table");
}

static TM_Result
ufsdemo_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
					Snapshot snapshot, Snapshot crosscheck, bool wait,
					TM_FailureData *tmfd, bool changingPart)
{
	elog(ERROR, "ufsdemo_tuple_update not supported on ufsdemo table");
	return TM_Ok;
}


static TM_Result
ufsdemo_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
					CommandId cid, Snapshot snapshot, Snapshot crosscheck,
					bool wait, TM_FailureData *tmfd,
					LockTupleMode *lockmode, bool *update_indexes)
{
	elog(ERROR, "ufsdemo_tuple_update not supported on ufsdemo table");
	return TM_Ok;
}

static TM_Result
ufsdemo_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
				  TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
				  LockWaitPolicy wait_policy, uint8 flags,
				  TM_FailureData *tmfd)
{
	elog(ERROR, "ufsdemo_tuple_lock insertion not supported on ufsdemo tables");
}

static void
ufsdemo_finish_bulk_insert(Relation relation, int options)
{
	elog(ERROR, "ufsdemo_finish_bulk_insert not supported on ufsdemo tables");
}

static bool
ufsdemo_fetch_row_version(Relation relation,
						 ItemPointer tid,
						 Snapshot snapshot,
						 TupleTableSlot *slot)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("feature not supported on ufsdemo relations")));
}

static void
ufsdemo_get_latest_tid(TableScanDesc sscan,
						  ItemPointer tid)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("feature not supported on ufsdemo relations")));
}

static bool
ufsdemo_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("feature not supported on ufsdemo relations")));
}

static bool
ufsdemo_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
								Snapshot snapshot)
{
	ereport(ERROR,
	        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		        errmsg("feature not supported on ufsdemo relations")));
}


static TransactionId
ufsdemo_index_delete_tuples(Relation rel,
						 TM_IndexDeleteOp *delstate)
{
	elog(ERROR, "feature not supported on ufsdemo relations");
}

static void
ufsdemo_relation_set_new_filenode(Relation rel,
								  const RelFileNode *newrnode,
								  char persistence,
								  TransactionId *freezeXid,
								  MultiXactId *minmulti)
{
	UfsFile *file;
	char errorMessage[4096];
	RelFileNode relFileNode = rel->rd_node;
	char *metadataFile = formatMetadataFileName(&relFileNode);

	file = UfsFileOpenEx(&relFileNode, metadataFile, O_CREAT, errorMessage, sizeof(errorMessage));
	if (file == NULL)
		elog(ERROR, "failed to create metadata file \"%s\": %s", metadataFile, errorMessage);

	UfsFileClose(file);
}

static void
ufsdemo_relation_nontransactional_truncate(Relation rel)
{
	elog(ERROR, "feature not supported on ufsdemo relations");
}

static void
ufsdemo_relation_copy_data(Relation rel, const RelFileNode *newrnode)
{
	elog(ERROR, "feature not supported on ufsdemo relations");
}

static void
ufsdemo_vacuum_rel(Relation onerel, VacuumParams *params,
					  BufferAccessStrategy bstrategy)
{
	elog(ERROR, "feature not supported on ufsdemo relations");
}

static void
ufsdemo_relation_copy_for_cluster(Relation OldHeap, Relation NewHeap,
								 Relation OldIndex, bool use_sort,
								 TransactionId OldestXmin,
								 TransactionId *xid_cutoff,
								 MultiXactId *multi_cutoff,
								 double *num_tuples,
								 double *tups_vacuumed,
								 double *tups_recently_dead)
{
	elog(ERROR, "feature not supported on ufsdemo relations");
}

static bool
ufsdemo_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
							   BufferAccessStrategy bstrategy)
{
	elog(ERROR, "feature not supported on ufsdemo relations");
}

static bool
ufsdemo_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
							   double *liverows, double *deadrows,
							   TupleTableSlot *slot)
{
	elog(ERROR, "feature not supported on ufsdemo relations");
	return false;
}

static double
ufsdemo_index_build_range_scan(Relation heapRelation,
							  Relation indexRelation,
							  IndexInfo *indexInfo,
							  bool allow_sync,
							  bool anyvisible,
							  bool progress,
							  BlockNumber start_blockno,
							  BlockNumber numblocks,
							  IndexBuildCallback callback,
							  void *callback_state,
							  TableScanDesc scan)
{
	elog(ERROR, "feature not supported on ufsdemo relations");
	return 10;
}

static void
ufsdemo_index_validate_scan(Relation heapRelation,
						   Relation indexRelation,
						   IndexInfo *indexInfo,
						   Snapshot snapshot,
						   ValidateIndexState *state)
{
	elog(ERROR, "feature not supported on ufsdemo relations");
}

static uint64
ufsdemo_relation_size(Relation rel, ForkNumber forkNumber)
{
	return 1024 * 1024 * 12;
}

static bool
ufsdemo_relation_needs_toast_table(Relation rel)
{
	return false;
}

static Oid
ufsdemo_relation_toast_am(Relation rel)
{
    return HEAP_TABLE_AM_OID;
}

/* ------------------------------------------------------------------------
 * Planner related callbacks for the appendonly AM
 * ------------------------------------------------------------------------
 */
static void
ufsdemo_estimate_rel_size(Relation rel, int32 *attr_widths,
						 BlockNumber *pages, double *tuples,
						 double *allvisfrac)
{
	*attr_widths = 512;
	*pages = 512;
	*tuples = 1024 * 512;
	*allvisfrac = 0;
}

static bool
ufsdemo_scan_bitmap_next_block(TableScanDesc scan,
								  TBMIterateResult *tbmres)
{
	return false;
}

static bool
ufsdemo_scan_bitmap_next_tuple(TableScanDesc scan,
								  TBMIterateResult *tbmres,
								  TupleTableSlot *slot)
{
	return false;
}

static bool
ufsdemo_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
{
	return false;
}

static bool
ufsdemo_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate,
							  TupleTableSlot *slot)
{
	return false;
}

static const TableAmRoutine ufsdemo_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = ufsdemo_slot_callbacks,

	.scan_begin = ufsdemo_beginscan,
	.scan_end = ufsdemo_endscan,
	.scan_rescan = ufsdemo_rescan,
	.scan_getnextslot = ufsdemo_getnextslot,

	.parallelscan_estimate = ufsdemo_parallelscan_estimate,
	.parallelscan_initialize = ufsdemo_parallelscan_initialize,
	.parallelscan_reinitialize = ufsdemo_parallelscan_reinitialize,

	.index_fetch_begin = ufsdemo_index_fetch_begin,
	.index_fetch_reset = ufsdemo_index_fetch_reset,
	.index_fetch_end = ufsdemo_index_fetch_end,
	.index_fetch_tuple = ufsdemo_index_fetch_tuple,

	.tuple_insert = ufsdemo_tuple_insert,
	.tuple_insert_speculative = ufsdemo_tuple_insert_speculative,
	.tuple_complete_speculative = ufsdemo_tuple_complete_speculative,
	.multi_insert = ufsdemo_multi_insert,
	.tuple_delete = ufsdemo_tuple_delete,
	.tuple_update = ufsdemo_tuple_update,
	.tuple_lock = ufsdemo_tuple_lock,
	.finish_bulk_insert = ufsdemo_finish_bulk_insert,

	.tuple_fetch_row_version = ufsdemo_fetch_row_version,
	.tuple_get_latest_tid = ufsdemo_get_latest_tid,
	.tuple_tid_valid = ufsdemo_tuple_tid_valid,
	.tuple_satisfies_snapshot = ufsdemo_tuple_satisfies_snapshot,
	.index_delete_tuples = ufsdemo_index_delete_tuples,

	.relation_set_new_filenode = ufsdemo_relation_set_new_filenode,
	.relation_nontransactional_truncate = ufsdemo_relation_nontransactional_truncate,
	.relation_copy_data = ufsdemo_relation_copy_data,
	.relation_copy_for_cluster = ufsdemo_relation_copy_for_cluster,
	.relation_vacuum = ufsdemo_vacuum_rel,
	.scan_analyze_next_block = ufsdemo_scan_analyze_next_block,
	.scan_analyze_next_tuple = ufsdemo_scan_analyze_next_tuple,
	.index_build_range_scan = ufsdemo_index_build_range_scan,
	.index_validate_scan = ufsdemo_index_validate_scan,

	.relation_size = ufsdemo_relation_size,
	.relation_needs_toast_table = ufsdemo_relation_needs_toast_table,
	.relation_toast_am = ufsdemo_relation_toast_am,

	.relation_estimate_size = ufsdemo_estimate_rel_size,

	.scan_bitmap_next_block = ufsdemo_scan_bitmap_next_block,
	.scan_bitmap_next_tuple = ufsdemo_scan_bitmap_next_tuple,
	.scan_sample_next_block = ufsdemo_scan_sample_next_block,
	.scan_sample_next_tuple = ufsdemo_scan_sample_next_tuple
};

Datum
ufsdemo_am_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&ufsdemo_methods);
}
