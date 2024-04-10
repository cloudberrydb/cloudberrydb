#include "access/pax_access_handle.h"

#include "comm/cbdb_api.h"

#include "access/pax_dml_state.h"
#include "access/pax_partition.h"
#include "access/pax_scanner.h"
#include "access/pax_updater.h"
#include "access/paxc_rel_options.h"
#include "access/paxc_scanner.h"
#include "catalog/pax_aux_table.h"
#include "catalog/pax_fastsequence.h"
#include "catalog/pg_pax_tables.h"
#include "comm/guc.h"
#include "comm/pax_memory.h"
#include "comm/vec_numeric.h"
#include "exceptions/CException.h"
#include "storage/local_file_system.h"

#define NOT_IMPLEMENTED_YET                        \
  ereport(ERROR,                                   \
          (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
           errmsg("not implemented yet on pax relations: %s", __func__)))

#define NOT_SUPPORTED_YET                                 \
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
                  errmsg("not supported on pax relations: %s", __func__)))

#define RELATION_IS_PAX(rel) \
  (OidIsValid((rel)->rd_rel->relam) && RelationIsPAX(rel))

// CBDB_TRY();
// {
//   // C++ implementation code
// }
// CBDB_CATCH_MATCH(std::exception &exp); // optional
// {
//    // specific exception handler
//    error_message.Append("error message: %s", error_message.Message());
// }
// CBDB_CATCH_DEFAULT();
// CBDB_END_TRY();
//
// CBDB_CATCH_MATCH() is optional and can have several match pattern.

char *global_pg_error_message = nullptr;
cbdb::CException global_exception(cbdb::CException::kExTypeInvalid);

// being of a try block w/o explicit handler
#define CBDB_TRY()                                          \
  do {                                                      \
    bool internal_cbdb_try_throw_error_ = false;            \
    bool internal_cbdb_try_throw_error_with_stack_ = false; \
    cbdb::ErrorMessage error_message;                       \
    try {
// begin of a catch block
#define CBDB_CATCH_MATCH(exception_decl) \
  }                                      \
  catch (exception_decl) {               \
    internal_cbdb_try_throw_error_ = true;

// catch c++ exception and rethrow ERROR to C code
// only used by the outer c++ code called by C
#define CBDB_CATCH_DEFAULT()                          \
  }                                                   \
  catch (cbdb::CException & e) {                      \
    internal_cbdb_try_throw_error_ = true;            \
    internal_cbdb_try_throw_error_with_stack_ = true; \
    global_pg_error_message = elog_message();         \
    elog(LOG, "\npax stack trace: \n%s", e.Stack());  \
    global_exception = e;                             \
  }                                                   \
  catch (...) {                                       \
    internal_cbdb_try_throw_error_ = true;            \
    internal_cbdb_try_throw_error_with_stack_ = false;

// like PG_FINALLY
#define CBDB_FINALLY(...) \
  }                       \
  {                       \
    do {                  \
      __VA_ARGS__;        \
    } while (0);

// end of a try-catch block
#define CBDB_END_TRY()                                                      \
  }                                                                         \
  if (internal_cbdb_try_throw_error_) {                                     \
    if (global_pg_error_message) {                                          \
      elog(LOG, "\npg error message:%s", global_pg_error_message);          \
    }                                                                       \
    if (internal_cbdb_try_throw_error_with_stack_) {                        \
      elog(LOG, "\npax stack trace: \n%s", global_exception.Stack());       \
      ereport(                                                              \
          ERROR,                                                            \
          errmsg("%s (PG message: %s)", global_exception.What().c_str(),    \
                 !global_pg_error_message ? "" : global_pg_error_message)); \
    }                                                                       \
    if (error_message.Length() == 0)                                        \
      error_message.Append("ERROR: %s", __func__);                          \
    ereport(ERROR, errmsg("%s", error_message.Message()));                  \
  }                                                                         \
  }                                                                         \
  while (0)

// access methods that are implemented in C++
namespace pax {

TableScanDesc CCPaxAccessMethod::ScanBegin(Relation relation, Snapshot snapshot,
                                           int nkeys, struct ScanKeyData *key,
                                           ParallelTableScanDesc pscan,
                                           uint32 flags) {
  CBDB_TRY();
  {
    return PaxScanDesc::BeginScan(relation, snapshot, nkeys, key, pscan, flags,
                                  nullptr, true);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_END_TRY();

  pg_unreachable();
}

void CCPaxAccessMethod::ScanEnd(TableScanDesc scan) {
  CBDB_TRY();
  {
    auto desc = PaxScanDesc::ToDesc(scan);
    desc->EndScan();
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
}

TableScanDesc CCPaxAccessMethod::ScanExtractColumns(
    Relation rel, Snapshot snapshot, int nkeys, struct ScanKeyData *key,
    ParallelTableScanDesc parallel_scan, struct PlanState *ps, uint32 flags) {
  CBDB_TRY();
  {
    return pax::PaxScanDesc::BeginScanExtractColumns(rel, snapshot, nkeys, key,
                                                     parallel_scan, ps, flags);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
  pg_unreachable();
}

struct IndexFetchTableData *CCPaxAccessMethod::IndexFetchBegin(Relation rel) {
  CBDB_TRY();
  {
    auto desc = PAX_NEW<PaxIndexScanDesc>(rel);
    return desc->ToBase();
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
  return nullptr;  // keep compiler quiet
}

void CCPaxAccessMethod::IndexFetchEnd(IndexFetchTableData *scan) {
  CBDB_TRY();
  {
    auto desc = PaxIndexScanDesc::FromBase(scan);
    PAX_DELETE(desc);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
}

bool CCPaxAccessMethod::IndexFetchTuple(struct IndexFetchTableData *scan,
                                        ItemPointer tid, Snapshot snapshot,
                                        TupleTableSlot *slot, bool *call_again,
                                        bool *all_dead) {
  CBDB_TRY();
  {
    auto desc = PaxIndexScanDesc::FromBase(scan);
    return desc->FetchTuple(tid, snapshot, slot, call_again, all_dead);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
  return false;  // keep compiler quiet
}

void CCPaxAccessMethod::IndexFetchReset(IndexFetchTableData * /*scan*/) {}

void CCPaxAccessMethod::RelationSetNewFilenode(Relation rel,
                                               const RelFileNode *newrnode,
                                               char persistence,
                                               TransactionId *freeze_xid,
                                               MultiXactId *minmulti) {
  Relation pax_tables_rel;
  ScanKeyData scan_key[1];
  SysScanDesc scan;
  HeapTuple tuple;
  Oid pax_relid;
  bool exists;

  *freeze_xid = *minmulti = InvalidTransactionId;

  pax_tables_rel = table_open(PAX_TABLES_RELATION_ID, RowExclusiveLock);
  pax_relid = RelationGetRelid(rel);

  ScanKeyInit(&scan_key[0], ANUM_PG_PAX_TABLES_RELID, BTEqualStrategyNumber,
              F_OIDEQ, ObjectIdGetDatum(pax_relid));
  scan = systable_beginscan(pax_tables_rel, PAX_TABLES_RELID_INDEX_ID, true,
                            NULL, 1, scan_key);
  tuple = systable_getnext(scan);
  exists = HeapTupleIsValid(tuple);
  if (exists) {
    Oid aux_relid;

    // set new filenode, not create new table
    //
    // 1. truncate aux table by new relfilenode
    aux_relid = ::paxc::GetPaxAuxRelid(pax_relid);
    Assert(OidIsValid(aux_relid));
    paxc::PaxAuxRelationSetNewFilenode(aux_relid);
  } else {
    // create new table
    //
    // 1. create aux table
    // 2. initialize fast sequence in pg_pax_fastsequence
    // 3. setup dependency
    paxc::CPaxCreateMicroPartitionTable(rel);
  }

  // initialize or reset the fast sequence number
  paxc::CPaxInitializeFastSequenceEntry(
      pax_relid,
      exists ? FASTSEQUENCE_INIT_TYPE_UPDATE : FASTSEQUENCE_INIT_TYPE_CREATE);

  systable_endscan(scan);
  table_close(pax_tables_rel, NoLock);

  // create relfilenode file for pax table
  auto srel = RelationCreateStorage(*newrnode, persistence, SMGR_MD, rel);
  smgrclose(srel);

  // create data directory
  CBDB_TRY();
  {
    FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();
    auto path = cbdb::BuildPaxDirectoryPath(*newrnode, rel->rd_backend);
    Assert(!path.empty());
    CBDB_CHECK((fs->CreateDirectory(path) == 0),
               cbdb::CException::ExType::kExTypeIOError);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
}

// * non transactional truncate table case:
// 1. create table inside transactional block, and then truncate table inside
// transactional block.
// 2.create table outside transactional block, insert data
// and truncate table inside transactional block.
void CCPaxAccessMethod::RelationNontransactionalTruncate(Relation rel) {
  CBDB_TRY();
  { pax::CCPaxAuxTable::PaxAuxRelationNontransactionalTruncate(rel); }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
}

void CCPaxAccessMethod::RelationCopyData(Relation rel,
                                         const RelFileNode *newrnode) {
  CBDB_TRY();
  { pax::CCPaxAuxTable::PaxAuxRelationCopyData(rel, newrnode); }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
}

/*
 * Used by rebuild_relation, like CLUSTER, VACUUM FULL, etc.
 *
 * PAX does not have dead tuples, but the core framework requires
 * to implement this callback to do CLUSTER/VACUUM FULL/etc.
 * PAX may have re-organize semantics for this function.
 *
 * TODO: how to split the set of micro-partitions to several QE handlers.
 */
void CCPaxAccessMethod::RelationCopyForCluster(
    Relation old_heap, Relation new_heap, Relation /*old_index*/,
    bool /*use_sort*/, TransactionId /*oldest_xmin*/,
    TransactionId * /*xid_cutoff*/, MultiXactId * /*multi_cutoff*/,
    double * /*num_tuples*/, double * /*tups_vacuumed*/,
    double * /*tups_recently_dead*/) {
  Assert(RELATION_IS_PAX(old_heap));
  Assert(RELATION_IS_PAX(new_heap));
  CBDB_TRY();
  { pax::CCPaxAuxTable::PaxAuxRelationCopyDataForCluster(old_heap, new_heap); }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
}

void CCPaxAccessMethod::RelationFileUnlink(RelFileNodeBackend rnode) {
  CBDB_TRY();
  {
    pax::CCPaxAuxTable::PaxAuxRelationFileUnlink(rnode.node, rnode.backend,
                                                 true);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
}

void CCPaxAccessMethod::ScanRescan(TableScanDesc scan, ScanKey key,
                                   bool set_params, bool allow_strat,
                                   bool allow_sync, bool allow_pagemode) {
  CBDB_TRY();
  {
    auto desc = PaxScanDesc::ToDesc(scan);
    desc->ReScan(key, set_params, allow_strat, allow_sync, allow_pagemode);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
}

bool CCPaxAccessMethod::ScanGetNextSlot(TableScanDesc scan,
                                        ScanDirection /*direction*/,
                                        TupleTableSlot *slot) {
  CBDB_TRY();
  {
    auto desc = PaxScanDesc::ToDesc(scan);
    bool result;

    result = desc->GetNextSlot(slot);
    if (result) {
      pgstat_count_heap_getnext(desc->GetRelation());
    }
    return result;
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();

  pg_unreachable();
}

void CCPaxAccessMethod::TupleInsert(Relation relation, TupleTableSlot *slot,
                                    CommandId cid, int options,
                                    BulkInsertState bistate) {
  CBDB_TRY();
  {
    MemoryContext old_ctx;
    Assert(cbdb::pax_memory_context);

    old_ctx = MemoryContextSwitchTo(cbdb::pax_memory_context);
    CPaxInserter::TupleInsert(relation, slot, cid, options, bistate);
    MemoryContextSwitchTo(old_ctx);

    pgstat_count_heap_insert(relation, 1);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({
      // FIXME: destroy CPaxInserter?
  });
  CBDB_END_TRY();
}

TM_Result CCPaxAccessMethod::TupleDelete(Relation relation, ItemPointer tid,
                                         CommandId cid, Snapshot snapshot,
                                         Snapshot /*crosscheck*/, bool /*wait*/,
                                         TM_FailureData *tmfd,
                                         bool /*changing_part*/) {
  CBDB_TRY();
  {
    auto result = CPaxDeleter::DeleteTuple(relation, tid, cid, snapshot, tmfd);
    if (result == TM_Ok) pgstat_count_heap_delete(relation);
    return result;
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
  pg_unreachable();
}

TM_Result CCPaxAccessMethod::TupleUpdate(Relation relation, ItemPointer otid,
                                         TupleTableSlot *slot, CommandId cid,
                                         Snapshot snapshot, Snapshot crosscheck,
                                         bool wait, TM_FailureData *tmfd,
                                         LockTupleMode *lockmode,
                                         bool *update_indexes) {
  CBDB_TRY();
  {
    MemoryContext old_ctx;
    TM_Result result;

    Assert(cbdb::pax_memory_context);
    old_ctx = MemoryContextSwitchTo(cbdb::pax_memory_context);
    result = CPaxUpdater::UpdateTuple(relation, otid, slot, cid, snapshot,
                                      crosscheck, wait, tmfd, lockmode,
                                      update_indexes);
    MemoryContextSwitchTo(old_ctx);
    if (result == TM_Ok) pgstat_count_heap_update(relation, false);
    return result;
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
  pg_unreachable();
}

bool CCPaxAccessMethod::ScanAnalyzeNextBlock(TableScanDesc scan,
                                             BlockNumber blockno,
                                             BufferAccessStrategy bstrategy) {
  CBDB_TRY();
  {
    auto desc = PaxScanDesc::ToDesc(scan);
    return desc->ScanAnalyzeNextBlock(blockno, bstrategy);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
  pg_unreachable();
}

bool CCPaxAccessMethod::ScanAnalyzeNextTuple(TableScanDesc scan,
                                             TransactionId oldest_xmin,
                                             double *liverows, double *deadrows,
                                             TupleTableSlot *slot) {
  CBDB_TRY();
  {
    auto desc = PaxScanDesc::ToDesc(scan);
    return desc->ScanAnalyzeNextTuple(oldest_xmin, liverows, deadrows, slot);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
  pg_unreachable();
}

bool CCPaxAccessMethod::ScanBitmapNextBlock(TableScanDesc scan,
                                            TBMIterateResult *tbmres) {
  CBDB_TRY();
  {
    auto desc = PaxScanDesc::ToDesc(scan);
    return desc->BitmapNextBlock(tbmres);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
  pg_unreachable();
}

bool CCPaxAccessMethod::ScanBitmapNextTuple(TableScanDesc scan,
                                            TBMIterateResult *tbmres,
                                            TupleTableSlot *slot) {
  CBDB_TRY();
  {
    auto desc = PaxScanDesc::ToDesc(scan);
    bool result;
    result = desc->BitmapNextTuple(tbmres, slot);
    if (result) {
      pgstat_count_heap_fetch(desc->GetRelation());
    }
    return result;
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
  pg_unreachable();
}

bool CCPaxAccessMethod::ScanSampleNextBlock(TableScanDesc scan,
                                            SampleScanState *scanstate) {
  CBDB_TRY();
  {
    auto desc = PaxScanDesc::ToDesc(scan);
    return desc->ScanSampleNextBlock(scanstate);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
  pg_unreachable();
}

bool CCPaxAccessMethod::ScanSampleNextTuple(TableScanDesc scan,
                                            SampleScanState *scanstate,
                                            TupleTableSlot *slot) {
  CBDB_TRY();
  {
    auto desc = PaxScanDesc::ToDesc(scan);
    return desc->ScanSampleNextTuple(scanstate, slot);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
  pg_unreachable();
}

void CCPaxAccessMethod::MultiInsert(Relation relation, TupleTableSlot **slots,
                                    int ntuples, CommandId cid, int options,
                                    BulkInsertState bistate) {
  CBDB_TRY();
  {
    MemoryContext old_ctx;
    Assert(cbdb::pax_memory_context);

    old_ctx = MemoryContextSwitchTo(cbdb::pax_memory_context);
    CPaxInserter::MultiInsert(relation, slots, ntuples, cid, options, bistate);
    MemoryContextSwitchTo(old_ctx);

    pgstat_count_heap_insert(relation, ntuples);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({
      // FIXME: destroy CPaxInserter?
  });
  CBDB_END_TRY();
}

void CCPaxAccessMethod::FinishBulkInsert(Relation relation, int options) {
  // Implement Pax dml cleanup for case "create table xxx1 as select * from
  // xxx2", which would not call ExtDmlFini callback function and relies on
  // FinishBulkInsert callback function to cleanup its dml state.
  CBDB_TRY();
  {
    // no need switch memory context
    // cause it just call dml finish
    pax::CPaxInserter::FinishBulkInsert(relation, options);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({
      // FIXME: destroy CPaxInserter?
  });
  CBDB_END_TRY();
}

void CCPaxAccessMethod::ExtDmlInit(Relation rel, CmdType operation) {
  if (!RELATION_IS_PAX(rel)) return;

  CBDB_TRY();
  { pax::CPaxDmlStateLocal::Instance()->InitDmlState(rel, operation); }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
}

void CCPaxAccessMethod::ExtDmlFini(Relation rel, CmdType operation) {
  if (!RELATION_IS_PAX(rel)) return;

  CBDB_TRY();
  { pax::CPaxDmlStateLocal::Instance()->FinishDmlState(rel, operation); }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
}

}  // namespace pax
// END of C++ implementation

// access methods that are implemented in C
namespace paxc {
const TupleTableSlotOps *PaxAccessMethod::SlotCallbacks(
    Relation /*rel*/) noexcept {
  return &TTSOpsVirtual;
}

uint32 PaxAccessMethod::ScanFlags(Relation relation) {
#ifdef VEC_BUILD
    return SCAN_SUPPORT_VECTORIZATION;
#else
    return 0;
#endif
}

Size PaxAccessMethod::ParallelscanEstimate(Relation /*rel*/) {
  NOT_IMPLEMENTED_YET;
  return 0;
}

Size PaxAccessMethod::ParallelscanInitialize(Relation /*rel*/,
                                             ParallelTableScanDesc /*pscan*/) {
  NOT_IMPLEMENTED_YET;
  return 0;
}

void PaxAccessMethod::ParallelscanReinitialize(
    Relation /*rel*/, ParallelTableScanDesc /*pscan*/) {
  NOT_IMPLEMENTED_YET;
}

void PaxAccessMethod::TupleInsertSpeculative(Relation /*relation*/,
                                             TupleTableSlot * /*slot*/,
                                             CommandId /*cid*/, int /*options*/,
                                             BulkInsertState /*bistate*/,
                                             uint32 /*spec_token*/) {
  NOT_IMPLEMENTED_YET;
}

void PaxAccessMethod::TupleCompleteSpeculative(Relation /*relation*/,
                                               TupleTableSlot * /*slot*/,
                                               uint32 /*spec_token*/,
                                               bool /*succeeded*/) {
  NOT_IMPLEMENTED_YET;
}

TM_Result PaxAccessMethod::TupleLock(Relation /*relation*/, ItemPointer /*tid*/,
                                     Snapshot /*snapshot*/,
                                     TupleTableSlot * /*slot*/,
                                     CommandId /*cid*/, LockTupleMode /*mode*/,
                                     LockWaitPolicy /*wait_policy*/,
                                     uint8 /*flags*/,
                                     TM_FailureData * /*tmfd*/) {
  NOT_IMPLEMENTED_YET;
  return TM_Ok;
}

bool PaxAccessMethod::TupleFetchRowVersion(Relation /*relation*/,
                                           ItemPointer /*tid*/,
                                           Snapshot /*snapshot*/,
                                           TupleTableSlot * /*slot*/) {
  NOT_IMPLEMENTED_YET;
  return false;
}

bool PaxAccessMethod::TupleTidValid(TableScanDesc /*scan*/,
                                    ItemPointer /*tid*/) {
  NOT_IMPLEMENTED_YET;
  return false;
}

void PaxAccessMethod::TupleGetLatestTid(TableScanDesc /*sscan*/,
                                        ItemPointer /*tid*/) {
  NOT_SUPPORTED_YET;
}

bool PaxAccessMethod::TupleSatisfiesSnapshot(Relation /*rel*/,
                                             TupleTableSlot * /*slot*/,
                                             Snapshot /*snapshot*/) {
  NOT_IMPLEMENTED_YET;
  return true;
}

TransactionId PaxAccessMethod::IndexDeleteTuples(
    Relation /*rel*/, TM_IndexDeleteOp * /*delstate*/) {
  NOT_SUPPORTED_YET;
  return 0;
}

void PaxAccessMethod::RelationVacuum(Relation /*onerel*/,
                                     VacuumParams * /*params*/,
                                     BufferAccessStrategy /*bstrategy*/) {
  /* PAX: micro-partitions have no dead tuples, so vacuum is empty */
}

uint64 PaxAccessMethod::RelationSize(Relation rel, ForkNumber fork_number) {
  Oid pax_aux_oid;
  Relation pax_aux_rel;
  TupleDesc aux_tup_desc;
  HeapTuple aux_tup;
  SysScanDesc aux_scan;
  uint64 pax_size = 0;

  if (fork_number != MAIN_FORKNUM) return 0;

  // Get the oid of pg_pax_blocks_xxx from pg_pax_tables
  pax_aux_oid = ::paxc::GetPaxAuxRelid(rel->rd_id);

  // Scan pg_pax_blocks_xxx to calculate size of micro partition
  pax_aux_rel = table_open(pax_aux_oid, AccessShareLock);
  aux_tup_desc = RelationGetDescr(pax_aux_rel);

  aux_scan = systable_beginscan(pax_aux_rel, InvalidOid, false, NULL, 0, NULL);
  while (HeapTupleIsValid(aux_tup = systable_getnext(aux_scan))) {
    bool isnull = false;
    // TODO(chenhongjie): Exactly what is needed and being obtained is
    // compressed size. Later, when the aux table supports size attributes
    // before/after compression, we need to distinguish two attributes by names.
    Datum tup_datum = heap_getattr(
        aux_tup, ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE, aux_tup_desc, &isnull);

    Assert(!isnull);
    pax_size += DatumGetUInt32(tup_datum);
  }

  systable_endscan(aux_scan);
  table_close(pax_aux_rel, AccessShareLock);

  return pax_size;
}

bool PaxAccessMethod::RelationNeedsToastTable(Relation /*rel*/) {
  // PAX never used the toasting, don't create the toast table from Cloudberry 7

  return false;
}

// Similar to the case of AO and AOCS tables, PAX table has auxiliary tables,
// size can be read directly from the auxiliary table, and there is not much
// space for optimization in estimating relsize. So this function is implemented
// in the same way as pax_relation_size().
void PaxAccessMethod::EstimateRelSize(Relation rel, int32 * /*attr_widths*/,
                                      BlockNumber *pages, double *tuples,
                                      double *allvisfrac) {
  Oid pax_aux_oid;
  Relation pax_aux_rel;
  TupleDesc aux_tup_desc;
  HeapTuple aux_tup;
  SysScanDesc aux_scan;
  uint64 total_tuples = 0;
  uint64 pax_size = 0;

  // Even an empty table takes at least one page,
  // but number of tuples for an empty table could be 0.
  *tuples = 0;
  *pages = 1;
  // index-only scan is not supported in PAX
  *allvisfrac = 0;

  // Get the oid of pg_pax_blocks_xxx from pg_pax_tables
  pax_aux_oid = ::paxc::GetPaxAuxRelid(rel->rd_id);

  // Scan pg_pax_blocks_xxx to get attributes
  pax_aux_rel = table_open(pax_aux_oid, AccessShareLock);
  aux_tup_desc = RelationGetDescr(pax_aux_rel);

  aux_scan = systable_beginscan(pax_aux_rel, InvalidOid, false, NULL, 0, NULL);
  while (HeapTupleIsValid(aux_tup = systable_getnext(aux_scan))) {
    Datum pttupcount_datum;
    Datum ptblocksize_datum;
    bool isnull = false;

    pttupcount_datum = heap_getattr(
        aux_tup, ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT, aux_tup_desc, &isnull);
    Assert(!isnull);
    total_tuples += DatumGetUInt32(pttupcount_datum);

    isnull = false;
    // TODO(chenhongjie): Exactly what we want to get here is uncompressed size,
    // but what we're getting is compressed size. Later, when the aux table
    // supports size attributes before/after compression, this needs to
    // be corrected.
    ptblocksize_datum = heap_getattr(
        aux_tup, ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE, aux_tup_desc, &isnull);

    Assert(!isnull);
    pax_size += DatumGetUInt32(ptblocksize_datum);
  }

  systable_endscan(aux_scan);
  table_close(pax_aux_rel, AccessShareLock);

  *tuples = static_cast<double>(total_tuples);
  *pages = RelationGuessNumberOfBlocksFromSize(pax_size);
}

double PaxAccessMethod::IndexBuildRangeScan(
    Relation heap_relation, Relation index_relation, IndexInfo *index_info,
    bool /*allow_sync*/, bool anyvisible, bool progress,
    BlockNumber start_blockno, BlockNumber numblocks,
    IndexBuildCallback callback, void *callback_state, TableScanDesc scan) {
  Datum values[INDEX_MAX_KEYS];
  bool isnull[INDEX_MAX_KEYS];
  double reltuples = 0;
  ExprState *predicate;
  TupleTableSlot *slot;
  EState *estate;
  ExprContext *econtext;
  Snapshot snapshot;

  bool checking_uniqueness;
  bool need_unregister_snapshot;
  BlockNumber previous_blkno = InvalidBlockNumber;

  Assert(OidIsValid(index_relation->rd_rel->relam));
  Assert(!IsSystemRelation(heap_relation));

  if (index_relation->rd_rel->relam != BTREE_AM_OID)
    elog(ERROR, "pax only support btree index");

  checking_uniqueness =
      (index_info->ii_Unique || index_info->ii_ExclusionOps != NULL);
  // "Any visible" mode is not compatible with uniqueness checks; make sure
  // only one of those is requested.
  (void)anyvisible;  // keep compiler quiet for release version
  Assert(!(anyvisible && checking_uniqueness));

  slot = table_slot_create(heap_relation, NULL);
  estate = CreateExecutorState();
  econtext = GetPerTupleExprContext(estate);
  econtext->ecxt_scantuple = slot;
  predicate = ExecPrepareQual(index_info->ii_Predicate, estate);

  if (!scan) {
    snapshot = RegisterSnapshot(GetTransactionSnapshot());
    scan = table_beginscan(heap_relation, snapshot, 0, NULL);
    need_unregister_snapshot = true;
  } else {
    snapshot = scan->rs_snapshot;
    need_unregister_snapshot = false;
  }

  // FIXME: Only brin index uses partial index now. setup start_blockno
  // and numblocks is too late after beginscan is called now, because
  // the current micro partition is opened. The workaround is ugly to
  // check and close the current micro partition and open another one.
  if (start_blockno != 0 || numblocks != InvalidBlockNumber)
    elog(ERROR, "PAX doesn't support partial index scan now");

  while (table_scan_getnextslot(scan, ForwardScanDirection, slot)) {
    CHECK_FOR_INTERRUPTS();

    if (progress) {
      BlockNumber blkno = pax::GetBlockNumber(slot->tts_tid);
      if (previous_blkno == InvalidBlockNumber)
        previous_blkno = blkno;
      else if (previous_blkno != blkno) {
        pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_DONE,
                                     blkno - start_blockno);
        previous_blkno = blkno;
      }
    }
    reltuples += 1;

    MemoryContextReset(econtext->ecxt_per_tuple_memory);

    /*
     * In a partial index, discard tuples that don't satisfy the
     * predicate.
     */
    if (predicate && !ExecQual(predicate, econtext)) continue;

    /*
     * For the current heap tuple, extract all the attributes we use in
     * this index, and note which are null.  This also performs evaluation
     * of any expressions needed.
     */
    FormIndexDatum(index_info, slot, estate, values, isnull);

    /*
     * You'd think we should go ahead and build the index tuple here, but
     * some index AMs want to do further processing on the data first.  So
     * pass the values[] and isnull[] arrays, instead.
     */
    callback(index_relation, &slot->tts_tid, values, isnull, true,
             callback_state);
  }

  /* Report scan progress one last time. */
  if (progress && previous_blkno != InvalidBlockNumber)
    pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_DONE,
                                 previous_blkno + 1 - start_blockno);

  table_endscan(scan);
  if (need_unregister_snapshot) UnregisterSnapshot(snapshot);

  ExecDropSingleTupleTableSlot(slot);
  FreeExecutorState(estate);

  /* These may have been pointing to the now-gone estate */
  index_info->ii_ExpressionsState = NIL;
  index_info->ii_PredicateState = NULL;

  return reltuples;
}

bool PaxAccessMethod::IndexUniqueCheck(Relation rel, ItemPointer tid,
                                       Snapshot snapshot, bool *all_dead) {
  return paxc::IndexUniqueCheck(rel, tid, snapshot, all_dead);
}

void PaxAccessMethod::IndexValidateScan(Relation /*heap_relation*/,
                                        Relation /*index_relation*/,
                                        IndexInfo * /*index_info*/,
                                        Snapshot /*snapshot*/,
                                        ValidateIndexState * /*state*/) {
  NOT_IMPLEMENTED_YET;
}

// Swap data between two pax tables, but not swap oids
// 1. swap partition-spec in pg_pax_tables
// 2. swap relation content for aux table and toast
void PaxAccessMethod::SwapRelationFiles(Oid relid1, Oid relid2,
                                        TransactionId frozen_xid,
                                        MultiXactId cutoff_multi) {
  HeapTuple old_tuple1;
  HeapTuple old_tuple2;
  Relation pax_rel;
  TupleDesc desc;
  ScanKeyData key[1];
  SysScanDesc scan;

  Oid aux_relid1;
  Oid aux_relid2;

  pax_rel = table_open(PAX_TABLES_RELATION_ID, RowExclusiveLock);
  desc = RelationGetDescr(pax_rel);

  // save ctid, auxrelid and partition-spec for the first pax relation
  ScanKeyInit(&key[0], ANUM_PG_PAX_TABLES_RELID, BTEqualStrategyNumber, F_OIDEQ,
              ObjectIdGetDatum(relid1));

  scan = systable_beginscan(pax_rel, PAX_TABLES_RELID_INDEX_ID, true, nullptr,
                            1, key);
  old_tuple1 = systable_getnext(scan);
  if (!HeapTupleIsValid(old_tuple1))
    ereport(ERROR, (errmsg("relid=%u is not a pax relation", relid1)));

  old_tuple1 = heap_copytuple(old_tuple1);
  systable_endscan(scan);

  // save ctid, auxrelid and partition-spec for the second pax relation
  ScanKeyInit(&key[0], ANUM_PG_PAX_TABLES_RELID, BTEqualStrategyNumber, F_OIDEQ,
              ObjectIdGetDatum(relid2));
  scan = systable_beginscan(pax_rel, PAX_TABLES_RELID_INDEX_ID, true, nullptr,
                            1, key);
  old_tuple2 = systable_getnext(scan);
  if (!HeapTupleIsValid(old_tuple2))
    ereport(ERROR, (errmsg("relid=%u is not a pax relation", relid2)));

  old_tuple2 = heap_copytuple(old_tuple2);
  systable_endscan(scan);

  // swap the entries
  {
    HeapTuple tuple1;
    HeapTuple tuple2;
    Datum values[NATTS_PG_PAX_TABLES];
    bool nulls[NATTS_PG_PAX_TABLES];
    Datum datum;
    bool isnull;

    datum =
        heap_getattr(old_tuple1, ANUM_PG_PAX_TABLES_AUXRELID, desc, &isnull);
    Assert(!isnull);
    aux_relid1 = DatumGetObjectId(datum);

    values[ANUM_PG_PAX_TABLES_RELID - 1] = ObjectIdGetDatum(relid1);
    values[ANUM_PG_PAX_TABLES_AUXRELID - 1] = datum;
    nulls[ANUM_PG_PAX_TABLES_RELID - 1] = false;
    nulls[ANUM_PG_PAX_TABLES_AUXRELID - 1] = false;

    datum = heap_getattr(old_tuple2, ANUM_PG_PAX_TABLES_PARTITIONSPEC, desc,
                         &isnull);
    if (!isnull) {
      auto vl = reinterpret_cast<struct varlena *>(DatumGetPointer(datum));
      vl = pg_detoast_datum_packed(vl);
      values[ANUM_PG_PAX_TABLES_PARTITIONSPEC - 1] = PointerGetDatum(vl);
    }
    nulls[ANUM_PG_PAX_TABLES_PARTITIONSPEC - 1] = isnull;

    tuple1 = heap_form_tuple(desc, values, nulls);
    tuple1->t_data->t_ctid = old_tuple1->t_data->t_ctid;
    tuple1->t_self = old_tuple1->t_self;
    tuple1->t_tableOid = old_tuple1->t_tableOid;

    datum =
        heap_getattr(old_tuple2, ANUM_PG_PAX_TABLES_AUXRELID, desc, &isnull);
    Assert(!isnull);
    aux_relid2 = DatumGetObjectId(datum);

    values[ANUM_PG_PAX_TABLES_RELID - 1] = ObjectIdGetDatum(relid2);
    values[ANUM_PG_PAX_TABLES_AUXRELID - 1] = datum;
    nulls[ANUM_PG_PAX_TABLES_RELID - 1] = false;
    nulls[ANUM_PG_PAX_TABLES_AUXRELID - 1] = false;

    datum = heap_getattr(old_tuple1, ANUM_PG_PAX_TABLES_PARTITIONSPEC, desc,
                         &isnull);
    if (!isnull) {
      auto vl = reinterpret_cast<struct varlena *>(DatumGetPointer(datum));
      vl = pg_detoast_datum_packed(vl);
      values[ANUM_PG_PAX_TABLES_PARTITIONSPEC - 1] = PointerGetDatum(vl);
    }
    nulls[ANUM_PG_PAX_TABLES_PARTITIONSPEC - 1] = isnull;

    tuple2 = heap_form_tuple(desc, values, nulls);
    tuple2->t_data->t_ctid = old_tuple2->t_data->t_ctid;
    tuple2->t_self = old_tuple2->t_self;
    tuple2->t_tableOid = old_tuple2->t_tableOid;

    CatalogIndexState indstate;

    indstate = CatalogOpenIndexes(pax_rel);
    CatalogTupleUpdateWithInfo(pax_rel, &tuple1->t_self, tuple1, indstate);
    CatalogTupleUpdateWithInfo(pax_rel, &tuple2->t_self, tuple2, indstate);
    CatalogCloseIndexes(indstate);
  }

  table_close(pax_rel, NoLock);

  /* swap fast seq */
  {
    int32 seqno1, seqno2;

    seqno1 = CPaxGetFastSequences(relid1, false);
    seqno2 = CPaxGetFastSequences(relid2, false);

    CPaxInitializeFastSequenceEntry(relid1, FASTSEQUENCE_INIT_TYPE_UPDATE,
                                    seqno2);
    CPaxInitializeFastSequenceEntry(relid2, FASTSEQUENCE_INIT_TYPE_UPDATE,
                                    seqno1);
  }
  SIMPLE_FAULT_INJECTOR("pax_finish_swap_fast_fastsequence");

  /* swap relation files for aux table */
  {
    Relation aux_rel1;
    Relation aux_rel2;
    ReindexParams reindex_params = {0};
    Relation toast_rel1 = nullptr;
    Relation toast_rel2 = nullptr;

    aux_rel1 = relation_open(aux_relid1, AccessExclusiveLock);
    aux_rel2 = relation_open(aux_relid2, AccessExclusiveLock);

    if (OidIsValid(aux_rel1->rd_rel->reltoastrelid))
      toast_rel1 =
          relation_open(aux_rel1->rd_rel->reltoastrelid, AccessExclusiveLock);
    if (OidIsValid(aux_rel2->rd_rel->reltoastrelid))
      toast_rel2 =
          relation_open(aux_rel2->rd_rel->reltoastrelid, AccessExclusiveLock);

    swap_relation_files(aux_relid1, aux_relid2, false, /* target_is_pg_class */
                        true, /* swap_toast_by_content */
                        true, /*swap_stats */
                        true, /* is_internal */
                        frozen_xid, cutoff_multi, NULL);

    if (toast_rel1) relation_close(toast_rel1, NoLock);
    if (toast_rel2) relation_close(toast_rel2, NoLock);
    relation_close(aux_rel1, NoLock);
    relation_close(aux_rel2, NoLock);

    reindex_relation(aux_relid1, 0, &reindex_params);
    reindex_relation(aux_relid2, 0, &reindex_params);
  }
}

bytea *PaxAccessMethod::AmOptions(Datum reloptions, char relkind,
                                  bool validate) {
  return paxc_default_rel_options(reloptions, relkind, validate);
}

void PaxAccessMethod::ValidateColumnEncodingClauses(List *encoding_opts) {
  paxc_validate_column_encoding_clauses(encoding_opts);
}

List *PaxAccessMethod::TransformColumnEncodingClauses(Relation /*rel*/,
                                                      List *encoding_opts,
                                                      bool validate,
                                                      bool from_type) {
  return paxc_transform_column_encoding_clauses(encoding_opts, validate,
                                                from_type);
}

}  // namespace paxc
// END of C implementation

extern "C" {

static const TableAmRoutine kPaxColumnMethods = {
    .type = T_TableAmRoutine,
    .slot_callbacks = paxc::PaxAccessMethod::SlotCallbacks,
    .scan_begin = pax::CCPaxAccessMethod::ScanBegin,
    .scan_begin_extractcolumns = pax::CCPaxAccessMethod::ScanExtractColumns,
    .scan_end = pax::CCPaxAccessMethod::ScanEnd,
    .scan_rescan = pax::CCPaxAccessMethod::ScanRescan,
    .scan_getnextslot = pax::CCPaxAccessMethod::ScanGetNextSlot,
    .scan_flags = paxc::PaxAccessMethod::ScanFlags,

    .parallelscan_estimate = paxc::PaxAccessMethod::ParallelscanEstimate,
    .parallelscan_initialize = paxc::PaxAccessMethod::ParallelscanInitialize,
    .parallelscan_reinitialize =
        paxc::PaxAccessMethod::ParallelscanReinitialize,

    .index_fetch_begin = pax::CCPaxAccessMethod::IndexFetchBegin,
    .index_fetch_reset = pax::CCPaxAccessMethod::IndexFetchReset,
    .index_fetch_end = pax::CCPaxAccessMethod::IndexFetchEnd,
    .index_fetch_tuple = pax::CCPaxAccessMethod::IndexFetchTuple,
    .index_unique_check = paxc::PaxAccessMethod::IndexUniqueCheck,

    .tuple_fetch_row_version = paxc::PaxAccessMethod::TupleFetchRowVersion,
    .tuple_tid_valid = paxc::PaxAccessMethod::TupleTidValid,
    .tuple_get_latest_tid = paxc::PaxAccessMethod::TupleGetLatestTid,
    .tuple_satisfies_snapshot = paxc::PaxAccessMethod::TupleSatisfiesSnapshot,
    .index_delete_tuples = paxc::PaxAccessMethod::IndexDeleteTuples,

    .tuple_insert = pax::CCPaxAccessMethod::TupleInsert,
    .tuple_insert_speculative = paxc::PaxAccessMethod::TupleInsertSpeculative,
    .tuple_complete_speculative =
        paxc::PaxAccessMethod::TupleCompleteSpeculative,
    .multi_insert = pax::CCPaxAccessMethod::MultiInsert,
    .tuple_delete = pax::CCPaxAccessMethod::TupleDelete,
    .tuple_update = pax::CCPaxAccessMethod::TupleUpdate,
    .tuple_lock = paxc::PaxAccessMethod::TupleLock,
    .finish_bulk_insert = pax::CCPaxAccessMethod::FinishBulkInsert,

    .relation_set_new_filenode = pax::CCPaxAccessMethod::RelationSetNewFilenode,
    .relation_nontransactional_truncate =
        pax::CCPaxAccessMethod::RelationNontransactionalTruncate,
    .relation_copy_data = pax::CCPaxAccessMethod::RelationCopyData,
    .relation_copy_for_cluster = pax::CCPaxAccessMethod::RelationCopyForCluster,
    .relation_vacuum = paxc::PaxAccessMethod::RelationVacuum,
    .scan_analyze_next_block = pax::CCPaxAccessMethod::ScanAnalyzeNextBlock,
    .scan_analyze_next_tuple = pax::CCPaxAccessMethod::ScanAnalyzeNextTuple,
    .index_build_range_scan = paxc::PaxAccessMethod::IndexBuildRangeScan,
    .index_validate_scan = paxc::PaxAccessMethod::IndexValidateScan,

    .relation_size = paxc::PaxAccessMethod::RelationSize,
    .relation_needs_toast_table =
        paxc::PaxAccessMethod::RelationNeedsToastTable,

    .relation_estimate_size = paxc::PaxAccessMethod::EstimateRelSize,
    .scan_bitmap_next_block = pax::CCPaxAccessMethod::ScanBitmapNextBlock,
    .scan_bitmap_next_tuple = pax::CCPaxAccessMethod::ScanBitmapNextTuple,
    .scan_sample_next_block = pax::CCPaxAccessMethod::ScanSampleNextBlock,
    .scan_sample_next_tuple = pax::CCPaxAccessMethod::ScanSampleNextTuple,

    .amoptions = paxc::PaxAccessMethod::AmOptions,
    .swap_relation_files = paxc::PaxAccessMethod::SwapRelationFiles,
    .validate_column_encoding_clauses =
        paxc::PaxAccessMethod::ValidateColumnEncodingClauses,
    .transform_column_encoding_clauses =
        paxc::PaxAccessMethod::TransformColumnEncodingClauses,
};

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(pax_tableam_handler);
Datum pax_tableam_handler(PG_FUNCTION_ARGS) {  // NOLINT
  PG_RETURN_POINTER(&kPaxColumnMethods);
}

static object_access_hook_type prev_object_access_hook = NULL;

static void PaxObjectAccessHook(ObjectAccessType access, Oid class_id,
                                Oid object_id, int sub_id, void *arg) {
  Relation rel;
  PartitionKey pkey;
  List *part;
  List *pby;
  paxc::PaxOptions *options;
  int relnatts;
  TupleDesc tupdesc;
  int64_t precision;

  if (prev_object_access_hook)
    prev_object_access_hook(access, class_id, object_id, sub_id, arg);

  if (access != OAT_POST_CREATE || class_id != RelationRelationId) return;

  CommandCounterIncrement();
  rel = relation_open(object_id, RowExclusiveLock);
  auto ok = ((rel->rd_rel->relkind == RELKIND_RELATION ||
              rel->rd_rel->relkind == RELKIND_MATVIEW) &&
             rel->rd_options && RelationIsPAX(rel));
  if (!ok) goto out;

  options = reinterpret_cast<paxc::PaxOptions *>(rel->rd_options);
  if (!options->partition_by()) {
    if (options->partition_ranges()) {
      elog(ERROR, "set '%s', but partition_by not specified",
           options->partition_ranges());
    }
    goto check_numeric_options;
  }

  pby = paxc_raw_parse(options->partition_by());
  pkey = paxc::PaxRelationBuildPartitionKey(rel, pby);
  if (pkey->partnatts > 1) elog(ERROR, "pax only support 1 partition key now");

  part = lappend(NIL, pby);
  if (options->partition_ranges()) {
    List *ranges;

    ranges = paxc_parse_partition_ranges(options->partition_ranges());
    ranges = paxc::PaxValidatePartitionRanges(rel, pkey, ranges);
    part = lappend(part, ranges);
  }
  // Currently, partition_ranges must be set to partition pax tables.
  // We hope this option be removed and automatically partition data set.
  else
    elog(ERROR, "partition_ranges must be set for partition_by='%s'",
         options->partition_by());

  ::paxc::PaxInitializePartitionSpec(rel, reinterpret_cast<Node *>(part));

check_numeric_options:
  if (!options->numeric_vec_storage) {
    goto out;
  }

#ifndef HAVE_INT128
  elog(ERROR, "option 'numeric_vec_storage' must be enable INT128 build");
#endif

  relnatts = RelationGetNumberOfAttributes(rel);
  tupdesc = RelationGetDescr(rel);
  for (int attno = 0; attno < relnatts; attno++) {
    Form_pg_attribute attr = TupleDescAttr(tupdesc, attno);
    if (attr->atttypid != NUMERICOID) {
      continue;
    }

    if (attr->atttypmod < 0) {
      elog(ERROR,
           "column '%s' created with not support precision(-1) and scale(-1).",
           NameStr(attr->attname));
    }

    precision = ((attr->atttypmod - VARHDRSZ) >> 16) & 0xffff;

    // no need check scale
    if (precision > VEC_SHORT_NUMERIC_MAX_PRECISION) {
      elog(ERROR,
           "column '%s' precision(%ld) out of range, precision should be (0, "
           "%d]",
           NameStr(attr->attname), precision, VEC_SHORT_NUMERIC_MAX_PRECISION);
    }
  }

out:
  relation_close(rel, RowExclusiveLock);
}

struct PaxObjectProperty {
  const char *name;
  Oid class_oid;
  Oid index_oid;
  AttrNumber attnum_oid;
};

static const struct PaxObjectProperty kPaxObjectProperties[] = {
    {"fast-sequence", PAX_FASTSEQUENCE_OID, PAX_FASTSEQUENCE_INDEX_OID,
     ANUM_PG_PAX_FAST_SEQUENCE_OBJID},
    {"pg_pax_tables", PAX_TABLES_RELATION_ID, PAX_TABLES_RELID_INDEX_ID,
     ANUM_PG_PAX_TABLES_RELID},
    // add pg_pax_tables here
};

static const struct PaxObjectProperty *FindPaxObjectProperty(Oid class_id) {
  for (const auto &property : kPaxObjectProperties) {
    const auto p = &property;
    if (p->class_oid == class_id) return p;
  }
  return NULL;
}

static void PaxDeleteObject(struct CustomObjectClass * /*self*/,
                            const ObjectAddress *object, int /*flags*/) {
  Relation rel;
  HeapTuple tup;
  SysScanDesc scan;
  ScanKeyData skey[1];

  const auto object_property = FindPaxObjectProperty(object->classId);
  Assert(object_property);
  Assert(object_property->class_oid == object->classId);

  rel = table_open(object->classId, RowExclusiveLock);
  ScanKeyInit(&skey[0], object_property->attnum_oid, BTEqualStrategyNumber,
              F_OIDEQ, ObjectIdGetDatum(object->objectId));

  scan =
      systable_beginscan(rel, object_property->index_oid, true, NULL, 1, skey);

  /* we expect exactly one match */
  tup = systable_getnext(scan);
  if (!HeapTupleIsValid(tup))
    elog(ERROR, "could not find tuple for %s %u", object_property->name,
         object->objectId);

  CatalogTupleDelete(rel, &tup->t_self);

  systable_endscan(scan);

  table_close(rel, RowExclusiveLock);
}

static void PaxTableTypeDesc(struct CustomObjectClass * /*self*/,
                             const ObjectAddress * /*object*/,
                             bool /*missing_ok*/,
                             struct StringInfoData *buffer) {
  appendStringInfoString(buffer, "pax table");
}

static char *pax_table_get_name(Oid oid, bool missing_ok) {
  char *pax_rel_name;
  Relation pax_rel;
  ScanKeyData skey;
  SysScanDesc scan;
  HeapTuple tup;

  pax_rel = table_open(PAX_TABLES_RELATION_ID, AccessShareLock);

  // save ctid, auxrelid and partition-spec for the first pax relation
  ScanKeyInit(&skey, ANUM_PG_PAX_TABLES_RELID, BTEqualStrategyNumber, F_OIDEQ,
              ObjectIdGetDatum(oid));

  scan = systable_beginscan(pax_rel, PAX_TABLES_RELID_INDEX_ID, true, nullptr,
                            1, &skey);

  tup = systable_getnext(scan);
  if (!HeapTupleIsValid(tup)) {
    if (!missing_ok) elog(ERROR, "pax table %u could not be found", oid);

    pax_rel_name = NULL;
  } else {
    // no need to get relid from tuple
    pax_rel_name = (char *)palloc(50);
    sprintf(pax_rel_name, "pax_table_%d", oid);
  }
  systable_endscan(scan);
  table_close(pax_rel, NoLock);
  return pax_rel_name;
}

static void PaxTableIdentityObject(struct CustomObjectClass * /*self*/,
                                   const ObjectAddress *object, List **objname,
                                   List ** /*objargs*/, bool missing_ok,
                                   struct StringInfoData *buffer) {
  char *pax_table_name;
  pax_table_name = pax_table_get_name(object->objectId, missing_ok);
  if (pax_table_name) {
    if (objname) *objname = list_make1(pax_table_name);
    appendStringInfo(
        buffer, "pax table identity %s: ", quote_identifier(pax_table_name));
  }
}

static void PaxFastSeqTypeDesc(struct CustomObjectClass * /*self*/,
                               const ObjectAddress * /*object*/,
                               bool /*missing_ok*/,
                               struct StringInfoData *buffer) {
  appendStringInfoString(buffer, "pax fast sequence");
}

static void PaxFastSeqIdentityObject(struct CustomObjectClass * /*self*/,
                                     const ObjectAddress *object,
                                     List **objname, List ** /*objargs*/,
                                     bool missing_ok,
                                     struct StringInfoData *buffer) {
  char *pax_fast_seq_name;
  pax_fast_seq_name =
      paxc::CPaxGetFastSequencesName(object->objectId, missing_ok);
  if (pax_fast_seq_name) {
    if (objname) *objname = list_make1(pax_fast_seq_name);
    appendStringInfo(buffer, "pax fast sequences identity %s: ",
                     quote_identifier(pax_fast_seq_name));
  }
}

static struct CustomObjectClass pax_fastsequence_coc = {
    .class_id = PAX_FASTSEQUENCE_OID,
    .do_delete = PaxDeleteObject,
    .object_type_desc = PaxFastSeqTypeDesc,
    .object_identity_parts = PaxFastSeqIdentityObject,
};

static struct CustomObjectClass pax_tables_coc = {
    .class_id = PAX_TABLES_RELATION_ID,
    .do_delete = PaxDeleteObject,
    .object_type_desc = PaxTableTypeDesc,
    .object_identity_parts = PaxTableIdentityObject,
};

void _PG_init(void) {  // NOLINT
  prev_object_access_hook = object_access_hook;
  object_access_hook = PaxObjectAccessHook;

  ext_dml_init_hook = pax::CCPaxAccessMethod::ExtDmlInit;
  ext_dml_finish_hook = pax::CCPaxAccessMethod::ExtDmlFini;
  file_unlink_hook = pax::CCPaxAccessMethod::RelationFileUnlink;

  register_custom_object_class(&pax_fastsequence_coc);
  register_custom_object_class(&pax_tables_coc);

  paxc::DefineGUCs();

  RegisterResourceReleaseCallback(paxc::FdHandleAbortCallback, NULL);

  paxc::paxc_reg_rel_options();
}
}  // extern "C"
