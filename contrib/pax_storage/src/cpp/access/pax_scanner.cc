#include "access/pax_scanner.h"

#include "access/pax_access_handle.h"
#include "catalog/pax_aux_table.h"
#include "catalog/pg_pax_tables.h"
#include "comm/guc.h"
#include "comm/pax_memory.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition.h"
#include "storage/micro_partition_iterator.h"
#include "storage/micro_partition_stats.h"
#include "storage/orc/porc.h"
#include "storage/pax.h"
#include "storage/pax_buffer.h"
#include "storage/pax_defined.h"

#ifdef ENABLE_PLASMA
#include "storage/cache/pax_plasma_cache.h"
#endif

#ifdef VEC_BUILD
#include "utils/am_vec.h"
#endif

namespace paxc {
bool IndexUniqueCheck(Relation rel, ItemPointer tid, Snapshot snapshot,
                      bool * /*all_dead*/) {
  paxc::ScanAuxContext context;
  HeapTuple tuple;
  char block_name[NAMEDATALEN];
  Oid aux_relid;
  bool exists;

  aux_relid = ::paxc::GetPaxAuxRelid(RelationGetRelid(rel));
  snprintf(block_name, sizeof(block_name), "%u", pax::GetBlockNumber(*tid));
  context.BeginSearchMicroPartition(aux_relid, InvalidOid, snapshot,
                                    AccessShareLock, block_name);
  tuple = context.SearchMicroPartitionEntry();
  exists = HeapTupleIsValid(tuple);
  context.EndSearchMicroPartition(AccessShareLock);
  return exists;
}
}  // namespace paxc

namespace pax {

PaxIndexScanDesc::PaxIndexScanDesc(Relation rel) : base_{.rel = rel} {
  Assert(rel);
  Assert(&base_ == reinterpret_cast<IndexFetchTableData *>(this));
  rel_path_ = cbdb::BuildPaxDirectoryPath(rel->rd_node, rel->rd_backend);
}

PaxIndexScanDesc::~PaxIndexScanDesc() {
  if (reader_) {
    reader_->Close();
    PAX_DELETE(reader_);
  }
}

bool PaxIndexScanDesc::FetchTuple(ItemPointer tid, Snapshot snapshot,
                                  TupleTableSlot *slot, bool *call_again,
                                  bool *all_dead) {
  BlockNumber block = pax::GetBlockNumber(*tid);
  if (block != current_block_ || !reader_) {
    if (!OpenMicroPartition(block, snapshot)) return false;
  }

  Assert(current_block_ == block && reader_);
  if (call_again) *call_again = false;
  if (all_dead) *all_dead = false;

  ExecClearTuple(slot);
  if (reader_->GetTuple(slot, pax::GetTupleOffset(*tid))) {
    SetBlockNumber(&slot->tts_tid, block);
    ExecStoreVirtualTuple(slot);

    return true;
  }

  return false;
}

bool PaxIndexScanDesc::OpenMicroPartition(BlockNumber block,
                                          Snapshot snapshot) {
  bool ok;

  Assert(block != current_block_);

  ok = cbdb::IsMicroPartitionVisible(base_.rel, block, snapshot);
  if (ok) {
    MicroPartitionReader::ReaderOptions options;

    auto block_name = std::to_string(block);
    auto file_name = cbdb::BuildPaxFilePath(rel_path_, block_name);
    options.block_id = block_name;
    auto file = Singleton<LocalFileSystem>::GetInstance()->Open(file_name,
                                                                fs::kReadMode);
    auto reader = PAX_NEW<OrcReader>(file);
    reader->Open(options);
    if (reader_) {
      reader_->Close();
      PAX_DELETE(reader_);
    }
    reader_ = reader;
    current_block_ = block;
  }

  return ok;
}

bool PaxScanDesc::BitmapNextBlock(struct TBMIterateResult *tbmres) {
  cindex_ = 0;
  if (!index_desc_) {
    index_desc_ = PAX_NEW<PaxIndexScanDesc>(rs_base_.rs_rd);
  }
  return true;
}

bool PaxScanDesc::BitmapNextTuple(struct TBMIterateResult *tbmres,
                                  TupleTableSlot *slot) {
  ItemPointerData tid;
  if (tbmres->ntuples < 0) {
    // lossy bitmap. The maximum value of the last 16 bits in CTID is
    // 0x7FFF + 1, i.e. 0x8000. See layout of ItemPointerData in PAX
    if (cindex_ > 0X8000) elog(ERROR, "unexpected offset in pax");

    ItemPointerSet(&tid, tbmres->blockno, cindex_);
  } else if (cindex_ < tbmres->ntuples) {
    // The maximum value of the last 16 bits in CTID is 0x7FFF + 1,
    // i.e. 0x8000. See layout of ItemPointerData in PAX
    if (tbmres->offsets[cindex_] > 0X8000)
      elog(ERROR, "unexpected offset in pax");

    ItemPointerSet(&tid, tbmres->blockno, tbmres->offsets[cindex_]);
  } else {
    return false;
  }
  ++cindex_;
  return index_desc_->FetchTuple(&tid, rs_base_.rs_snapshot, slot, nullptr,
                                 nullptr);
}

TableScanDesc PaxScanDesc::BeginScan(Relation relation, Snapshot snapshot,
                                     int nkeys, struct ScanKeyData * /*key*/,
                                     ParallelTableScanDesc pscan, uint32 flags,
                                     PaxFilter *filter, bool build_bitmap) {
  PaxScanDesc *desc;
  MemoryContext old_ctx;
  TableReader::ReaderOptions reader_options{};

  StaticAssertStmt(
      offsetof(PaxScanDesc, rs_base_) == 0,
      "rs_base should be the first field and aligned to the object address");

  desc = PAX_NEW<PaxScanDesc>();

  desc->memory_context_ = cbdb::AllocSetCtxCreate(
      CurrentMemoryContext, "Pax Storage", PAX_ALLOCSET_DEFAULT_SIZES);

  memset(&desc->rs_base_, 0, sizeof(desc->rs_base_));
  desc->rs_base_.rs_rd = relation;
  desc->rs_base_.rs_snapshot = snapshot;
  desc->rs_base_.rs_nkeys = nkeys;
  desc->rs_base_.rs_flags = flags;
  desc->rs_base_.rs_parallel = pscan;
  desc->reused_buffer_ = PAX_NEW<DataBuffer<char>>(pax_scan_reuse_buffer_size);
  desc->filter_ = filter;
  if (!desc->filter_) {
    desc->filter_ = PAX_NEW<PaxFilter>();
  }

  if (!desc->filter_->GetColumnProjection().first) {
    auto natts = cbdb::RelationGetAttributesNumber(relation);
    auto cols = PAX_NEW_ARRAY<bool>(natts);
    memset(cols, true, natts);
    desc->filter_->SetColumnProjection(cols, natts);
  }

#ifdef VEC_BUILD
  if (flags & SO_TYPE_VECTOR) {
    reader_options.is_vec = true;
    reader_options.adapter = std::make_shared<VecAdapter>(
        cbdb::RelationGetTupleDesc(relation), build_bitmap);
  }
#endif  // VEC_BUILD

#ifdef ENABLE_PLASMA
  if (pax_enable_plasma_in_mem) {
    std::string plasma_socket_path =
        std::string(desc->plasma_socket_path_prefix_);
    plasma_socket_path.append(std::to_string(PostPortNumber));
    plasma_socket_path.append("\0");
    PaxPlasmaCache::CacheOptions cache_options;
    cache_options.domain_socket = plasma_socket_path;
    cache_options.memory_quota = 0;
    cache_options.waitting_ms = 0;

    desc->pax_cache_ = PAX_NEW<PaxPlasmaCache>(std::move(cache_options));
    auto status = desc->pax_cache_->Initialize();
    if (!status.Ok()) {
      elog(WARNING, "Plasma cache client init failed, message: %s",
           status.Error().c_str());
      PAX_DELETE(desc->pax_cache_);
      desc->pax_cache_ = nullptr;
    }

    reader_options.pax_cache = desc->pax_cache_;
  }

#endif  // ENABLE_PLASMA

  old_ctx = MemoryContextSwitchTo(desc->memory_context_);

  // build reader
  reader_options.build_bitmap = build_bitmap;
  reader_options.reused_buffer = desc->reused_buffer_;
  reader_options.rel_oid = desc->rs_base_.rs_rd->rd_id;
  reader_options.filter = filter;

  auto iter = MicroPartitionInfoIterator::New(relation, snapshot);
  if (filter && filter->HasMicroPartitionFilter()) {
    auto wrap = PAX_NEW<FilterIterator<MicroPartitionMetadata>>(
        std::move(iter), [filter, relation](const auto &x) {
          MicroPartitionStatsProvider provider(x.GetStats());
          auto ok = filter->TestScan(provider, RelationGetDescr(relation),
                                     PaxFilterStatisticsKind::kFile);
          return ok;
        });
    iter = std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(wrap);
  }
  desc->reader_ = PAX_NEW<TableReader>(std::move(iter), reader_options);
  desc->reader_->Open();

  MemoryContextSwitchTo(old_ctx);
  pgstat_count_heap_scan(relation);
  return &desc->rs_base_;
}

void PaxScanDesc::EndScan() {
  if (pax_enable_debug && filter_) {
    filter_->LogStatistics();
  }

  Assert(reader_);
  reader_->Close();

  PAX_DELETE(reused_buffer_);
  PAX_DELETE(reader_);
  PAX_DELETE(filter_);

#ifdef ENABLE_PLASMA
  if (pax_cache_) {
    pax_cache_->Destroy();
    PAX_DELETE(pax_cache_);
  }
#endif

  PAX_DELETE(index_desc_);

  // TODO(jiaqizho): please double check with abort transaction @gongxun
  Assert(memory_context_);
  cbdb::MemoryCtxDelete(memory_context_);
  auto self = this;
  PAX_DELETE(self);
}

TableScanDesc PaxScanDesc::BeginScanExtractColumns(
    Relation rel, Snapshot snapshot, int /*nkeys*/,
    struct ScanKeyData * /*key*/, ParallelTableScanDesc parallel_scan,
    struct PlanState *ps, uint32 flags) {
  TableScanDesc paxscan;
  PaxFilter *filter;
  List *targetlist = ps->plan->targetlist;
  List *qual = ps->plan->qual;
  auto natts = cbdb::RelationGetAttributesNumber(rel);
  bool *cols;
  bool found = false;
  bool build_bitmap = true;
  PaxcExtractcolumnContext extract_column;

  filter = PAX_NEW<PaxFilter>();

  Assert(natts >= 0);

  cols = PAX_NEW_ARRAY<bool>(natts);
  memset(cols, false, natts);

  extract_column.cols = cols;
  extract_column.natts = natts;

  found = cbdb::ExtractcolumnsFromNode(reinterpret_cast<Node *>(targetlist),
                                       &extract_column);
  found = cbdb::ExtractcolumnsFromNode(reinterpret_cast<Node *>(qual), cols,
                                       natts) ||
          found;
  build_bitmap = cbdb::IsSystemAttrNumExist(&extract_column,
                                            SelfItemPointerAttributeNumber);

  // In some cases (for example, count(*)), targetlist and qual may be null,
  // extractcolumns_walker will return immediately, so no columns are specified.
  // We always scan the first column.
  if (!found && !build_bitmap && natts > 0) cols[0] = true;

  // The `cols` life cycle will be bound to `PaxFilter`
  filter->SetColumnProjection(cols, natts);

  if (pax_enable_filter) {
    ScanKey scan_keys = nullptr;
    int n_scan_keys = 0;
    auto ok = pax::BuildScanKeys(rel, qual, false, &scan_keys, &n_scan_keys);
    if (ok) filter->SetScanKeys(scan_keys, n_scan_keys);

    if (gp_enable_predicate_pushdown
#ifdef VEC_BUILD
        && !(flags & SO_TYPE_VECTOR)
#endif
    )
      filter->BuildExecutionFilterForColumns(rel, ps);
  }
  paxscan = BeginScan(rel, snapshot, 0, nullptr, parallel_scan, flags, filter,
                      build_bitmap);

  return paxscan;
}

// FIXME: shall we take these parameters into account?
void PaxScanDesc::ReScan(ScanKey /*key*/, bool /*set_params*/,
                         bool /*allow_strat*/, bool /*allow_sync*/,
                         bool /*allow_pagemode*/) {
  MemoryContext old_ctx;
  Assert(reader_);

  old_ctx = MemoryContextSwitchTo(memory_context_);
  reader_->ReOpen();
  MemoryContextSwitchTo(old_ctx);
}

bool PaxScanDesc::GetNextSlot(TupleTableSlot *slot) {
  MemoryContext old_ctx;
  bool ok = false;

  old_ctx = MemoryContextSwitchTo(memory_context_);

  Assert(reader_);
  ok = reader_->ReadTuple(slot);

  MemoryContextSwitchTo(old_ctx);
  return ok;
}

bool PaxScanDesc::ScanAnalyzeNextBlock(BlockNumber blockno,
                                       BufferAccessStrategy /*bstrategy*/) {
  target_tuple_id_ = blockno;
  return true;
}

bool PaxScanDesc::ScanAnalyzeNextTuple(TransactionId /*oldest_xmin*/,
                                       double *liverows,
                                       const double * /* deadrows */,
                                       TupleTableSlot *slot) {
  MemoryContext old_ctx;
  bool ok = false;

  old_ctx = MemoryContextSwitchTo(memory_context_);
  while (next_tuple_id_ < target_tuple_id_) {
    ok = GetNextSlot(slot);
    if (!ok) break;
    next_tuple_id_++;
  }
  if (next_tuple_id_ == target_tuple_id_) {
    ok = GetNextSlot(slot);
    next_tuple_id_++;
    if (ok) *liverows += 1;
  }
  MemoryContextSwitchTo(old_ctx);
  return ok;
}

bool PaxScanDesc::ScanSampleNextBlock(SampleScanState *scanstate) {
  MemoryContext old_ctx;
  TsmRoutine *tsm = scanstate->tsmroutine;
  BlockNumber blockno = 0;
  BlockNumber pages = 0;
  double total_tuples = 0;
  int32 attrwidths = 0;
  double allvisfrac = 0;
  bool ok = false;

  old_ctx = MemoryContextSwitchTo(memory_context_);

  if (total_tuples_ == 0) {
    paxc::PaxAccessMethod::EstimateRelSize(rs_base_.rs_rd, &attrwidths, &pages,
                                           &total_tuples, &allvisfrac);
    total_tuples_ = total_tuples;
  }

  if (tsm->NextSampleBlock)
    blockno = tsm->NextSampleBlock(scanstate, total_tuples_);
  else
    blockno = system_nextsampleblock(scanstate, total_tuples_);

  ok = BlockNumberIsValid(blockno);
  if (ok) fetch_tuple_id_ = blockno;

  MemoryContextSwitchTo(old_ctx);
  return ok;
}

bool PaxScanDesc::ScanSampleNextTuple(SampleScanState * /*scanstate*/,
                                      TupleTableSlot *slot) {
  MemoryContext old_ctx;
  bool ok = false;

  old_ctx = MemoryContextSwitchTo(memory_context_);
  while (next_tuple_id_ < fetch_tuple_id_) {
    ok = GetNextSlot(slot);
    if (!ok) break;
    next_tuple_id_++;
  }
  MemoryContextSwitchTo(old_ctx);
  return ok;
}

}  // namespace pax
