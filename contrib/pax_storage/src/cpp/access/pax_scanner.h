#pragma once

#include "comm/cbdb_api.h"

#include <unordered_set>

#include "storage/pax.h"
#include "storage/pax_filter.h"
#ifdef VEC_BUILD
#include "storage/vec/pax_vec_adapter.h"
#endif

namespace paxc {
bool IndexUniqueCheck(Relation rel, ItemPointer tid, Snapshot snapshot,
                      bool *all_dead);
}

namespace pax {
class PaxIndexScanDesc final {
 public:
  explicit PaxIndexScanDesc(Relation rel);
  ~PaxIndexScanDesc();
  bool FetchTuple(ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot,
                  bool *call_again, bool *all_dead);
  inline IndexFetchTableData *ToBase() { return &base_; }
  static inline PaxIndexScanDesc *FromBase(IndexFetchTableData *base) {
    return reinterpret_cast<PaxIndexScanDesc *>(base);
  }

 private:
  bool OpenMicroPartition(BlockNumber block, Snapshot snapshot);

  std::string rel_path_;
  IndexFetchTableData base_;
  BlockNumber current_block_ = InvalidBlockNumber;
  MicroPartitionReader *reader_ = nullptr;
};

class PaxScanDesc {
 public:
  static TableScanDesc BeginScan(Relation relation, Snapshot snapshot,
                                 int nkeys, struct ScanKeyData *key,
                                 ParallelTableScanDesc pscan, uint32 flags,
                                 PaxFilter *filter, bool build_bitmap);

  static TableScanDesc BeginScanExtractColumns(
      Relation rel, Snapshot snapshot, int nkeys, struct ScanKeyData *key,
      ParallelTableScanDesc parallel_scan, struct PlanState *ps, uint32 flags);

  void EndScan();
  void ReScan(ScanKey key, bool set_params, bool allow_strat, bool allow_sync,
              bool allow_pagemode);

  bool GetNextSlot(TupleTableSlot *slot);

  bool ScanAnalyzeNextBlock(BlockNumber blockno,
                            BufferAccessStrategy bstrategy);
  bool ScanAnalyzeNextTuple(TransactionId oldest_xmin, double *liverows,
                            const double *deadrows, TupleTableSlot *slot);

  bool ScanSampleNextBlock(SampleScanState *scanstate);

  bool ScanSampleNextTuple(SampleScanState *scanstate, TupleTableSlot *slot);

  bool BitmapNextBlock(struct TBMIterateResult *tbmres);
  bool BitmapNextTuple(struct TBMIterateResult *tbmres, TupleTableSlot *slot);

  ~PaxScanDesc() = default;

  static inline PaxScanDesc *ToDesc(TableScanDesc scan) {
    auto desc = reinterpret_cast<PaxScanDesc *>(scan);
    return desc;
  }

 private:
  PaxScanDesc() = default;

 private:
  TableScanDescData rs_base_{};

  TableReader *reader_ = nullptr;

  DataBuffer<char> *reused_buffer_ = nullptr;

  MemoryContext memory_context_ = nullptr;

  // Only used by `scan analyze` and `scan sample`
  uint64 next_tuple_id_ = 0;
  // Only used by `scan analyze`
  uint64 target_tuple_id_ = 0;
  // Only used by `scan sample`
  uint64 fetch_tuple_id_ = 0;
  uint64 total_tuples_ = 0;

  // filter used to do column projection
  PaxFilter *filter_ = nullptr;
#ifdef VEC_BUILD
  VecAdapter *vec_adapter_ = nullptr;
#endif

#ifdef ENABLE_PLASMA
  const std::string plasma_socket_path_prefix_ = "/tmp/.s.plasma.";
  PaxCache *pax_cache_ = nullptr;
#endif

  // used only by bitmap index scan
  PaxIndexScanDesc *index_desc_ = nullptr;
  int cindex_ = 0;
};  // class PaxScanDesc

}  // namespace pax
