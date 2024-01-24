#pragma once
#include "comm/cbdb_api.h"

#include <map>
#include <utility>
#include <vector>

#include "comm/guc.h"
#include "comm/log.h"
#include "storage/pax_defined.h"
namespace pax {
namespace stats {
class MicroPartitionStatisticsInfo;
class ColumnBasicInfo;
class ColumnDataStats;
}  // namespace stats

struct ExecutionFilterContext {
  ExprContext *econtext;
  ExprState *estate_final = nullptr;
  ExprState **estates;
  AttrNumber *attnos;
  int size = 0;
  inline bool HasExecutionFilter() const { return size > 0 || estate_final; }
};

bool BuildScanKeys(Relation rel, List *quals, bool isorderby,
                   ScanKey *scan_keys, int *num_scan_keys);
bool BuildExecutionFilterForColumns(Relation rel, PlanState *ps,
                                    pax::ExecutionFilterContext *ctx);
class ColumnStatsProvider {
 public:
  virtual ~ColumnStatsProvider() = default;
  virtual int ColumnSize() const = 0;
  virtual bool AllNull(int column_index) const = 0;
  virtual bool HasNull(int column_index) const = 0;
  virtual const ::pax::stats::ColumnBasicInfo &ColumnInfo(
      int column_index) const = 0;
  virtual const ::pax::stats::ColumnDataStats &DataStats(
      int column_index) const = 0;
};

class PaxFilter final {
 public:
  explicit PaxFilter(bool allow_fallback_to_pg = pax_allow_oper_fallback);

  ~PaxFilter();

  bool HasMicroPartitionFilter() const { return num_scan_keys_ > 0; }

  std::pair<bool *, size_t> GetColumnProjection();

  void SetColumnProjection(bool *proj, size_t proj_len);

  void SetScanKeys(ScanKey scan_keys, int num_scan_keys);

  ExecutionFilterContext *GetExecutionFilterContext() { return &efctx_; }
  const std::vector<AttrNumber> &GetRemainingColumns() const {
    return remaining_attnos_;
  }

  // true: if failed to filter the whole micro-partition, reader SHOULD scan the
  // tuples false: if success to filter the micro-partition, the whole
  // micro-partition SHOULD be ignored.
  inline bool TestScan(const ColumnStatsProvider &provider,
                       const TupleDesc desc, int kind) {
    bool filter_failed = true;
    if (num_scan_keys_ == 0) goto finish;
    filter_failed = TestScanInternal(provider, desc);

  finish:
    if (!filter_failed) {
      hits_[kind] += 1;
    }

    totals_[kind] += 1;
    return filter_failed;
  }

  inline void LogStatistics() {
    for (size_t i = 0; i < filter_kind_desc.size(); i++) {
      if (this->totals_[i] == 0) {
        PAX_LOG("kind %s, no filter. ", filter_kind_desc[i]);
      } else {
        PAX_LOG("kind %s, filter rate: %d / %d", filter_kind_desc[i], hits_[i],
                this->totals_[i]);
      }
    }
  }

  inline bool HasRowScanFilter() const { return efctx_.HasExecutionFilter(); }
  inline bool BuildExecutionFilterForColumns(Relation rel, PlanState *ps) {
    auto ok = pax::BuildExecutionFilterForColumns(rel, ps, &efctx_);
    if (ok) FillRemainingColumns(rel);
    return ok;
  }

 private:
  bool TestScanInternal(const ColumnStatsProvider &provider,
                        TupleDesc desc) const;

  void FillRemainingColumns(Relation rel);

  bool allow_fallback_to_pg_ = false;

  // micro partition filter: we use the scan keys to filter a whole of micro
  // partition by comparing the scan keys with the min/max values in micro
  // partition stats. The memory of the scan keys is allocated by alloc.
  // PaxFilter assumes it only references them.
  ScanKey scan_keys_ = nullptr;
  int num_scan_keys_ = 0;

  // column projection
  bool *proj_ = nullptr;
  size_t proj_len_ = 0;

  // row-level filter
  ExecutionFilterContext efctx_;
  // all selected columns - single row filting columns
  // before running final cross columns expression filtering, the remaining
  // columns should be filled.
  std::vector<AttrNumber> remaining_attnos_;

  std::map<int, int> hits_;
  std::map<int, int> totals_;
};  // class PaxFilter

}  // namespace pax
