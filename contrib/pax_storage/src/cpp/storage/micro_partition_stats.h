#pragma once

#include <string>
#include <utility>
#include <vector>

#include "comm/guc.h"
#include "storage/oper/pax_stats.h"
#include "storage/pax_filter.h"

namespace pax {
namespace stats {
class MicroPartitionStatisticsInfo;
class ColumnBasicInfo;
class ColumnDataStats;
}  // namespace stats
class MicroPartitionStatsData;
struct SumStatsInMem;

class MicroPartitionStats final {
 public:
  MicroPartitionStats(TupleDesc desc, bool allow_fallback_to_pg = false);
  ~MicroPartitionStats();

  void Initialize(const std::vector<int> &minmax_columns);
  void AddRow(TupleTableSlot *slot, const std::vector<Datum> &detoast_vals);
  MicroPartitionStats *Reset();
  ::pax::stats::MicroPartitionStatisticsInfo *Serialize();

  void MergeTo(MicroPartitionStats *stats);
  ::pax::stats::ColumnBasicInfo *GetColumnBasicInfo(int column_index) const;

  // used to encode/decode datum
  static std::string ToValue(Datum datum, int typlen, bool typbyval);
  static Datum FromValue(const std::string &s, int typlen, bool typbyval,
                         bool *ok);

  static bool MicroPartitionStatisticsInfoCombine(
      stats::MicroPartitionStatisticsInfo *left,
      stats::MicroPartitionStatisticsInfo *right, TupleDesc desc,
      bool allow_fallback_to_pg = false);

 private:
  void AddNullColumn(int column_index);
  void AddNonNullColumn(int column_index, Datum value, Datum detoast);
  void UpdateMinMaxValue(int column_index, Datum datum, Oid collation,
                         int typlen, bool typbyval);
  void UpdateSumValue(int column_index, SumStatsInMem *sum_stats);
  void CopyDatum(Datum src, Datum *dst, int typlen, bool typbyval);

 private:
  TupleDesc tuple_desc_;
  // stats_: only references the info object by pointer
  MicroPartitionStatsData *stats_;
  std::vector<Datum> min_in_mem_;
  std::vector<Datum> max_in_mem_;

  // the stats to desc sum
  std::vector<SumStatsInMem> sum_stats_;
  AggState *agg_state_;
  ExprContext expr_context_;

  std::vector<Oid> opfamilies_;
  // less: pair[0], greater: pair[1]
  std::vector<std::pair<FmgrInfo, FmgrInfo>> finfos_;
  std::vector<std::pair<OperMinMaxFunc, OperMinMaxFunc>> local_funcs_;
  bool allow_fallback_to_pg_ = false;  // only effect min/max

  // status to indicate whether the oids are initialized
  // or the min-max values are initialized
  std::vector<char> status_;
  bool initialized_ = false;
};

class MicroPartitionStatsProvider final : public ColumnStatsProvider {
 public:
  explicit MicroPartitionStatsProvider(
      const ::pax::stats::MicroPartitionStatisticsInfo &stats);
  int ColumnSize() const override;
  bool AllNull(int column_index) const override;
  bool HasNull(int column_index) const override;
  const ::pax::stats::ColumnBasicInfo &ColumnInfo(
      int column_index) const override;
  const ::pax::stats::ColumnDataStats &DataStats(
      int column_index) const override;

 private:
  const ::pax::stats::MicroPartitionStatisticsInfo &stats_;
};

}  // namespace pax
