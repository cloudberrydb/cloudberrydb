#pragma once
#include "comm/cbdb_api.h"

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

class MicroPartitionStatsData {
 public:
  virtual void CopyFrom(MicroPartitionStatsData *stats) = 0;
  virtual ::pax::stats::ColumnBasicInfo *GetColumnBasicInfo(
      int column_index) = 0;
  virtual ::pax::stats::ColumnDataStats *GetColumnDataStats(
      int column_index) = 0;
  virtual int ColumnSize() const = 0;
  virtual void SetAllNull(int column_index, bool allnull) = 0;
  virtual void SetHasNull(int column_index, bool hasnull) = 0;
  virtual bool GetAllNull(int column_index) = 0;
  virtual bool GetHasNull(int column_index) = 0;
  virtual ~MicroPartitionStatsData() = default;
};

class MicroPartittionFileStatsData final : public MicroPartitionStatsData {
 public:
  MicroPartittionFileStatsData(::pax::stats::MicroPartitionStatisticsInfo *info,
                               int natts);
  void CopyFrom(MicroPartitionStatsData *stats) override;
  ::pax::stats::ColumnBasicInfo *GetColumnBasicInfo(int column_index) override;
  ::pax::stats::ColumnDataStats *GetColumnDataStats(int column_index) override;
  int ColumnSize() const override;
  void SetAllNull(int column_index, bool allnull) override;
  void SetHasNull(int column_index, bool hasnull) override;
  bool GetAllNull(int column_index) override;
  bool GetHasNull(int column_index) override;

 private:
  ::pax::stats::MicroPartitionStatisticsInfo *info_ = nullptr;
};

class MicroPartitionStats final {
 public:
  explicit MicroPartitionStats(bool allow_fallback_to_pg = false);
  ~MicroPartitionStats();
  MicroPartitionStats *SetMinMaxColumnIndex(std::vector<int> &&minmax_columns);
  MicroPartitionStats *SetStatsMessage(MicroPartitionStatsData *stats,
                                       int natts);
  MicroPartitionStats *LightReset();

  void AddRow(TupleTableSlot *slot);
  MicroPartitionStatsData *GetStatsData() { return stats_; }
  const MicroPartitionStatsData *GetStatsData() const { return stats_; }
  const std::vector<int> GetMinMaxColumnIndex() const { return minmax_columns_; }

  void MergeTo(MicroPartitionStats *stats, TupleDesc desc);

  static std::string ToValue(Datum datum, int typlen, bool typbyval);
  static Datum FromValue(const std::string &s, int typlen, bool typbyval,
                         bool *ok);

  void DoInitialCheck(TupleDesc desc);

 private:
  void AddNullColumn(int column_index);
  void AddNonNullColumn(int column_index, Datum value, TupleDesc desc);

  void UpdateMinMaxValue(int column_index, Datum datum, Oid collation,
                         int typlen, bool typbyval);

  // stats_: only references the info object by pointer
  MicroPartitionStatsData *stats_ = nullptr;

  std::vector<Oid> opfamilies_;
  // less: pair[0], greater: pair[1]
  std::vector<std::pair<FmgrInfo, FmgrInfo>> finfos_;
  std::vector<std::pair<OperMinMaxFunc, OperMinMaxFunc>> local_funcs_;
  bool allow_fallback_to_pg_ = false;

  // status to indicate whether the oids are initialized
  // or the min-max values are initialized
  // 'u': all is uninitialized
  // 'x': column doesn't support min-max
  // 'n': oids are initialized, but min-max value is missing
  // 'y': min-max is set, needs update.
  std::vector<char> status_;
  std::vector<int> minmax_columns_;
  bool initial_check_ = false;
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
