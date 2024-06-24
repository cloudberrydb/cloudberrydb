#include "storage/micro_partition_stats.h"

#include "comm/cbdb_api.h"

#include "comm/cbdb_wrappers.h"
#include "comm/fmt.h"
#include "comm/pax_memory.h"
#include "storage/micro_partition_metadata.h"
#include "storage/proto/proto_wrappers.h"

namespace pax {

class MicroPartitionStatsData final {
 public:
  MicroPartitionStatsData(int natts);
  void Reset();
  void CopyFrom(MicroPartitionStatsData *stats);
  ::pax::stats::ColumnBasicInfo *GetColumnBasicInfo(int column_index);
  ::pax::stats::ColumnDataStats *GetColumnDataStats(int column_index);
  int ColumnSize() const;
  void SetAllNull(int column_index, bool allnull);
  void SetHasNull(int column_index, bool hasnull);
  bool GetAllNull(int column_index);
  bool GetHasNull(int column_index);

  inline ::pax::stats::MicroPartitionStatisticsInfo *GetStatsInfoRef() {
    return &info_;
  }

 private:
  ::pax::stats::MicroPartitionStatisticsInfo info_;
};

bool MicroPartitionStats::MicroPartitionStatisticsInfoCombine(
    stats::MicroPartitionStatisticsInfo *left,
    stats::MicroPartitionStatisticsInfo *right, TupleDesc desc,
    bool allow_fallback_to_pg) {
  std::vector<std::pair<OperMinMaxFunc, OperMinMaxFunc>> funcs;
  std::vector<std::pair<FmgrInfo, FmgrInfo>> finfos;

  Assert(left);
  Assert(right);
  Assert(left->columnstats_size() <= desc->natts);
  if (left->columnstats_size() != right->columnstats_size()) {
    // exist drop/add column, schema not match
    return false;
  }

  funcs.reserve(desc->natts);
  finfos.reserve(desc->natts);

  // check before update left stats
  for (int i = 0; i < left->columnstats_size(); i++) {
    auto left_column_stats = left->columnstats(i);
    auto right_column_stats = right->columnstats(i);
    auto left_column_data_stats = left_column_stats.datastats();
    auto right_column_data_stats = right_column_stats.datastats();

    auto attr = TupleDescAttr(desc, i);
    auto collation = attr->attcollation;
    Oid op_family;
    FmgrInfo finfo;
    bool get_pg_oper_succ = false;

    // all null && has null must be exist.
    // And we must not check has_allnull/has_hasnull, cause it may return false
    // if we have not update allnull/hasnull it in `AddRows`.
    // The current stats may not have been serialized in disk.

    // if current column stats do have collation but
    // not same, then we can't combine two of stats
    if (left_column_stats.info().collation() !=
        right_column_stats.info().collation()) {
      return false;
    }

    // Current relation collation changed, the min/max may not work
    if (collation != left_column_stats.info().collation() &&
        left_column_stats.info().collation() != 0) {
      return false;
    }

    // the column_stats.info() can be null, if current column is allnull
    // So don't assert typeoid/opfamily/collation here

    funcs.emplace_back(
        std::pair<OperMinMaxFunc, OperMinMaxFunc>({nullptr, nullptr}));
    GetStrategyProcinfo(attr->atttypid, attr->atttypid, funcs[i]);
    if (allow_fallback_to_pg) {
      finfos[i] = {finfo, finfo};
      get_pg_oper_succ = GetStrategyProcinfo(attr->atttypid, attr->atttypid,
                                             &op_family, finfos[i]);
    }

    // if current min/max from pg_oper, but now allow_fallback_to_pg is false
    if (right_column_data_stats.has_minimal() &&
        left_column_data_stats.has_minimal() &&
        !(funcs[i].first && CollateIsSupport(collation)) && !get_pg_oper_succ) {
      return false;
    }

    if (right_column_data_stats.has_maximum() &&
        left_column_data_stats.has_maximum() &&
        !(funcs[i].second && CollateIsSupport(collation)) &&
        !get_pg_oper_succ) {
      return false;
    }
  }

  for (int i = 0; i < left->columnstats_size(); i++) {
    auto left_column_stats = left->mutable_columnstats(i);
    auto right_column_stats = right->mutable_columnstats(i);

    auto left_column_data_stats = left_column_stats->mutable_datastats();
    auto right_column_data_stats = right_column_stats->mutable_datastats();
    auto attr = TupleDescAttr(desc, i);

    auto typlen = attr->attlen;
    auto typbyval = attr->attbyval;
    bool ok = false;
    auto collation = right_column_stats->info().collation();

    if (left_column_stats->allnull() && !right_column_stats->allnull()) {
      left_column_stats->set_allnull(false);
    }

    if (!left_column_stats->hasnull() && right_column_stats->hasnull()) {
      left_column_stats->set_hasnull(true);
    }

    if (right_column_data_stats->has_minimal()) {
      if (left_column_data_stats->has_minimal()) {
        auto left_min_datum = MicroPartitionStats::FromValue(
            left_column_data_stats->minimal(), typlen, typbyval, &ok);
        CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError,
                   fmt("Fail to parse the MIN datum in LEFT pb [typbyval=%d, "
                       "typlen=%d, column_index=%d]",
                       typbyval, typlen, i));

        auto right_min_datum = MicroPartitionStats::FromValue(
            right_column_data_stats->minimal(), typlen, typbyval, &ok);
        CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError,
                   fmt("Fail to parse the MIN datum in RIGHT pb [typbyval=%d, "
                       "typlen=%d, column_index=%d]",
                       typbyval, typlen, i));
        bool min_rc = false;

        // can direct call the oper, no need check exist again
        if (funcs[i].first) {
          min_rc = funcs[i].first(&right_min_datum, &left_min_datum, collation);
        } else {
          min_rc = DatumGetBool(cbdb::FunctionCall2Coll(
              &finfos[i].first, collation, right_min_datum, left_min_datum));
        }

        if (min_rc) {
          left_column_data_stats->set_minimal(
              ToValue(right_min_datum, typlen, typbyval));
        }
      } else {
        left_column_data_stats->set_minimal(right_column_data_stats->minimal());
      }
    }

    if (right_column_data_stats->has_maximum()) {
      if (left_column_data_stats->has_maximum()) {
        auto left_max_datum = MicroPartitionStats::FromValue(
            left_column_data_stats->maximum(), typlen, typbyval, &ok);
        CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError,
                   fmt("Fail to parse the MAX datum in LEFT pb [typbyval=%d, "
                       "typlen=%d, column_index=%d]",
                       typbyval, typlen, i));

        auto right_max_datum = MicroPartitionStats::FromValue(
            right_column_data_stats->maximum(), typlen, typbyval, &ok);
        CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError,
                   fmt("Fail to parse the MAX datum in RIGHT pb [typbyval=%d, "
                       "typlen=%d, column_index=%d]",
                       typbyval, typlen, i));
        bool max_rc = false;

        if (funcs[i].second) {
          max_rc =
              funcs[i].second(&right_max_datum, &left_max_datum, collation);
        } else {  // no need check again
          max_rc = DatumGetBool(cbdb::FunctionCall2Coll(
              &finfos[i].second, collation, right_max_datum, left_max_datum));
        }

        if (max_rc) {
          left_column_data_stats->set_maximum(
              MicroPartitionStats::ToValue(right_max_datum, typlen, typbyval));
        }

      } else {
        left_column_data_stats->set_maximum(right_column_data_stats->maximum());
      }
    }
  }
  return true;
}

MicroPartitionStats::MicroPartitionStats(TupleDesc desc,
                                         bool allow_fallback_to_pg)
    : tuple_desc_(desc), allow_fallback_to_pg_(allow_fallback_to_pg) {
  FmgrInfo finfo;
  int natts;

  Assert(tuple_desc_);
  natts = tuple_desc_->natts;

  stats_ = PAX_NEW<MicroPartitionStatsData>(natts);

  memset(&finfo, 0, sizeof(finfo));
  opfamilies_.clear();
  finfos_.clear();
  local_funcs_.clear();
  status_.clear();
  for (int i = 0; i < natts; i++) {
    opfamilies_.emplace_back(InvalidOid);
    finfos_.emplace_back(std::pair<FmgrInfo, FmgrInfo>({finfo, finfo}));
    local_funcs_.emplace_back(
        std::pair<OperMinMaxFunc, OperMinMaxFunc>({nullptr, nullptr}));
    status_.emplace_back('u');
    min_in_mem_.emplace_back(0);
    max_in_mem_.emplace_back(0);
  }
}

MicroPartitionStats::~MicroPartitionStats() { PAX_DELETE(stats_); }

::pax::stats::MicroPartitionStatisticsInfo *MicroPartitionStats::Serialize() {
  auto n = tuple_desc_->natts;
  stats::ColumnDataStats *data_stats;

  for (auto column_index = 0; column_index < n; column_index++) {
    // only 'y' need set to the stats_
    if (status_[column_index] == 'y') {
      auto att = TupleDescAttr(tuple_desc_, column_index);
      auto typlen = att->attlen;
      auto typbyval = att->attbyval;

      data_stats = stats_->GetColumnDataStats(column_index);
      data_stats->set_minimal(
          ToValue(min_in_mem_[column_index], typlen, typbyval));
      data_stats->set_maximum(
          ToValue(max_in_mem_[column_index], typlen, typbyval));

      // after serialize to pb, clear the memory
      if (!typbyval && typlen == -1) {
        char *ref = (char *)cbdb::DatumToPointer(min_in_mem_[column_index]);
        PAX_DELETE(ref);
        ref = (char *)cbdb::DatumToPointer(max_in_mem_[column_index]);
        PAX_DELETE(ref);
      }

      min_in_mem_[column_index] = 0;
      max_in_mem_[column_index] = 0;
    }
  }

  return stats_->GetStatsInfoRef();
}

::pax::stats::ColumnBasicInfo *MicroPartitionStats::GetColumnBasicInfo(
    int column_index) const {
  return stats_->GetColumnBasicInfo(column_index);
}

MicroPartitionStats *MicroPartitionStats::Reset() {
  for (char &status : status_) {
    Assert(status == 'n' || status == 'y' || status == 'x');
    if (status == 'y') status = 'n';
  }
  stats_->Reset();
  return this;
}

void MicroPartitionStats::AddRow(TupleTableSlot *slot,
                                 const std::vector<Datum> &detoast_vals) {
  auto n = tuple_desc_->natts;

  Assert(initialized_);
  CBDB_CHECK(status_.size() == static_cast<size_t>(n),
             cbdb::CException::ExType::kExTypeSchemaNotMatch,
             fmt("Current stats initialized [N=%lu], in tuple desc [natts=%d] ",
                 status_.size(), n));
  for (auto i = 0; i < n; i++) {
    AssertImply(tuple_desc_->attrs[i].attisdropped, slot->tts_isnull[i]);
    if (slot->tts_isnull[i])
      AddNullColumn(i);
    else
      AddNonNullColumn(i, slot->tts_values[i], detoast_vals[i]);
  }
}

void MicroPartitionStats::MergeTo(MicroPartitionStats *stats) {
  Assert(status_.size() == stats->status_.size());
  for (size_t column_index = 0; column_index < status_.size(); column_index++) {
    auto att = TupleDescAttr(tuple_desc_, column_index);
    auto collation = att->attcollation;
    auto typlen = att->attlen;
    auto typbyval = att->attbyval;

    Assert(status_[column_index] != 'u' && stats->status_[column_index] != 'u');
    AssertImply(stats->status_[column_index] == 'x',
                status_[column_index] == 'x');

    if (stats->status_[column_index] == 'n' ||
        stats->status_[column_index] == 'x') {
      // still need update all and has null
      if (stats_->GetAllNull(column_index) &&
          !stats->stats_->GetAllNull(column_index)) {
        stats_->SetAllNull(column_index, false);
      }

      if (!stats_->GetHasNull(column_index) &&
          stats->stats_->GetHasNull(column_index)) {
        stats_->SetHasNull(column_index, true);
      }
      continue;
    }

    Assert(stats->status_[column_index] == 'y');
    Assert(status_[column_index] != 'x');
    if (status_[column_index] == 'y') {
      auto col_basic_stats_merge pg_attribute_unused() =
          stats->stats_->GetColumnBasicInfo(column_index);

      auto col_basic_stats pg_attribute_unused() =
          stats_->GetColumnBasicInfo(column_index);

      Assert(col_basic_stats->typid() == col_basic_stats_merge->typid());
      Assert(col_basic_stats->opfamily() == col_basic_stats_merge->opfamily());
      Assert(col_basic_stats->collation() ==
             col_basic_stats_merge->collation());
      Assert(col_basic_stats->collation() == collation);

      if (stats_->GetAllNull(column_index) &&
          !stats->stats_->GetAllNull(column_index)) {
        stats_->SetAllNull(column_index, false);
      }

      if (!stats_->GetHasNull(column_index) &&
          stats->stats_->GetHasNull(column_index)) {
        stats_->SetHasNull(column_index, true);
      }

      UpdateMinMaxValue(column_index, stats->min_in_mem_[column_index],
                        collation, typlen, typbyval);
      UpdateMinMaxValue(column_index, stats->max_in_mem_[column_index],
                        collation, typlen, typbyval);
    } else if (status_[column_index] == 'n') {
      stats_->CopyFrom(stats->stats_);

      CopyMinMaxValue(stats->min_in_mem_[column_index],
                      &min_in_mem_[column_index], typlen, typbyval);
      CopyMinMaxValue(stats->max_in_mem_[column_index],
                      &max_in_mem_[column_index], typlen, typbyval);

      status_[column_index] = 'y';
    } else {
      Assert(false);
    }
  }
}

void MicroPartitionStats::AddNullColumn(int column_index) {
  Assert(column_index >= 0);
  Assert(column_index < static_cast<int>(opfamilies_.size()));

  stats_->SetHasNull(column_index, true);
}

void MicroPartitionStats::AddNonNullColumn(int column_index, Datum value,
                                           Datum detoast) {
  Assert(column_index >= 0);
  Assert(column_index < static_cast<int>(opfamilies_.size()));

  auto att = TupleDescAttr(tuple_desc_, column_index);
  auto collation = att->attcollation;
  auto typlen = att->attlen;
  auto typbyval = att->attbyval;
  auto info = stats_->GetColumnBasicInfo(column_index);
  stats_->SetAllNull(column_index, false);

  if (detoast != 0 && value != detoast) {
    value = detoast;
  }

  // update min/max
  switch (status_[column_index]) {
    case 'x':
      break;
    case 'y':
      Assert(info->has_typid());
      Assert(info->has_opfamily());
      Assert(info->typid() == att->atttypid);
      Assert(info->collation() == collation);
      Assert(info->opfamily() == opfamilies_[column_index]);

      UpdateMinMaxValue(column_index, value, collation, typlen, typbyval);
      break;
    case 'n': {
      AssertImply(info->has_typid(), info->typid() == att->atttypid);
      AssertImply(info->has_collation(), info->collation() == collation);
      AssertImply(info->has_opfamily(),
                  info->opfamily() == opfamilies_[column_index]);
#ifdef USE_ASSERT_CHECKING
      auto data_stats = stats_->GetColumnDataStats(column_index);
      Assert(!data_stats->has_minimal());
      Assert(!data_stats->has_maximum());
#endif

      info->set_typid(att->atttypid);
      info->set_collation(collation);
      info->set_opfamily(opfamilies_[column_index]);

      CopyMinMaxValue(value, &min_in_mem_[column_index], typlen, typbyval);
      CopyMinMaxValue(value, &max_in_mem_[column_index], typlen, typbyval);

      status_[column_index] = 'y';
      break;
    }
    default:
      Assert(false);
  }
}

void MicroPartitionStats::UpdateMinMaxValue(int column_index, Datum datum,
                                            Oid collation, int typlen,
                                            bool typbyval) {
  Datum min_datum, max_datum;
  bool umin = false, umax = false;

  Assert(initialized_);
  Assert(column_index >= 0 &&
         static_cast<size_t>(column_index) < status_.size());
  Assert(status_[column_index] == 'y');

  min_datum = min_in_mem_[column_index];
  max_datum = max_in_mem_[column_index];

  // If do have local oper here, then direct call it
  // But if local oper is null, it must be fallback to pg
  auto &lfunc = local_funcs_[column_index];
  if (lfunc.first && CollateIsSupport(collation)) {
    Assert(lfunc.second);

    umin = lfunc.first(&datum, &min_datum, collation);
    umax = lfunc.second(&datum, &max_datum, collation);
  } else if (allow_fallback_to_pg_) {  // may not support collation,
    auto &finfos = finfos_[column_index];
    umin = DatumGetBool(
        cbdb::FunctionCall2Coll(&finfos.first, collation, datum, min_datum));
    umax = DatumGetBool(
        cbdb::FunctionCall2Coll(&finfos.second, collation, datum, max_datum));
  } else {
    // unreachable
    Assert(false);
  }

  if (umin)
    CopyMinMaxValue(datum, &min_in_mem_[column_index], typlen, typbyval);
  if (umax)
    CopyMinMaxValue(datum, &max_in_mem_[column_index], typlen, typbyval);
}

void MicroPartitionStats::CopyMinMaxValue(Datum src, Datum *dst, int typlen,
                                          bool typbyval) {
  if (typbyval) {
    Assert(typlen == 1 || typlen == 2 || typlen == 4 || typlen == 8);
    *dst = src;
  } else if (typlen == -1) {
    struct varlena *val;
    int len;

    val = (struct varlena *)cbdb::PointerAndLenFromDatum(src, &len);
    Assert(val && len != -1);

    auto alloc_new_datum = [](struct varlena *vl, int vl_len, Datum *dest) {
      char *tmp = PAX_NEW_ARRAY<char>(vl_len);
      memcpy(tmp, (char *)vl, vl_len);
      *dest = PointerGetDatum(tmp);
    };

    // check the source datum
    if (*dst == 0) {
      alloc_new_datum(val, len, dst);
    } else {
      int dst_len;
      char *result = (char *)cbdb::PointerAndLenFromDatum(*dst, &dst_len);
      if (dst_len > len) {
        memcpy(result, (char *)val, len);
      } else {
        PAX_DELETE(result);
        alloc_new_datum(val, len, dst);
      }
    }
  }
}

void MicroPartitionStats::Initialize(const std::vector<int> &minmax_columns) {
  auto natts = tuple_desc_->natts;
  int num_minmax_columns;

  Assert(natts == static_cast<int>(status_.size()));
  Assert(status_.size() == opfamilies_.size());
  Assert(status_.size() == finfos_.size());

  if (initialized_) {
    return;
  }

  num_minmax_columns = static_cast<int>(minmax_columns.size());
  Assert(num_minmax_columns <= natts);

#ifdef USE_ASSERT_CHECKING
  // minmax_columns should be sorted
  for (int i = 1; i < num_minmax_columns; i++)
    Assert(minmax_columns[i - 1] < minmax_columns[i]);
#endif

  for (int i = 0, j = 0; i < natts; i++) {
    auto att = TupleDescAttr(tuple_desc_, i);

    if (att->attisdropped) {
      status_[i] = 'x';
      continue;
    }
    while (j < num_minmax_columns && minmax_columns[j] < i) j++;
    if (j >= num_minmax_columns || minmax_columns[j] != i) {
      status_[i] = 'x';
      continue;
    }
    j++;

    if (GetStrategyProcinfo(att->atttypid, att->atttypid, local_funcs_[i])) {
      status_[i] = 'n';
      continue;
    }

    if (!allow_fallback_to_pg_ ||
        !GetStrategyProcinfo(att->atttypid, att->atttypid, &opfamilies_[i],
                             finfos_[i])) {
      status_[i] = 'x';
      continue;
    }

    status_[i] = 'n';
  }
  initialized_ = true;
}

Datum MicroPartitionStats::FromValue(const std::string &s, int typlen,
                                     bool typbyval, bool *ok) {
  const char *p = s.data();
  *ok = true;
  if (typbyval) {
    Assert(typlen > 0);
    switch (typlen) {
      case 1: {
        int8 i = *reinterpret_cast<const int8 *>(p);
        return cbdb::Int8ToDatum(i);
      }
      case 2: {
        int16 i = *reinterpret_cast<const int16 *>(p);
        return cbdb::Int16ToDatum(i);
      }
      case 4: {
        int32 i = *reinterpret_cast<const int32 *>(p);
        return cbdb::Int32ToDatum(i);
      }
      case 8: {
        int64 i = *reinterpret_cast<const int64 *>(p);
        return cbdb::Int64ToDatum(i);
      }
      default:
        Assert(!"unexpected typbyval, len not in 1,2,4,8");
        *ok = false;
        break;
    }
    return 0;
  }

  Assert(typlen == -1 || typlen > 0);
  return PointerGetDatum(p);
}

std::string MicroPartitionStats::ToValue(Datum datum, int typlen,
                                         bool typbyval) {
  if (typbyval) {
    Assert(typlen > 0);
    switch (typlen) {
      case 1: {
        int8 i = cbdb::DatumToInt8(datum);
        return std::string(reinterpret_cast<char *>(&i), sizeof(i));
      }
      case 2: {
        int16 i = cbdb::DatumToInt16(datum);
        return std::string(reinterpret_cast<char *>(&i), sizeof(i));
      }
      case 4: {
        int32 i = cbdb::DatumToInt32(datum);
        return std::string(reinterpret_cast<char *>(&i), sizeof(i));
      }
      case 8: {
        int64 i = cbdb::DatumToInt64(datum);
        return std::string(reinterpret_cast<char *>(&i), sizeof(i));
      }
      default:
        Assert(!"unexpected typbyval, len not in 1,2,4,8");
        break;
    }
    CBDB_RAISE(cbdb::CException::kExTypeLogicError,
               fmt("Invalid typlen %d", typlen));
  }

  if (typlen == -1) {
    void *v;
    int len;

    v = cbdb::PointerAndLenFromDatum(datum, &len);
    Assert(v && len != -1);
    return std::string(reinterpret_cast<char *>(v), len);
  }
  // byref but fixed size
  Assert(typlen > 0);
  return std::string(reinterpret_cast<char *>(cbdb::DatumToPointer(datum)),
                     typlen);
}

MicroPartitionStatsData::MicroPartitionStatsData(int natts) : info_() {
  Assert(natts >= 0);

  for (int i = 0; i < natts; i++) {
    auto col pg_attribute_unused() = info_.add_columnstats();
    Assert(col->allnull() && !col->hasnull());
  }
}

void MicroPartitionStatsData::Reset() {
  auto n = info_.columnstats_size();

  info_.mutable_columnstats()->Clear();
  for (int i = 0; i < n; i++) {
    auto col pg_attribute_unused() = info_.add_columnstats();
    Assert(col->allnull() && !col->hasnull());
  }
}

void MicroPartitionStatsData::CopyFrom(MicroPartitionStatsData *stats) {
  Assert(stats);
  Assert(typeid(this) == typeid(stats));

  info_.CopyFrom(stats->info_);
}

::pax::stats::ColumnBasicInfo *MicroPartitionStatsData::GetColumnBasicInfo(
    int column_index) {
  return info_.mutable_columnstats(column_index)->mutable_info();
}
::pax::stats::ColumnDataStats *MicroPartitionStatsData::GetColumnDataStats(
    int column_index) {
  return info_.mutable_columnstats(column_index)->mutable_datastats();
}
int MicroPartitionStatsData::ColumnSize() const {
  return info_.columnstats_size();
}
void MicroPartitionStatsData::SetAllNull(int column_index, bool allnull) {
  info_.mutable_columnstats(column_index)->set_allnull(allnull);
}
void MicroPartitionStatsData::SetHasNull(int column_index, bool hasnull) {
  info_.mutable_columnstats(column_index)->set_hasnull(hasnull);
}

bool MicroPartitionStatsData::GetAllNull(int column_index) {
  return info_.columnstats(column_index).allnull();
}

bool MicroPartitionStatsData::GetHasNull(int column_index) {
  return info_.columnstats(column_index).hasnull();
}

MicroPartitionStatsProvider::MicroPartitionStatsProvider(
    const ::pax::stats::MicroPartitionStatisticsInfo &stats)
    : stats_(stats) {}
int MicroPartitionStatsProvider::ColumnSize() const {
  return stats_.columnstats_size();
}
bool MicroPartitionStatsProvider::AllNull(int column_index) const {
  return stats_.columnstats(column_index).allnull();
}
bool MicroPartitionStatsProvider::HasNull(int column_index) const {
  return stats_.columnstats(column_index).hasnull();
}
const ::pax::stats::ColumnBasicInfo &MicroPartitionStatsProvider::ColumnInfo(
    int column_index) const {
  return stats_.columnstats(column_index).info();
}
const ::pax::stats::ColumnDataStats &MicroPartitionStatsProvider::DataStats(
    int column_index) const {
  return stats_.columnstats(column_index).datastats();
}
}  // namespace pax

static inline const char *BoolToString(bool b) { return b ? "true" : "false"; }

static char *TypeValueToCString(Oid typid, Oid collation,
                                const std::string &value) {
  FmgrInfo finfo;
  HeapTuple tuple;
  Form_pg_type form;
  Datum datum;
  bool ok;

  tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
  if (!HeapTupleIsValid(tuple))
    elog(ERROR, "cache lookup failed for type %u", typid);

  form = (Form_pg_type)GETSTRUCT(tuple);
  Assert(OidIsValid(form->typoutput));

  datum = pax::MicroPartitionStats::FromValue(value, form->typlen,
                                              form->typbyval, &ok);
  if (!ok) elog(ERROR, "unexpected typlen: %d\n", form->typlen);

  fmgr_info_cxt(form->typoutput, &finfo, CurrentMemoryContext);
  datum = FunctionCall1Coll(&finfo, collation, datum);
  ReleaseSysCache(tuple);

  return DatumGetCString(datum);
}

// define stat type for custom output
extern "C" {
extern Datum MicroPartitionStatsInput(PG_FUNCTION_ARGS);
extern Datum MicroPartitionStatsOutput(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(MicroPartitionStatsInput);
PG_FUNCTION_INFO_V1(MicroPartitionStatsOutput);
}

Datum MicroPartitionStatsInput(PG_FUNCTION_ARGS) {
  ereport(ERROR, (errmsg("unsupport MicroPartitionStatsInput")));
  (void)fcinfo;
  PG_RETURN_POINTER(NULL);
}

Datum MicroPartitionStatsOutput(PG_FUNCTION_ARGS) {
  struct varlena *v = PG_GETARG_VARLENA_PP(0);
  pax::stats::MicroPartitionStatisticsInfo stats;
  StringInfoData str;

  bool ok = stats.ParseFromArray(VARDATA_ANY(v), VARSIZE_ANY_EXHDR(v));
  if (!ok) ereport(ERROR, (errmsg("micropartition stats is corrupt")));

  initStringInfo(&str);
  for (int i = 0, n = stats.columnstats_size(); i < n; i++) {
    const auto &column = stats.columnstats(i);

    if (i > 0) appendStringInfoChar(&str, ',');

    appendStringInfo(&str, "[(%s,%s)", BoolToString(column.allnull()),
                     BoolToString(column.hasnull()));

    if (!column.has_datastats()) {
      appendStringInfoString(&str, ",None]");
      continue;
    }

    const auto &data_stats = column.datastats();
    Assert(data_stats.has_minimal() == data_stats.has_maximum());
    if (!data_stats.has_minimal()) {
      appendStringInfoString(&str, ",None]");
      continue;
    }

    const auto &info = column.info();
    appendStringInfo(&str, ",(%s,%s)]",
                     TypeValueToCString(info.typid(), info.collation(),
                                        data_stats.minimal()),
                     TypeValueToCString(info.typid(), info.collation(),
                                        data_stats.maximum()));
  }

  PG_RETURN_CSTRING(str.data);
}
