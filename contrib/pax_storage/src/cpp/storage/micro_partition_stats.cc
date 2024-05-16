#include "storage/micro_partition_stats.h"

#include "comm/cbdb_api.h"

#include "comm/cbdb_wrappers.h"
#include "comm/pax_memory.h"
#include "storage/micro_partition_metadata.h"
#include "storage/proto/proto_wrappers.h"

namespace pax {

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
        CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);
        auto right_min_datum = MicroPartitionStats::FromValue(
            right_column_data_stats->minimal(), typlen, typbyval, &ok);
        CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);
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
        CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);
        auto right_max_datum = MicroPartitionStats::FromValue(
            right_column_data_stats->maximum(), typlen, typbyval, &ok);
        CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);
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

MicroPartitionStats::MicroPartitionStats(bool allow_fallback_to_pg)
    : allow_fallback_to_pg_(allow_fallback_to_pg) {}

MicroPartitionStats::~MicroPartitionStats() { PAX_DELETE(stats_); }
// SetStatsMessage may be called several times in a write,
// one for each micro partition, so all members need to reset.
// Some metainfo like typid, collation, oids for less/greater,
// fmgr should be exactly consistent.
MicroPartitionStats *MicroPartitionStats::SetStatsMessage(
    MicroPartitionStatsData *stats, int natts) {
  FmgrInfo finfo;

  Assert(natts >= 0);
  Assert(stats);
  initial_check_ = false;
  PAX_DELETE(stats_);
  stats_ = stats;

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
  }
  Assert(stats_->ColumnSize() == natts);
  return this;
}

MicroPartitionStats *MicroPartitionStats::SetMinMaxColumnIndex(
    std::vector<int> &&minmax_columns) {
  minmax_columns_ = std::move(minmax_columns);
  return this;
}

MicroPartitionStats *MicroPartitionStats::LightReset() {
  for (char &status : status_) {
    Assert(status == 'n' || status == 'y' || status == 'x');
    if (status == 'y') status = 'n';
  }
  return this;
}

void MicroPartitionStats::AddRow(TupleTableSlot *slot, TupleDesc desc) {
  auto n = desc->natts;

  DoInitialCheck(desc);
  CBDB_CHECK(status_.size() == static_cast<size_t>(n),
             cbdb::CException::ExType::kExTypeSchemaNotMatch);
  for (auto i = 0; i < n; i++) {
    AssertImply(desc->attrs[i].attisdropped, slot->tts_isnull[i]);
    if (slot->tts_isnull[i])
      AddNullColumn(i);
    else
      AddNonNullColumn(i, slot->tts_values[i], desc);
  }
}

void MicroPartitionStats::MergeTo(MicroPartitionStats *stats, TupleDesc desc) {
  Assert(status_.size() == stats->status_.size());
  for (size_t column_index = 0; column_index < status_.size(); column_index++) {
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
      auto col_data_stats_merge =
          stats->stats_->GetColumnDataStats(column_index);

      auto col_basic_stats pg_attribute_unused() =
          stats_->GetColumnBasicInfo(column_index);

      auto att = TupleDescAttr(desc, column_index);
      auto collation = att->attcollation;
      auto typlen = att->attlen;
      auto typbyval = att->attbyval;

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

      bool ok = false;
      auto min_val =
          FromValue(col_data_stats_merge->minimal(), typlen, typbyval, &ok);
      CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);
      auto max_val =
          FromValue(col_data_stats_merge->maximum(), typlen, typbyval, &ok);
      CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);

      UpdateMinMaxValue(column_index, min_val, collation, typlen, typbyval);
      UpdateMinMaxValue(column_index, max_val, collation, typlen, typbyval);
    } else if (status_[column_index] == 'n') {
      stats_->CopyFrom(stats->stats_);
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
                                           TupleDesc desc) {
  Assert(column_index >= 0);
  Assert(column_index < static_cast<int>(opfamilies_.size()));

  auto att = TupleDescAttr(desc, column_index);
  auto collation = att->attcollation;
  auto typlen = att->attlen;
  auto typbyval = att->attbyval;
  auto info = stats_->GetColumnBasicInfo(column_index);
  auto data_stats = stats_->GetColumnDataStats(column_index);
  stats_->SetAllNull(column_index, false);

  // update min/max
  switch (status_[column_index]) {
    case 'x':
      break;
    case 'y':
      Assert(data_stats->has_minimal());
      Assert(data_stats->has_maximum());
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
      Assert(!data_stats->has_minimal());
      Assert(!data_stats->has_maximum());

      info->set_typid(att->atttypid);
      info->set_collation(collation);
      info->set_opfamily(opfamilies_[column_index]);
      data_stats->set_minimal(ToValue(value, typlen, typbyval));
      data_stats->set_maximum(ToValue(value, typlen, typbyval));
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
  auto data_stats = stats_->GetColumnDataStats(column_index);
  const auto &min = data_stats->minimal();
  const auto &max = data_stats->maximum();
  Datum min_datum, max_datum;
  bool ok, umin = false, umax = false;

  Assert(initial_check_);
  Assert(column_index >= 0 &&
         static_cast<size_t>(column_index) < status_.size());
  Assert(status_[column_index] == 'y');

  min_datum = FromValue(min, typlen, typbyval, &ok);
  CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);

  max_datum = FromValue(max, typlen, typbyval, &ok);
  CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);

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

  if (umin) data_stats->set_minimal(ToValue(datum, typlen, typbyval));
  if (umax) data_stats->set_maximum(ToValue(datum, typlen, typbyval));
}

void MicroPartitionStats::DoInitialCheck(TupleDesc desc) {
  auto natts = desc->natts;
  auto num_minmax_columns = static_cast<int>(minmax_columns_.size());

  Assert(natts == static_cast<int>(status_.size()));
  Assert(status_.size() == opfamilies_.size());
  Assert(status_.size() == finfos_.size());
  Assert(num_minmax_columns <= natts);

  if (initial_check_) {
    return;
  }

#ifdef USE_ASSERT_CHECKING
  // minmax_columns_ should be sorted
  for (int i = 1; i < num_minmax_columns; i++)
    Assert(minmax_columns_[i - 1] < minmax_columns_[i]);
#endif

  for (int i = 0, j = 0; i < natts; i++) {
    auto att = TupleDescAttr(desc, i);

    if (att->attisdropped) {
      status_[i] = 'x';
      continue;
    }
    while (j < num_minmax_columns && minmax_columns_[j] < i) j++;
    if (j >= num_minmax_columns || minmax_columns_[j] != i) {
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
  initial_check_ = true;
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
    CBDB_RAISE(cbdb::CException::kExTypeLogicError);
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

MicroPartittionFileStatsData::MicroPartittionFileStatsData(
    ::pax::stats::MicroPartitionStatisticsInfo *info, int natts)
    : info_(info) {
  Assert(info);
  Assert(info->columnstats_size() == 0);
  for (int i = 0; i < natts; i++) {
    auto col pg_attribute_unused() = info->add_columnstats();
    Assert(col->allnull() && !col->hasnull());
  }
  Assert(info->columnstats_size() == natts);
}

void MicroPartittionFileStatsData::CopyFrom(MicroPartitionStatsData *stats) {
  Assert(stats);
  if (typeid(this) == typeid(stats)) {
    auto file_stats = dynamic_cast<MicroPartittionFileStatsData *>(stats);
    Assert(file_stats);
    info_->CopyFrom(*file_stats->info_);
  } else {
    Assert(info_->columnstats_size() == stats->ColumnSize());
    for (int index = 0; index < stats->ColumnSize(); index++) {
      auto column_stats = info_->mutable_columnstats(index);
      column_stats->set_allnull(stats->GetAllNull(index));
      column_stats->set_hasnull(stats->GetHasNull(index));
      column_stats->mutable_datastats()->CopyFrom(
          *stats->GetColumnDataStats(index));
      column_stats->mutable_info()->CopyFrom(*stats->GetColumnBasicInfo(index));
    }
  }
}

::pax::stats::ColumnBasicInfo *MicroPartittionFileStatsData::GetColumnBasicInfo(
    int column_index) {
  return info_->mutable_columnstats(column_index)->mutable_info();
}
::pax::stats::ColumnDataStats *MicroPartittionFileStatsData::GetColumnDataStats(
    int column_index) {
  return info_->mutable_columnstats(column_index)->mutable_datastats();
}
int MicroPartittionFileStatsData::ColumnSize() const {
  return info_->columnstats_size();
}
void MicroPartittionFileStatsData::SetAllNull(int column_index, bool allnull) {
  info_->mutable_columnstats(column_index)->set_allnull(allnull);
}
void MicroPartittionFileStatsData::SetHasNull(int column_index, bool hasnull) {
  info_->mutable_columnstats(column_index)->set_hasnull(hasnull);
}

bool MicroPartittionFileStatsData::GetAllNull(int column_index) {
  return info_->columnstats(column_index).allnull();
}

bool MicroPartittionFileStatsData::GetHasNull(int column_index) {
  return info_->columnstats(column_index).hasnull();
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
