#include "storage/micro_partition_stats.h"

#include "comm/cbdb_api.h"

#include "comm/cbdb_wrappers.h"
#include "storage/micro_partition_metadata.h"
#include "storage/proto/proto_wrappers.h"

namespace pax {
MicroPartitionStats::~MicroPartitionStats() { delete stats_; }
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
  delete stats_;
  stats_ = stats;

  memset(&finfo, 0, sizeof(finfo));
  opfamilies_.clear();
  finfos_.clear();
  status_.clear();
  for (int i = 0; i < natts; i++) {
    opfamilies_.emplace_back(InvalidOid);
    finfos_.emplace_back(std::pair<FmgrInfo, FmgrInfo>({finfo, finfo}));
    status_.emplace_back('u');
  }
  Assert(stats_->ColumnSize() == natts);
  return this;
}

MicroPartitionStats *MicroPartitionStats::LightReset() {
  for (char &status : status_) {
    Assert(status == 'n' || status == 'y' || status == 'x');
    if (status == 'y') status = 'n';
  }
  return this;
}

void MicroPartitionStats::AddRow(TupleTableSlot *slot) {
  auto desc = slot->tts_tupleDescriptor;
  auto n = desc->natts;

  if (!initial_check_) {
    DoInitialCheck(desc);
    initial_check_ = true;
  }
  CBDB_CHECK(status_.size() == static_cast<size_t>(n),
             cbdb::CException::ExType::kExTypeSchemaNotMatch);
  for (auto i = 0; i < n; i++) {
    auto att = &desc->attrs[i];

    AssertImply(att->attisdropped, slot->tts_isnull[i]);
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
      continue;
    }

    Assert(stats->status_[column_index] == 'y');
    Assert(status_[column_index] != 'x');
    if (status_[column_index] == 'y') {
      auto col_basic_stats_merge =
          stats->stats_->GetColumnBasicInfo(column_index);
      auto col_data_stats_merge =
          stats->stats_->GetColumnDataStats(column_index);

      auto col_basic_stats = stats_->GetColumnBasicInfo(column_index);

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
  Assert(initial_check_);
  Assert(column_index >= 0 &&
         static_cast<size_t>(column_index) < status_.size());
  Assert(status_[column_index] == 'y');

  auto &finfos = finfos_[column_index];
  auto data_stats = stats_->GetColumnDataStats(column_index);
  bool ok;

  {
    const auto &min = data_stats->minimal();
    auto val = FromValue(min, typlen, typbyval, &ok);
    CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);
    auto update = DatumGetBool(
        cbdb::FunctionCall2Coll(&finfos.first, collation, datum, val));
    if (update) data_stats->set_minimal(ToValue(datum, typlen, typbyval));
  }
  {
    const auto &max = data_stats->maximum();
    auto val = FromValue(max, typlen, typbyval, &ok);
    CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);
    auto update = DatumGetBool(
        cbdb::FunctionCall2Coll(&finfos.second, collation, datum, val));
    if (update) data_stats->set_maximum(ToValue(datum, typlen, typbyval));
  }
}

bool MicroPartitionStats::GetStrategyProcinfo(
    Oid typid, Oid subtype, Oid *opfamily,
    std::pair<FmgrInfo, FmgrInfo> &finfos) {
  Oid opfamily1;
  Oid opfamily2;
  auto ok =
      cbdb::MinMaxGetStrategyProcinfo(typid, subtype, &opfamily1, &finfos.first,
                                      BTLessStrategyNumber) &&
      cbdb::MinMaxGetStrategyProcinfo(typid, subtype, &opfamily2,
                                      &finfos.second, BTGreaterStrategyNumber);
  if (ok) {
    Assert(opfamily1 == opfamily2);
    Assert(OidIsValid(opfamily1));
    *opfamily = opfamily1;
  }
  return ok;
}

void MicroPartitionStats::DoInitialCheck(TupleDesc desc) {
  auto natts = desc->natts;

  Assert(natts == static_cast<int>(status_.size()));
  Assert(status_.size() == opfamilies_.size());
  Assert(status_.size() == finfos_.size());

  for (int i = 0; i < natts; i++) {
    auto att = TupleDescAttr(desc, i);
    if (att->attisdropped ||
        !GetStrategyProcinfo(att->atttypid, att->atttypid, &opfamilies_[i],
                             finfos_[i])) {
      status_[i] = 'x';
      continue;
    }
    status_[i] = 'n';
  }
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
    auto col = info->add_columnstats();
    Assert(col->allnull() && !col->hasnull());
  }
  Assert(info->columnstats_size() == natts);
}

void MicroPartittionFileStatsData::CopyFrom(MicroPartitionStatsData *stats) {
  Assert(stats);
  auto file_stats = dynamic_cast<MicroPartittionFileStatsData *>(stats);
  Assert(file_stats);
  info_->CopyFrom(*file_stats->info_);
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
    const auto &info = column.info();
    appendStringInfo(&str, ",(%s,%s)]",
                     TypeValueToCString(info.typid(), info.collation(),
                                        data_stats.minimal()),
                     TypeValueToCString(info.typid(), info.collation(),
                                        data_stats.maximum()));
  }

  PG_RETURN_CSTRING(str.data);
}