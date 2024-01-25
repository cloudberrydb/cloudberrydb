#include "comm/cbdb_api.h"

#include <string>
#include <utility>
#include <vector>

#include "comm/cbdb_wrappers.h"
#include "exceptions/CException.h"
#include "storage/columns/pax_column_cache.h"
#include "storage/orc/orc.h"
#include "storage/orc/orc_defined.h"
#include "storage/orc/orc_group.h"
#include "storage/pax_filter.h"

namespace pax {

class OrcGroupStatsProvider final : public ColumnStatsProvider {
 public:
  OrcGroupStatsProvider(const OrcFormatReader &format_reader,
                        size_t group_index)
      : format_reader_(format_reader), group_index_(group_index) {
    Assert(group_index >= 0 && group_index < format_reader.GetStripeNums());
  }
  int ColumnSize() const override {
    return static_cast<int>(format_reader_.file_footer_.colinfo_size());
  }
  bool AllNull(int column_index) const override {
    return format_reader_.file_footer_.stripes(group_index_)
        .colstats(column_index)
        .allnull();
  }
  bool HasNull(int column_index) const override {
    return format_reader_.file_footer_.stripes(group_index_)
        .colstats(column_index)
        .hasnull();
  }
  const ::pax::stats::ColumnBasicInfo &ColumnInfo(
      int column_index) const override {
    return format_reader_.file_footer_.colinfo(column_index);
  }
  const ::pax::stats::ColumnDataStats &DataStats(
      int column_index) const override {
    return format_reader_.file_footer_.stripes(group_index_)
        .colstats(column_index)
        .coldatastats();
  }

 private:
  const OrcFormatReader &format_reader_;
  size_t group_index_;
};

OrcReader::OrcReader(File *file)
    : working_group_(nullptr),
      cached_group_(nullptr),
      current_group_index_(0),
      proj_map_(nullptr),
      proj_len_(0),
      format_reader_(file),
      is_closed_(true) {}

std::unique_ptr<ColumnStatsProvider> OrcReader::GetGroupStatsInfo(
    size_t group_index) {
  auto x = PAX_NEW<OrcGroupStatsProvider>(format_reader_, group_index);
  return std::unique_ptr<ColumnStatsProvider>(x);
}

MicroPartitionReader::Group *OrcReader::ReadGroup(size_t group_index) {
  PaxColumns *pax_columns = nullptr;

  Assert(group_index < GetGroupNums());
#ifdef ENABLE_PLASMA
  if (pax_column_cache_) {
    PaxColumns *columns_readed = nullptr;
    bool *proj_copy = nullptr;
    bool still_remain = false;
    std::tie(pax_columns, release_key_, proj_copy) =
        pax_column_cache_->ReadCache(group_index);

    for (size_t i = 0; i < proj_len_; i++) {
      if (proj_copy[i]) {
        still_remain = true;
        break;
      }
    }

    if (still_remain) {
      columns_readed =
          format_reader_.ReadStripe(group_index, proj_copy, proj_len_);
      pax_column_cache_->WriteCache(columns_readed, group_index);
    }

    PAX_DELETE_ARRAY(proj_copy);

    // No get the cache columns
    if (pax_columns->GetRows() == 0) {
      Assert(columns_readed);
      PAX_DELETE(pax_columns);
      pax_columns = columns_readed;
    } else if (still_remain) {
      Assert(columns_readed);
      pax_columns->MergeTo(columns_readed);
    }

  } else {
    pax_columns = format_reader_.ReadStripe(group_index, proj_map_, proj_len_);
  }
#else  // ENABLE_PLASMA
  pax_columns = format_reader_.ReadStripe(group_index, proj_map_, proj_len_);

#endif  // ENABLE_PLASMA

#ifdef ENABLE_DEBUG
  for (size_t i = 0; i < pax_columns->GetColumns(); i++) {
    auto column = (*pax_columns)[i];
    if (column && !column->GetBuffer().first) {
      auto bm = column->GetBitmap();
      Assert(bm);
      for (size_t n = 0; n < column->GetRows(); n++) {
        Assert(!bm->Test(n));
      }
    }
  }
#endif  // ENABLE_DEBUG

  size_t group_offset = format_reader_.GetStripeOffset(group_index);
  if (COLUMN_STORAGE_FORMAT_IS_VEC(pax_columns))
    return PAX_NEW<OrcVecGroup>(pax_columns, group_offset);
  else
    return PAX_NEW<OrcGroup>(pax_columns, group_offset);
}

size_t OrcReader::GetGroupNums() { return format_reader_.GetStripeNums(); }

void OrcReader::Open(const ReaderOptions &options) {
  // Must not open twice.
  Assert(is_closed_);
  if (options.reused_buffer) {
    CBDB_CHECK(options.reused_buffer->IsMemTakeOver(),
               cbdb::CException::ExType::kExTypeLogicError);
    options.reused_buffer->BrushBackAll();
    format_reader_.SetReusedBuffer(options.reused_buffer);
  }

  if (options.filter) {
    std::tie(proj_map_, proj_len_) = options.filter->GetColumnProjection();
  }

#ifdef ENABLE_PLASMA
  if (options.pax_cache)
    pax_column_cache_ = PAX_NEW<PaxColumnCache>(options.pax_cache, options.block_id,
                                           proj_map_, proj_len_);
#endif
  format_reader_.Open();

  is_closed_ = false;
}

void OrcReader::ResetCurrentReading() {
  PAX_DELETE(working_group_);
  working_group_ = nullptr;

  PAX_DELETE(cached_group_);
  cached_group_ = nullptr;

  current_group_index_ = 0;
}

void OrcReader::Close() {
  if (is_closed_) {
    return;
  }

#ifdef ENABLE_PLASMA
  if (pax_column_cache_) {
    pax_column_cache_->ReleaseCache(release_key_);
    PAX_DELETE(pax_column_cache_);
  }
#endif

  ResetCurrentReading();
  format_reader_.Close();
  is_closed_ = true;
}

bool OrcReader::ReadTuple(CTupleSlot *cslot) {
  TupleTableSlot *slot;

  slot = cslot->GetTupleTableSlot();

retry_read_group:
  if (!working_group_) {
    if (current_group_index_ >= GetGroupNums()) {
      return false;
    }

    working_group_ = ReadGroup(current_group_index_++);
    auto columns = working_group_->GetAllColumns();

    // The column number in Pax file meta could be smaller than the column
    // number in TupleSlot in case after alter table add column DDL operation
    // was done.
    CBDB_CHECK(columns->GetColumns() <=
                   static_cast<size_t>(slot->tts_tupleDescriptor->natts),
               cbdb::CException::ExType::kExTypeSchemaNotMatch);
  }

  bool ok = false;
  size_t group_row_offset = 0;

  std::tie(ok, group_row_offset) = working_group_->ReadTuple(slot);
  if (!ok) {
    PAX_DELETE(working_group_);
    working_group_ = nullptr;
    goto retry_read_group;
  }

  cslot->SetOffset(working_group_->GetRowOffset() + group_row_offset);
  return true;
}

bool OrcReader::GetTuple(CTupleSlot *cslot, size_t row_index) {
  int32 group_index = -1;
  size_t nums_of_group;
  int left, right;

  TupleTableSlot *slot;
  size_t group_offset, number_of_rows;

  nums_of_group = GetGroupNums();
  left = 0;
  right = nums_of_group - 1;

  slot = cslot->GetTupleTableSlot();

  // current `row_index` in group
  if (cached_group_ && cached_group_->GetRowOffset() >= row_index &&
      row_index < (cached_group_->GetRowOffset() + cached_group_->GetRows())) {
    goto found;
  }

  while (left <= right) {
    auto mid = (right - left) / 2 + left;
    group_offset = format_reader_.GetStripeOffset(mid);
    number_of_rows = format_reader_.GetStripeNumberOfRows(mid);

    if (row_index >= group_offset &&
        row_index < (group_offset + number_of_rows)) {
      group_index = mid;
      break;
    } else if (row_index < group_offset) {
      right = mid - 1;
    } else {  // row_index >= (group_offset + number_of_rows)
      left = mid + 1;
    }
  }

  if (group_index == -1) {
    return false;
  }

  // group_offset have been inited in loop
  // and must not in cached_group_
  PAX_DELETE(cached_group_);
  cached_group_ = ReadGroup(group_index);

found:
  auto ok =
      cached_group_->GetTuple(slot, row_index - cached_group_->GetRowOffset());
  Assert(ok);
  cslot->SetOffset(row_index);
  return ok;
}

}  // namespace pax
