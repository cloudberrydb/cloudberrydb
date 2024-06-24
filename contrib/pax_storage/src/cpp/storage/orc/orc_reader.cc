#include "comm/cbdb_api.h"

#include <string>
#include <utility>
#include <vector>

#include "comm/cbdb_wrappers.h"
#include "exceptions/CException.h"
#include "storage/orc/orc_defined.h"
#include "storage/orc/orc_group.h"
#include "storage/orc/porc.h"
#include "storage/pax_filter.h"
#include "storage/pax_itemptr.h"

namespace pax {

class OrcGroupStatsProvider final : public ColumnStatsProvider {
 public:
  OrcGroupStatsProvider(const OrcFormatReader &format_reader,
                        size_t group_index)
      : format_reader_(format_reader), group_index_(group_index) {
    Assert(group_index < format_reader.GetStripeNums());
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

OrcReader::OrcReader(File *file, File *toast_file)
    : working_group_(nullptr),
      cached_group_(nullptr),
      current_group_index_(0),
      proj_map_(nullptr),
      proj_len_(0),
      proj_column_index_(nullptr),
      format_reader_(file, toast_file),
      is_closed_(true) {}

std::unique_ptr<ColumnStatsProvider> OrcReader::GetGroupStatsInfo(
    size_t group_index) {
  auto x = PAX_NEW<OrcGroupStatsProvider>(format_reader_, group_index);
  return std::unique_ptr<ColumnStatsProvider>(x);
}

MicroPartitionReader::Group *OrcReader::ReadGroup(size_t group_index) {
  PaxColumns *pax_columns = nullptr;

  Assert(group_index < GetGroupNums());

  pax_columns = format_reader_.ReadStripe(group_index, proj_map_, proj_len_);

#ifdef ENABLE_DEBUG
  for (size_t i = 0; i < pax_columns->GetColumns(); i++) {
    auto column = (*pax_columns)[i];
    if (column && !column->GetBuffer().first) {
      auto bm = column->GetBitmap();
      // Assert(bm);
      if (bm) {
        for (size_t n = 0; n < column->GetRows(); n++) {
          Assert(!bm->Test(n));
        }
      }
    }
  }
#endif  // ENABLE_DEBUG

  MicroPartitionReader::Group *group;
  size_t group_offset = format_reader_.GetStripeOffset(group_index);
  if (COLUMN_STORAGE_FORMAT_IS_VEC(pax_columns))
    group = PAX_NEW<OrcVecGroup>(pax_columns, group_offset, proj_column_index_);
  else
    group = PAX_NEW<OrcGroup>(pax_columns, group_offset, proj_column_index_);

  group->SetVisibilityMap(visibility_bitmap_);
  return group;
}

size_t OrcReader::GetGroupNums() { return format_reader_.GetStripeNums(); }

void OrcReader::Open(const ReaderOptions &options) {
  // Must not open twice.
  Assert(is_closed_);
  if (options.reused_buffer) {
    CBDB_CHECK(options.reused_buffer->IsMemTakeOver(),
               cbdb::CException::ExType::kExTypeLogicError,
               "Invalid memory owner in resued buffer");
    options.reused_buffer->BrushBackAll();
    format_reader_.SetReusedBuffer(options.reused_buffer);
  }

  if (options.filter) {
    std::tie(proj_map_, proj_len_) = options.filter->GetColumnProjection();
    SetProjColumnIndex(options.filter->GetColumnProjectionIndex());
  }

  format_reader_.Open();
  is_closed_ = false;

  // only referenced, owner by caller who constructed ReadOptions
  visibility_bitmap_ = options.visibility_bitmap;
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

  ResetCurrentReading();
  format_reader_.Close();
  is_closed_ = true;
}

bool OrcReader::ReadTuple(TupleTableSlot *slot) {
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
               cbdb::CException::ExType::kExTypeSchemaNotMatch,
               fmt("There are more number of column in the current file than "
                   "in TupleDesc. [in file=%lu, in desc=%d]",
                   columns->GetColumns(), slot->tts_tupleDescriptor->natts));
  }

  bool ok = false;
  size_t group_row_offset = 0;

  std::tie(ok, group_row_offset) = working_group_->ReadTuple(slot);
  if (!ok) {
    PAX_DELETE(working_group_);
    working_group_ = nullptr;
    goto retry_read_group;
  }

  SetTupleOffset(&slot->tts_tid,
                 working_group_->GetRowOffset() + group_row_offset);
  return true;
}

bool OrcReader::GetTuple(TupleTableSlot *slot, size_t row_index) {
  int32 group_index = -1;
  size_t nums_of_group;
  int left, right;

  size_t group_offset, number_of_rows;

  nums_of_group = GetGroupNums();
  left = 0;
  right = nums_of_group - 1;

  // current `row_index` in group
  if (cached_group_ && cached_group_->GetRowOffset() <= row_index &&
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
  SetTupleOffset(&slot->tts_tid, row_index);
  return ok;
}

}  // namespace pax
