#include "storage/micro_partition_row_filter_reader.h"

#include "comm/guc.h"
#include "comm/log.h"
#include "comm/pax_memory.h"
#include "storage/pax_defined.h"
#include "storage/pax_filter.h"
#include "storage/pax_itemptr.h"

namespace pax {
static inline bool TestExecQual(ExprState *estate, ExprContext *econtext) {
  CBDB_WRAP_START;
  { return ExecQual(estate, econtext); }
  CBDB_WRAP_END;
}

MicroPartitionRowFilterReader *MicroPartitionRowFilterReader::New(
    MicroPartitionReader *reader, PaxFilter *filter,
    Bitmap8 *visibility_bitmap) {
  Assert(reader);
  Assert(filter && filter->HasRowScanFilter());

  auto r = PAX_NEW<MicroPartitionRowFilterReader>();
  r->SetReader(reader);
  r->SetVisibilityBitmap(visibility_bitmap);
  r->filter_ = filter;
  return r;
}

MicroPartitionReader::Group *MicroPartitionRowFilterReader::GetNextGroup(
    TupleDesc desc) {
  auto ngroups = reader_->GetGroupNums();
retry_next_group:
  if (group_index_ >= ngroups) return nullptr;
  auto info = reader_->GetGroupStatsInfo(group_index_);
  ++group_index_;
  if (filter_ &&
      !filter_->TestScan(*info, desc, PaxFilterStatisticsKind::kGroup)) {
    goto retry_next_group;
  }
  group_ = reader_->ReadGroup(group_index_ - 1);
  current_group_row_index_ = 0;
  return group_;
}

bool MicroPartitionRowFilterReader::ReadTuple(TupleTableSlot *slot) {
  auto g = group_;
  auto ctx = filter_->GetExecutionFilterContext();
  const auto &remaining_columns = filter_->GetRemainingColumns();
  size_t nrows;
  TupleDesc desc;

  desc = slot->tts_tupleDescriptor;
  slot->tts_nvalid = desc->natts;

retry_next_group:
  if (group_ == nullptr) {
    g = GetNextGroup(desc);
    if (!g) return false;
  }
  nrows = g->GetRows();
retry_next:
  if (current_group_row_index_ >= nrows) {
    PAX_DELETE(group_);
    group_ = nullptr;
    goto retry_next_group;
  }

  if (micro_partition_visibility_bitmap_) {
    while (micro_partition_visibility_bitmap_->Test(current_group_row_index_ +
                                                    g->GetRowOffset())) {
      current_group_row_index_++;
      if (current_group_row_index_ >= nrows) {
        PAX_DELETE(group_);
        group_ = nullptr;
        goto retry_next_group;
      }
    }
  }

  for (int i = 0; i < ctx->size; i++) {
    auto attno = ctx->attnos[i];
    Assert(attno > 0);
    std::tie(slot->tts_values[attno - 1], slot->tts_isnull[attno - 1]) =
        g->GetColumnValue(desc, attno - 1, current_group_row_index_);
    if (!TestRowScanInternal(slot, ctx->estates[i], attno)) {
      current_group_row_index_++;
      goto retry_next;
    }
  }
  for (auto attno : remaining_columns) {
    std::tie(slot->tts_values[attno - 1], slot->tts_isnull[attno - 1]) =
        g->GetColumnValue(desc, attno - 1, current_group_row_index_);
  }
  current_group_row_index_++;
  SetTupleOffset(&slot->tts_tid, g->GetRowOffset() + current_group_row_index_ - 1);
  if (ctx->estate_final && !TestRowScanInternal(slot, ctx->estate_final, 0))
    goto retry_next;

  return true;
}

bool MicroPartitionRowFilterReader::TestRowScanInternal(TupleTableSlot *slot,
                                                        ExprState *estate,
                                                        AttrNumber attno) {
  Assert(filter_);
  Assert(estate);
  Assert(slot);
  Assert(attno >= 0);

  auto ctx = filter_->GetExecutionFilterContext();
  auto econtext = ctx->econtext;
  econtext->ecxt_scantuple = slot;

  return TestExecQual(estate, econtext);
}
}  // namespace pax
