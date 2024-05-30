#include "storage/orc/orc_group.h"

#include "comm/pax_memory.h"
#include "storage/toast/pax_toast.h"

namespace pax {

OrcGroup::OrcGroup(PaxColumns *pax_column, size_t row_offset,
                   const std::vector<int> *proj_col_index,
                   Bitmap8 *micro_partition_visibility_bitmap)
    : pax_columns_(pax_column),
      micro_partition_visibility_bitmap_(micro_partition_visibility_bitmap),
      row_offset_(row_offset),
      current_row_index_(0),
      proj_col_index_(proj_col_index) {
  Assert(pax_columns_);

  current_nulls_ = PAX_NEW_ARRAY<uint32>(pax_columns_->GetColumns());
  memset(current_nulls_, 0, pax_columns_->GetColumns() * sizeof(uint32));
}

OrcGroup::~OrcGroup() {
  PAX_DELETE(pax_columns_);
  PAX_DELETE_ARRAY(current_nulls_);
  for (auto buffer : buffer_holder_) {
    auto ref = DatumGetPointer(buffer);
    PAX_DELETE(ref);
  }
}

size_t OrcGroup::GetRows() const { return pax_columns_->GetRows(); }

size_t OrcGroup::GetRowOffset() const { return row_offset_; }

PaxColumns *OrcGroup::GetAllColumns() const { return pax_columns_; }

std::pair<bool, size_t> OrcGroup::ReadTuple(TupleTableSlot *slot) {
  int index = 0;
  int nattrs = 0;
  int column_nums = 0;

  Assert(pax_columns_);
  Assert(slot);

  // already consumed
  if (current_row_index_ >= pax_columns_->GetRows()) {
    return {false, current_row_index_};
  }

  nattrs = slot->tts_tupleDescriptor->natts;
  column_nums = pax_columns_->GetColumns();

  if (micro_partition_visibility_bitmap_) {
    // skip invisible rows in micro partition
    while (micro_partition_visibility_bitmap_->Test(row_offset_ +
                                                    current_row_index_)) {
      for (index = 0; index < column_nums; index++) {
        auto column = ((*pax_columns_)[index]);

        if (!column) {
          continue;
        }

        if (column->HasNull()) {
          auto bm = column->GetBitmap();
          Assert(bm);
          if (!bm->Test(current_row_index_)) {
            current_nulls_[index]++;
          }
        }
      }
      current_row_index_++;
    }

    if (current_row_index_ >= pax_columns_->GetRows()) {
      return {false, current_row_index_};
    }
  }

  // proj_col_index_ is not empty
  if (proj_col_index_ && !proj_col_index_->empty()) {
    for (size_t i = 0; i < proj_col_index_->size(); i++) {
      // filter with projection
      index = (*proj_col_index_)[i];

      // handle PAX columns number inconsistent with pg catalog nattrs in case
      // data not been inserted yet or read pax file conserved before last add
      // column DDL is done, for these cases it is normal that pg catalog schema
      // is not match with that in PAX file.
      if (index >= column_nums) {
        cbdb::SlotGetMissingAttrs(slot, index, index + 1);
        continue;
      }

      // In case column is droped, then set its value as null without reading
      // data tuples.
      if (unlikely(slot->tts_tupleDescriptor->attrs[index].attisdropped)) {
        slot->tts_isnull[index] = true;
        continue;
      }

      auto column = ((*pax_columns_)[index]);
      Assert(column);

      std::tie(slot->tts_values[index], slot->tts_isnull[index]) =
          GetColumnValue(column, current_row_index_, &(current_nulls_[index]));
    }
  } else {
    for (index = 0; index < nattrs; index++) {
      // Still need filter with old projection
      // If current proj_col_index_ no build or empty
      // It means current tuple only need return CTID
      if (index < column_nums && !((*pax_columns_)[index])) {
        continue;
      }

      // handle PAX columns number inconsistent with pg catalog nattrs in case
      // data not been inserted yet or read pax file conserved before last add
      // column DDL is done, for these cases it is normal that pg catalog schema
      // is not match with that in PAX file.
      if (index >= column_nums) {
        cbdb::SlotGetMissingAttrs(slot, index, nattrs);
        break;
      }

      // In case column is droped, then set its value as null without reading
      // data tuples.
      if (unlikely(slot->tts_tupleDescriptor->attrs[index].attisdropped)) {
        slot->tts_isnull[index] = true;
        continue;
      }

      auto column = ((*pax_columns_)[index]);
      std::tie(slot->tts_values[index], slot->tts_isnull[index]) =
          GetColumnValue(column, current_row_index_, &(current_nulls_[index]));
    }
  }

  current_row_index_++;
  return {true, current_row_index_ - 1};
}

bool OrcGroup::GetTuple(TupleTableSlot *slot, size_t row_index) {
  size_t index = 0;
  size_t nattrs = 0;
  size_t column_nums = 0;

  Assert(pax_columns_);
  Assert(slot);

  if (row_index >= pax_columns_->GetRows()) {
    return false;
  }

  // if tuple has been deleted, return false;
  if (micro_partition_visibility_bitmap_ &&
      micro_partition_visibility_bitmap_->Test(row_offset_ + row_index)) {
    return false;
  }

  nattrs = static_cast<size_t>(slot->tts_tupleDescriptor->natts);
  column_nums = pax_columns_->GetColumns();

  for (index = 0; index < nattrs; index++) {
    // Same logic with `ReadTuple`
    if (index >= column_nums) {
      cbdb::SlotGetMissingAttrs(slot, index, nattrs);
      break;
    }

    auto column = ((*pax_columns_)[index]);

    if (!column) {
      continue;
    }

    if (unlikely(slot->tts_tupleDescriptor->attrs[index].attisdropped)) {
      slot->tts_isnull[index] = true;
      continue;
    }

    // different with `ReadTuple`
    std::tie(slot->tts_values[index], slot->tts_isnull[index]) =
        GetColumnValue(column, row_index);
  }

  return true;
}

// Used in `GetColumnValue`
// After accumulating `null_counts` in `GetColumnValue`
// Then we can direct get Datum when storage type is `orc`
static std::pair<Datum, Datum> GetDatumWithNonNull(PaxColumn *column,
                                                   size_t non_null_offset,
                                                   size_t row_offset) {
  Datum datum = 0, ref = 0;
  char *buffer;
  size_t buffer_len;

  Assert(column);

  std::tie(buffer, buffer_len) = column->GetBuffer(non_null_offset);
  switch (column->GetPaxColumnTypeInMem()) {
    case kTypeBpChar:
    case kTypeDecimal:
    case kTypeNonFixed: {
      datum = PointerGetDatum(buffer);
      if (column->IsToast(row_offset)) {
        auto external_buffer = column->GetExternalToastDataBuffer();
        Datum d = pax_detoast(
            datum, external_buffer ? external_buffer->Start() : nullptr,
            external_buffer ? external_buffer->Used() : 0);
        datum = d;
        ref = datum;
      }
      break;
    }
    case kTypeBitPacked:
    case kTypeFixed: {
      Assert(buffer_len > 0);
      switch (buffer_len) {
        case 1:
          datum = cbdb::Int8ToDatum(*reinterpret_cast<int8 *>(buffer));
          break;
        case 2:
          datum = cbdb::Int16ToDatum(*reinterpret_cast<int16 *>(buffer));
          break;
        case 4:
          datum = cbdb::Int32ToDatum(*reinterpret_cast<int32 *>(buffer));
          break;
        case 8:
          datum = cbdb::Int64ToDatum(*reinterpret_cast<int64 *>(buffer));
          break;
        default:
          Assert(!"should't be here, fixed type len should be 1, 2, 4, 8");
      }
      break;
    }
    case kTypeVecBitPacked:
    case kTypeVecBpChar:
    case kTypeVecDecimal:
    default:
      Assert(!"should't be here, non-implemented column type in memory");
      break;
  }
  return {datum, ref};
}

std::pair<Datum, bool> OrcGroup::GetColumnValue(TupleDesc desc,
                                                size_t column_index,
                                                size_t row_index) {
  Assert(row_index < pax_columns_->GetRows());
  Assert(column_index < static_cast<size_t>(desc->natts));

  auto is_dropped = desc->attrs[column_index].attisdropped;
  if (is_dropped) {
    return {0, true};
  }

  if (column_index < pax_columns_->GetColumns()) {
    auto column = (*pax_columns_)[column_index];

    // dropped column
    if (!column) {
      return {0, true};
    }

    return GetColumnValue(column, row_index);
  }

  AttrMissing *attrmiss = nullptr;
  if (desc->constr) attrmiss = desc->constr->missing;

  if (!attrmiss) {
    // no missing values array at all, so just fill everything in as NULL
    return {0, true};
  } else {
    // fill with default value
    return {attrmiss[column_index].am_value,
            !attrmiss[column_index].am_present};
  }
}

std::pair<Datum, bool> OrcGroup::GetColumnValue(PaxColumn *column,  // NOLINT
                                                size_t row_index,
                                                uint32 *null_counts) {
  Assert(column);
  Assert(row_index < column->GetRows());
  Assert(row_index >= *null_counts);
  Datum rc, ref;

  if (column->HasNull()) {
    auto bm = column->GetBitmap();
    Assert(bm);
    if (!bm->Test(row_index)) {
      *null_counts += 1;
      return {0, true};
    }
  }
  std::tie(rc, ref) =
      GetDatumWithNonNull(column, row_index - *null_counts, row_index);
  if (ref) {
    buffer_holder_.emplace_back(ref);
  }

  return {rc, false};
}

std::pair<Datum, bool> OrcGroup::GetColumnValue(size_t column_index,
                                                size_t row_index) {
  Assert(column_index < pax_columns_->GetColumns());

  auto column = (*pax_columns_)[column_index];
  Assert(column);

  return GetColumnValue(column, row_index);
}

std::pair<Datum, bool> OrcGroup::GetColumnValue(PaxColumn *column,
                                                size_t row_index) {
  Assert(column);
  Assert(row_index < column->GetRows());
  Datum rc, ref;

  if (column->HasNull()) {
    auto bm = column->GetBitmap();
    Assert(bm);
    if (!bm->Test(row_index)) {
      return {0, true};
    }
  }

  std::tie(rc, ref) = GetDatumWithNonNull(
      column, column->GetRangeNonNullRows(0, row_index), row_index);
  if (ref) {
    buffer_holder_.emplace_back(ref);
  }

  return {rc, false};
}

}  // namespace pax
