#include "storage/orc/orc_group.h"

namespace pax {

OrcVecGroup::OrcVecGroup(PaxColumns *pax_column, size_t row_offset)
    : OrcGroup(pax_column, row_offset) {
  Assert(pax_column);
  Assert(COLUMN_STORAGE_FORMAT_IS_VEC(pax_column));
}

OrcVecGroup::~OrcVecGroup() {
  for (auto buffer : buffer_holder_) {
    cbdb::Pfree(buffer);
  }
}

static std::pair<Datum, struct varlena *> GetDatumWithNonNull(PaxColumn *column,
                                                              size_t row_index);

std::pair<Datum, bool> OrcVecGroup::GetColumnValue(size_t column_index,
                                                   size_t row_index) {
  Assert(column_index < pax_columns_->GetColumns());

  auto column = (*pax_columns_)[column_index];
  Assert(column);

  return GetColumnValue(column, row_index);
}

std::pair<Datum, bool> OrcVecGroup::GetColumnValue(PaxColumn *column,
                                                   size_t row_index) {
  Assert(column);
  Assert(row_index < column->GetRows());
  Datum datum;
  struct varlena *buffer_ref = nullptr;

  if (column->HasNull()) {
    auto bm = column->GetBitmap();
    Assert(bm);
    if (!bm->Test(row_index)) {
      return {0, true};
    }
  }

  std::tie(datum, buffer_ref) = GetDatumWithNonNull(column, row_index);
  if (buffer_ref) buffer_holder_.emplace_back(buffer_ref);

  return {datum, false};
}

static std::pair<Datum, struct varlena *> GetDatumWithNonNull(
    PaxColumn *column, size_t row_index) {
  Assert(column);
  Datum datum = 0;
  char *buffer;
  size_t buffer_len;
  struct varlena *result_ref = nullptr;

  std::tie(buffer, buffer_len) = column->GetBuffer(row_index);
  switch (column->GetPaxColumnTypeInMem()) {
    case kTypeNonFixed:
      CBDB_WRAP_START;
      {
        result_ref = reinterpret_cast<struct varlena *>(
            palloc(TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len + VARHDRSZ)));
        SET_VARSIZE(result_ref, buffer_len + VARHDRSZ);
        memcpy(VARDATA(result_ref), buffer, buffer_len);
        datum = PointerGetDatum(result_ref);
      }
      CBDB_WRAP_END;
      break;
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
    case kTypeDecimal: {
      datum = PointerGetDatum(buffer);
      break;
    }
    default:
      Assert(!"should't be here, non-implemented column type in memory");
      break;
  }

  return {datum, result_ref};
}

std::pair<Datum, bool> OrcVecGroup::GetColumnValue(PaxColumn *column,
                                                   size_t row_index,
                                                   uint32 * /*null_counts*/) {
  if (column->HasNull()) {
    auto bm = column->GetBitmap();
    Assert(bm);
    if (!bm->Test(row_index)) {
      return {0, true};
    }
  }
  Datum datum;
  struct varlena *buffer_ref = nullptr;
  std::tie(datum, buffer_ref) = GetDatumWithNonNull(column, row_index);
  if (buffer_ref) buffer_holder_.emplace_back(buffer_ref);

  return {datum, false};
}

}  // namespace pax