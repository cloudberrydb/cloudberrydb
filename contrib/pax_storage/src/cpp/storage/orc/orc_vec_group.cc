#include "storage/orc/orc_group.h"
#include "storage/toast/pax_toast.h"
namespace pax {

OrcVecGroup::OrcVecGroup(PaxColumns *pax_column, size_t row_offset,
                         const std::vector<int> *proj_col_index)
    : OrcGroup(pax_column, row_offset, proj_col_index) {
  Assert(pax_column);
  Assert(COLUMN_STORAGE_FORMAT_IS_VEC(pax_column));
}

static std::pair<Datum, Datum> GetDatumWithNonNull(PaxColumn *column,
                                                   size_t row_index) {
  Datum datum = 0, ref = 0;
  char *buffer;
  size_t buffer_len;

  Assert(column);

  std::tie(buffer, buffer_len) = column->GetBuffer(row_index);
  switch (column->GetPaxColumnTypeInMem()) {
    case kTypeVecBpChar:
    case kTypeNonFixed:
      datum = PointerGetDatum(buffer);
      if (column->IsToast(row_index)) {
        auto external_buffer = column->GetExternalToastDataBuffer();
        ref = pax_detoast(datum,
                          external_buffer ? external_buffer->Start() : nullptr,
                          external_buffer ? external_buffer->Used() : 0);
        datum = ref;
        break;
      }

      CBDB_WRAP_START;
      {
        ref = PointerGetDatum(PAX_NEW_ARRAY<char>(
            TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len + VARHDRSZ)));
        SET_VARSIZE(ref, buffer_len + VARHDRSZ);
        memcpy(VARDATA(ref), buffer, buffer_len);
        datum = ref;
      }
      CBDB_WRAP_END;
      break;
    case kTypeVecBitPacked:
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
    case kTypeVecDecimal:
    case kTypeVecNoHeader: {
      datum = PointerGetDatum(buffer);
      break;
    }
    case kTypeBitPacked:
    case kTypeBpChar:
    case kTypeDecimal:
    default:
      Assert(!"should't be here, non-implemented column type in memory");
      break;
  }

  return {datum, ref};
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
  Datum datum, buffer_ref;

  std::tie(datum, buffer_ref) = GetDatumWithNonNull(column, row_index);
  if (buffer_ref) buffer_holder_.emplace_back(buffer_ref);

  return {datum, false};
}

}  // namespace pax