#include "storage/vec/pax_vec_adapter.h"

#ifdef VEC_BUILD

#include "comm/vec_numeric.h"
#include "storage/columns/pax_column_traits.h"
#include "storage/orc/orc_type.h"
#include "storage/toast/pax_toast.h"
#include "storage/vec/pax_vec_comm.h"

namespace pax {

template <typename T>
static std::pair<bool, size_t> ColumnTransMemory(PaxColumn *column) {
  Assert(column->GetStorageFormat() == PaxStorageFormat::kTypeStoragePorcVec);

  auto vec_column = dynamic_cast<T *>(column);
  auto data_buffer = vec_column->GetDataBuffer();
  auto data_cap = data_buffer->Capacity();

  if (data_buffer->IsMemTakeOver() && data_cap != 0) {
    Assert(data_cap % MEMORY_ALIGN_SIZE == 0);

    data_buffer->SetMemTakeOver(false);
    return {true, data_cap};
  }

  return {false, 0};
}

std::pair<size_t, size_t> VecAdapter::AppendPorcVecFormat(PaxColumns *columns) {
  PaxColumn *column;

  size_t total_rows = columns->GetRows();
  auto null_align_bytes =
      TYPEALIGN(MEMORY_ALIGN_SIZE, BITS_TO_BYTES(total_rows));

  for (size_t index = 0; index < columns->GetColumns(); index++) {
    if ((*columns)[index] == nullptr) {
      continue;
    }

    DataBuffer<char> *vec_buffer = &(vec_cache_buffer_[index].vec_buffer);
    DataBuffer<char> *null_bits_buffer =
        &(vec_cache_buffer_[index].null_bits_buffer);
    DataBuffer<int32> *offset_buffer =
        &(vec_cache_buffer_[index].offset_buffer);

    column = (*columns)[index];
    Assert(index < (size_t)vec_cache_buffer_lens_ && vec_cache_buffer_);

    char *buffer = nullptr;
    size_t buffer_len = 0;
    bool trans_succ = false;
    size_t cap_len = 0;

    vec_cache_buffer_[index].null_counts =
        total_rows - column->GetNonNullRows();

    switch (column->GetPaxColumnTypeInMem()) {
      case PaxColumnTypeInMem::kTypeVecBpChar:
      case PaxColumnTypeInMem::kTypeNonFixed: {
        Assert(!vec_buffer->GetBuffer());
        Assert(!offset_buffer->GetBuffer());

        std::tie(buffer, buffer_len) = column->GetBuffer();
        std::tie(trans_succ, cap_len) =
            ColumnTransMemory<PaxVecNonFixedColumn>(column);

        if (trans_succ) {
          vec_buffer->Set(buffer, cap_len);
          vec_buffer->BrushAll();
        } else {
          vec_buffer->Set(BlockBuffer::Alloc<char *>(
                              TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len)),
                          TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len));
          vec_buffer->Write(buffer, buffer_len);
          vec_buffer->BrushAll();
        }

        std::tie(buffer, buffer_len) =
            dynamic_cast<PaxVecNonFixedColumn *>(column)->GetOffsetBuffer(
                false);
        // TODO(jiaqizho): this buffer can also be transferred
        offset_buffer->Set(BlockBuffer::Alloc<char *>(
                               TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len)),
                           TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len));
        offset_buffer->Write((int *)buffer, buffer_len);
        offset_buffer->BrushAll();
        break;
      }
      case PaxColumnTypeInMem::kTypeVecDecimal: {
        std::tie(buffer, buffer_len) = column->GetBuffer();
        std::tie(trans_succ, cap_len) =
            ColumnTransMemory<PaxShortNumericColumn>(column);

        if (trans_succ) {
          vec_buffer->Set(buffer, cap_len);
          vec_buffer->BrushAll();
        } else {
          vec_buffer->Set(
              (char *)cbdb::Palloc0(TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len)),
              TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len));
          vec_buffer->Write(buffer, buffer_len);
          vec_buffer->BrushAll();
        }
        break;
      }
      case PaxColumnTypeInMem::kTypeVecBitPacked:
      case PaxColumnTypeInMem::kTypeFixed: {
        Assert(!vec_buffer->GetBuffer());
        std::tie(buffer, buffer_len) = column->GetBuffer();

        switch (column->GetTypeLength()) {
          case 1:
            std::tie(trans_succ, cap_len) =
                ColumnTransMemory<PaxVecCommColumn<int8>>(column);
            break;
          case 2:
            std::tie(trans_succ, cap_len) =
                ColumnTransMemory<PaxVecCommColumn<int16>>(column);
            break;
          case 4:
            std::tie(trans_succ, cap_len) =
                ColumnTransMemory<PaxVecCommColumn<int32>>(column);
            break;
          case 8:
            std::tie(trans_succ, cap_len) =
                ColumnTransMemory<PaxVecCommColumn<int64>>(column);
            break;
          default:
            Assert(false);
        }

        if (trans_succ) {
          vec_buffer->Set(buffer, cap_len);
          vec_buffer->BrushAll();
        } else {
          vec_buffer->Set(BlockBuffer::Alloc<char *>(
                              TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len)),
                          TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len));
          vec_buffer->Write(buffer, buffer_len);
          vec_buffer->BrushAll();
        }
        break;
      }
      default: {
        CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
      }
    }

    if (column->HasNull()) {
      Bitmap8 *bitmap = nullptr;
      Assert(!null_bits_buffer->GetBuffer());
      null_bits_buffer->Set(BlockBuffer::Alloc0<char *>(null_align_bytes),
                            null_align_bytes);
      bitmap = column->GetBitmap();
      Assert(bitmap);

      CopyBitmap(bitmap, 0, total_rows, null_bits_buffer);
    }
  }

  return std::make_pair(total_rows, total_rows);
}

}  // namespace pax

#endif  // VEC_BUILD
