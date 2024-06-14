#include "storage/vec/pax_vec_adapter.h"

#ifdef VEC_BUILD

#include "comm/vec_numeric.h"
#include "storage/columns/pax_column_traits.h"
#include "storage/toast/pax_toast.h"
#include "storage/vec/pax_vec_comm.h"

namespace pax {

void CopyFixedRawBufferWithNull(PaxColumn *column,
                                std::shared_ptr<Bitmap8> visibility_map_bitset,
                                size_t bitset_index_begin, size_t range_begin,
                                size_t range_lens, size_t data_index_begin,
                                size_t data_range_lens,
                                DataBuffer<char> *out_data_buffer) {
  char *buffer;
  size_t buffer_len;

  std::tie(buffer, buffer_len) =
      column->GetRangeBuffer(data_index_begin, data_range_lens);

  auto null_bitmap = column->GetBitmap();
  size_t non_null_offset = 0;
  size_t type_len = column->GetTypeLength();
  for (size_t i = range_begin; i < (range_begin + range_lens); i++) {
    // filted by row_filter or bloom_filter
    if (visibility_map_bitset &&
        visibility_map_bitset->Test(i - range_begin + bitset_index_begin)) {
      if (null_bitmap->Test(i)) {
        non_null_offset += type_len;
      }
      continue;
    }
    if (null_bitmap->Test(i)) {
      out_data_buffer->Write(buffer + non_null_offset, type_len);
      non_null_offset += type_len;
    }
    out_data_buffer->Brush(type_len);
  }
}

static inline void CopyFixedRawBuffer(char *buffer, size_t len,
                                      DataBuffer<char> *data_buffer) {
  data_buffer->Write(buffer, len);
  data_buffer->Brush(len);
}

void CopyFixedBuffer(PaxColumn *column,
                     std::shared_ptr<Bitmap8> visibility_map_bitset,
                     size_t bitset_index_begin, size_t range_begin,
                     size_t range_lens, size_t data_index_begin,
                     size_t data_range_lens,
                     DataBuffer<char> *out_data_buffer) {
  if (column->HasNull()) {
    CopyFixedRawBufferWithNull(
        column, visibility_map_bitset, bitset_index_begin, range_begin,
        range_lens, data_index_begin, data_range_lens, out_data_buffer);
  } else {
    char *buffer;
    size_t buffer_len;
    std::tie(buffer, buffer_len) =
        column->GetRangeBuffer(data_index_begin, data_range_lens);

    if (visibility_map_bitset == nullptr) {
      CopyFixedRawBuffer(buffer, buffer_len, out_data_buffer);
    } else {
      size_t non_null_offset = 0;
      size_t type_len = column->GetTypeLength();
      for (size_t i = range_begin; i < (range_begin + range_lens); i++) {
        if (visibility_map_bitset &&
            visibility_map_bitset->Test(i - range_begin + bitset_index_begin)) {
          non_null_offset += type_len;
          continue;
        }
        out_data_buffer->Write(buffer + non_null_offset, type_len);
        out_data_buffer->Brush(type_len);
        non_null_offset += type_len;
      }
    }
  }
}

void CopyNonFixedBuffer(PaxColumn *column,
                        std::shared_ptr<Bitmap8> visibility_map_bitset,
                        size_t bitset_index_begin, size_t range_begin,
                        size_t range_lens, size_t data_index_begin,
                        size_t data_range_lens,
                        DataBuffer<int32> *offset_buffer,
                        DataBuffer<char> *out_data_buffer, bool is_bpchar) {
  size_t dst_offset = out_data_buffer->Used();
  char *buffer = nullptr;
  size_t buffer_len = 0;

  auto null_bitmap = column->GetBitmap();
  size_t non_null_offset = 0;

  for (size_t i = range_begin; i < (range_begin + range_lens); i++) {
    if (visibility_map_bitset &&
        visibility_map_bitset->Test(i - range_begin + bitset_index_begin)) {
      // tuples that are marked for deletion also need to calculate
      // non-null-offset, which is used to calculate the address of valid
      // data. null_bitmap: 0 represents null, 1 represents non-null
      if (!null_bitmap || null_bitmap->Test(i)) {
        non_null_offset++;
      }
      continue;
    }

    if (null_bitmap && !null_bitmap->Test(i)) {
      offset_buffer->Write(dst_offset);
      offset_buffer->Brush(sizeof(int32));

    } else {
      size_t read_len = 0;
      char *read_data;

      std::tie(buffer, buffer_len) =
          column->GetBuffer(data_index_begin + non_null_offset);

      // deal toast
      if (column->IsToast(i)) {
        auto et_buffer = column->GetExternalToastDataBuffer();

#ifdef USE_ASSERT_CHECKING
        auto toast_raw_size = pax_toast_raw_size(PointerGetDatum(buffer));
        Assert(toast_raw_size <= out_data_buffer->Available());
#endif

        auto decompress_size = pax_detoast_raw(
            PointerGetDatum(buffer), out_data_buffer->GetAvailableBuffer(),
            out_data_buffer->Available(),
            et_buffer ? et_buffer->Start() : nullptr,
            et_buffer ? et_buffer->Used() : 0);
        out_data_buffer->Brush(decompress_size);

        offset_buffer->Write(dst_offset);
        offset_buffer->Brush(sizeof(int32));

        dst_offset += decompress_size;
      } else {
        VarlenaToRawBuffer(buffer, buffer_len, &read_data, &read_len);

        // In vec, bpchar not allow store empty char after the actual characters
        if (is_bpchar) {
          read_len = bpchartruelen(read_data, read_len);
        }

        out_data_buffer->Write(read_data, read_len);
        out_data_buffer->Brush(read_len);

        offset_buffer->Write(dst_offset);
        offset_buffer->Brush(sizeof(int32));

        dst_offset += read_len;
      }

      non_null_offset++;
    }
  }

  offset_buffer->Write(dst_offset);
  offset_buffer->Brush(sizeof(int32));

  AssertImply(visibility_map_bitset == nullptr,
              non_null_offset == data_range_lens);
  AssertImply(visibility_map_bitset, non_null_offset <= data_range_lens);

  // if not the marking deletion，the non_null_offset is equal to the
  // data_range_lens; when there is a Visibility Map, part of the data is
  // invalid. Non_null_offset may be less than the data_range_lens.
  if (visibility_map_bitset == nullptr) {
    CBDB_CHECK(non_null_offset == data_range_lens,
               cbdb::CException::ExType::kExTypeOutOfRange);
  }
}

static void CopyDecimalBuffer(PaxColumn *column,
                              std::shared_ptr<Bitmap8> visibility_map_bitset,
                              size_t bitset_index_begin, size_t range_begin,
                              size_t range_lens, size_t data_index_begin,
                              size_t data_range_lens,
                              DataBuffer<char> *out_data_buffer) {
  size_t non_null_offset = 0;
  char *buffer = nullptr;
  size_t buffer_len = 0;
  int32 type_len;

  auto null_bitmap = column->GetBitmap();
  type_len = VEC_SHORT_NUMERIC_STORE_BYTES;

  for (size_t i = range_begin; i < (range_begin + range_lens); i++) {
    if (visibility_map_bitset &&
        visibility_map_bitset->Test(i - range_begin + bitset_index_begin)) {
      if (!null_bitmap || null_bitmap->Test(i)) {
        non_null_offset++;
      }
      continue;
    }

    if (null_bitmap && !null_bitmap->Test(i)) {
      out_data_buffer->Brush(type_len);
    } else {
      Numeric numeric;
      size_t num_len = 0;
      std::tie(buffer, buffer_len) =
          column->GetBuffer(data_index_begin + non_null_offset);

      auto vl = (struct varlena *)DatumGetPointer(buffer);
      num_len = VARSIZE_ANY_EXHDR(vl);
      numeric = cbdb::DatumToNumeric(PointerGetDatum(buffer));

      char *dest_buff = out_data_buffer->GetAvailableBuffer();
      Assert(out_data_buffer->Available() >= (size_t)type_len);
      pg_short_numeric_to_vec_short_numeric(
          numeric, num_len, (int64 *)dest_buff,
          (int64 *)(dest_buff + sizeof(int64)));
      out_data_buffer->Brush(type_len);
      non_null_offset++;
    }
  }

  AssertImply(visibility_map_bitset == nullptr,
              non_null_offset == data_range_lens);
  AssertImply(visibility_map_bitset, non_null_offset <= data_range_lens);

  if (visibility_map_bitset == nullptr) {
    CBDB_CHECK(non_null_offset == data_range_lens,
               cbdb::CException::ExType::kExTypeOutOfRange);
  }
}

void CopyBitPackedBuffer(PaxColumn *column,
                         std::shared_ptr<Bitmap8> visibility_map_bitset,
                         size_t bitset_index_begin, size_t range_begin,
                         size_t range_lens, size_t data_index_begin,
                         size_t data_range_lens, Bitmap8 *out_data_buffer) {
  char *buffer;
  size_t buffer_len;
  std::tie(buffer, buffer_len) =
      column->GetRangeBuffer(data_index_begin, data_range_lens);

  auto null_bitmap = column->GetBitmap();
  size_t bit_index = 0;
  size_t non_null_offset = 0;
  size_t type_len = column->GetTypeLength();

  for (size_t i = range_begin; i < (range_begin + range_lens); i++) {
    bool is_visible =
        !visibility_map_bitset ||
        !visibility_map_bitset->Test(i - range_begin + bitset_index_begin);
    bool has_null = column->HasNull();
    AssertImply(has_null, null_bitmap != nullptr);
    bool is_null = has_null && !null_bitmap->Test(i);

    if (is_visible) {
      if (!is_null) {
        bool value = *(bool *)(buffer + non_null_offset);
        if (value) {
          out_data_buffer->Set(bit_index);
        }
        bit_index++;
      }
    }

    if (!is_null) {
      non_null_offset += type_len;
    }
  }
}

static size_t CalcRecordBatchDataBufferSize(PaxColumn *column,
                                            size_t range_buffer_len,
                                            size_t num_of_not_nulls) {
  size_t toast_counts;
  size_t raw_data_size;
  Assert(column);

  toast_counts = column->ToastCounts();
  if (toast_counts > 0) {
    char *toast_buff;
    size_t toast_buff_size;
    auto toast_indexes = column->GetToastIndexes();
    int64 toast_total_size = 0;
    for (size_t i = 0; i < toast_indexes->GetSize(); i++) {
      auto toast_index = (*toast_indexes)[i];
      std::tie(toast_buff, toast_buff_size) = column->GetBuffer(
          toast_index - column->GetRangeNonNullRows(0, toast_index));
      toast_total_size += pax_toast_raw_size(PointerGetDatum(toast_buff));
      toast_total_size -= pax_toast_hdr_size(PointerGetDatum(toast_buff));
    }

    raw_data_size = range_buffer_len -
                    ((num_of_not_nulls - toast_counts) * VARHDRSZ_SHORT) +
                    toast_total_size;
  } else {
    raw_data_size = range_buffer_len - (num_of_not_nulls * VARHDRSZ_SHORT);
  }
  return TYPEALIGN(MEMORY_ALIGN_SIZE, raw_data_size);
}

std::pair<size_t, size_t> VecAdapter::AppendPorcFormat(PaxColumns *columns,
                                                       size_t range_begin,
                                                       size_t range_lens) {
  PaxColumn *column;
  size_t filter_count;
  size_t out_range_lens;

  // recompute `range_lens`, if remain data LT `max_batch_size_`
  // then should reduce `range_lens`
  if ((range_begin + range_lens) > columns->GetRows()) {
    range_lens = columns->GetRows() - range_begin;
  }

  filter_count = GetInvisibleNumber(range_begin, range_lens);
  Assert(range_lens >= filter_count);
  out_range_lens = range_lens - filter_count;

  if (out_range_lens == 0) {
    return std::make_pair(0, range_begin + range_lens);
  }

  for (size_t index = 0; index < columns->GetColumns(); index++) {
    size_t data_index_begin = 0;
    size_t num_of_not_nulls = 0;
    DataBuffer<char> *vec_buffer = nullptr;
    DataBuffer<int32> *offset_buffer = nullptr;

    char *raw_buffer = nullptr;
    size_t buffer_len = 0;
    PaxColumnTypeInMem column_type;

    if ((*columns)[index] == nullptr) {
      continue;
    }

    column = (*columns)[index];
    Assert(index < (size_t)vec_cache_buffer_lens_ && vec_cache_buffer_);

    data_index_begin = column->GetRangeNonNullRows(0, range_begin);
    num_of_not_nulls = column->GetRangeNonNullRows(range_begin, range_lens);

    // data buffer holder
    vec_buffer = &(vec_cache_buffer_[index].vec_buffer);
    offset_buffer = &(vec_cache_buffer_[index].offset_buffer);

    // copy null bitmap
    vec_cache_buffer_[index].null_counts = 0;
    CopyBitmapBuffer(column, micro_partition_visibility_bitmap_,
                     range_begin + group_base_offset_, range_begin, range_lens,
                     num_of_not_nulls, out_range_lens,
                     &(vec_cache_buffer_[index].null_bits_buffer),
                     &(vec_cache_buffer_[index].null_counts));

    // copy data
    std::tie(raw_buffer, buffer_len) =
        column->GetRangeBuffer(data_index_begin, num_of_not_nulls);
    column_type = column->GetPaxColumnTypeInMem();

    switch (column_type) {
      case PaxColumnTypeInMem::kTypeDecimal: {
        // notice that: current arrow require the 16 width numeric return
        auto align_size =
            TYPEALIGN(MEMORY_ALIGN_SIZE,
                      (out_range_lens * VEC_SHORT_NUMERIC_STORE_BYTES));
        Assert(!vec_buffer->GetBuffer());

        vec_buffer->Set(BlockBuffer::Alloc<char *>(align_size), align_size);

        CopyDecimalBuffer(column, micro_partition_visibility_bitmap_,
                          range_begin + group_base_offset_, range_begin,
                          range_lens, data_index_begin, num_of_not_nulls,
                          vec_buffer);
        break;
      }
      case PaxColumnTypeInMem::kTypeBpChar:
      case PaxColumnTypeInMem::kTypeNonFixed: {
        auto data_align_size =
            CalcRecordBatchDataBufferSize(column, buffer_len, num_of_not_nulls);
        auto offset_align_bytes =
            TYPEALIGN(MEMORY_ALIGN_SIZE, (out_range_lens + 1) * sizeof(int32));

        Assert(!vec_buffer->GetBuffer() && !offset_buffer->GetBuffer());
        vec_buffer->Set(BlockBuffer::Alloc<char *>(data_align_size),
                        data_align_size);
        offset_buffer->Set(BlockBuffer::Alloc<char *>(offset_align_bytes),
                           offset_align_bytes);

        CopyNonFixedBuffer(column, micro_partition_visibility_bitmap_,
                           range_begin + group_base_offset_, range_begin,
                           range_lens, data_index_begin, num_of_not_nulls,
                           offset_buffer, vec_buffer,
                           column_type == PaxColumnTypeInMem::kTypeBpChar);

        break;
      }
      case PaxColumnTypeInMem::kTypeFixed: {
        Assert(column->GetTypeLength() > 0);
        auto align_size = TYPEALIGN(MEMORY_ALIGN_SIZE,
                                    (out_range_lens * column->GetTypeLength()));
        Assert(!vec_buffer->GetBuffer());

        vec_buffer->Set(BlockBuffer::Alloc<char *>(align_size), align_size);
        CopyFixedBuffer(column, micro_partition_visibility_bitmap_,
                        range_begin + group_base_offset_, range_begin,
                        range_lens, data_index_begin, num_of_not_nulls,
                        vec_buffer);

        break;
      }
      case PaxColumnTypeInMem::kTypeBitPacked: {
        auto align_size =
            TYPEALIGN(MEMORY_ALIGN_SIZE, BITS_TO_BYTES(out_range_lens));
        Assert(!vec_buffer->GetBuffer());
        // the boolean_buffer is bitpacked-layout, we must use Alloc0 to fill it
        // with zeros. then we can only set the bit according to the index of
        // true value.
        auto boolean_buffer = BlockBuffer::Alloc0<char *>(align_size);
        vec_buffer->Set(boolean_buffer, align_size);

        Bitmap8 vec_bool_bitmap(
            BitmapRaw<uint8>((uint8 *)(boolean_buffer), align_size),
            BitmapTpl<uint8>::ReadOnlyRefBitmap);

        CopyBitPackedBuffer(column, micro_partition_visibility_bitmap_,
                            range_begin + group_base_offset_, range_begin,
                            range_lens, data_index_begin, num_of_not_nulls,
                            &vec_bool_bitmap);
        break;
      }
      default: {
        CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
      }
    }  // switch column type
  }

  return std::make_pair(out_range_lens, range_begin + range_lens);
}

}  // namespace pax

#endif  // VEC_BUILD
