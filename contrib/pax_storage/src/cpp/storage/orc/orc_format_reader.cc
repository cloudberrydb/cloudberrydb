#include "storage/orc/orc_format_reader.h"

#include "comm/cbdb_wrappers.h"
#include "comm/pax_memory.h"
#include "storage/columns/pax_column_traits.h"
#include "storage/orc/orc_defined.h"

namespace pax {

OrcFormatReader::OrcFormatReader(File *file)
    : file_(file),
      reused_buffer_(nullptr),
      num_of_stripes_(0),
      is_vec_(false) {}

OrcFormatReader::~OrcFormatReader() { PAX_DELETE(file_); }

void OrcFormatReader::SetReusedBuffer(DataBuffer<char> *data_buffer) {
  reused_buffer_ = data_buffer;
}

void OrcFormatReader::Open() {
  size_t file_length = 0;
  uint64 post_script_len = 0;

  Assert(file_);
  auto read_in_disk = [this](size_t file_size, size_t ps_len,
                             bool skip_read_post_script) {
    // read post script
    if (!skip_read_post_script) {
      char post_script_buffer[ps_len];
      off_t offset;

      offset = (off_t)(file_size - ORC_POST_SCRIPT_SIZE - ps_len);
      Assert(offset >= 0);

      file_->PReadN(post_script_buffer, ps_len, offset);

      CBDB_CHECK(post_script_.ParseFromArray(&post_script_buffer,
                                             static_cast<int>(ps_len)),
                 cbdb::CException::ExType::kExTypeIOError);
    }

    size_t footer_len = post_script_.footerlength();
    size_t tail_len = ORC_POST_SCRIPT_SIZE + ps_len + footer_len;
    size_t footer_offset = file_size - tail_len;

    // read file_footer
    {
      // The footer contains statistical information. The min/max implementation
      // may have a large attribute value. buffer cannot be allocated on the
      // stack, which may cause stack overflow.
      std::unique_ptr<char[]> buffer(new char[footer_len]());

      file_->PReadN(buffer.get(), footer_len, footer_offset);

      SeekableInputStream input_stream(buffer.get(), footer_len);
      CBDB_CHECK(file_footer_.ParseFromZeroCopyStream(&input_stream),
                 cbdb::CException::ExType::kExTypeIOError);
    }
  };

  file_length = file_->FileLength();
  if (file_length > ORC_TAIL_SIZE) {
    size_t footer_len;
    size_t tail_len;
    size_t footer_offset;
    char tail_buffer[ORC_TAIL_SIZE];

    file_->PReadN(tail_buffer, ORC_TAIL_SIZE,
                  (off_t)(file_length - ORC_TAIL_SIZE));

    static_assert(sizeof(post_script_len) == ORC_POST_SCRIPT_SIZE,
                  "post script type len not match.");
    memcpy(&post_script_len, &tail_buffer[ORC_TAIL_SIZE - ORC_POST_SCRIPT_SIZE],
           ORC_POST_SCRIPT_SIZE);
    if (post_script_len + ORC_POST_SCRIPT_SIZE > ORC_TAIL_SIZE) {
      read_in_disk(file_length, post_script_len, false);
      goto finish_read;
    }

    auto post_script_offset =
        (off_t)(ORC_TAIL_SIZE - ORC_POST_SCRIPT_SIZE - post_script_len);
    CBDB_CHECK(post_script_.ParseFromArray(tail_buffer + post_script_offset,
                                           static_cast<int>(post_script_len)),
               cbdb::CException::ExType::kExTypeIOError);

    footer_len = post_script_.footerlength();
    tail_len = ORC_POST_SCRIPT_SIZE + post_script_len + footer_len;
    if (tail_len > ORC_TAIL_SIZE) {
      read_in_disk(file_length, post_script_len, true);
      goto finish_read;
    }

    footer_offset = ORC_TAIL_SIZE - tail_len;
    SeekableInputStream input_stream(tail_buffer + footer_offset, footer_len);
    CBDB_CHECK(file_footer_.ParseFromZeroCopyStream(&input_stream),
               cbdb::CException::ExType::kExTypeIOError);
  } else {
    static_assert(sizeof(post_script_len) == ORC_POST_SCRIPT_SIZE,
                  "post script type len not match.");
    file_->PReadN(&post_script_len, ORC_POST_SCRIPT_SIZE,
                  (off_t)(file_length - ORC_POST_SCRIPT_SIZE));
    read_in_disk(file_length, post_script_len, false);
  }

finish_read:
  num_of_stripes_ = file_footer_.stripes_size();
  is_vec_ = file_footer_.storageformat() == kTypeStorageOrcVec;

  // Build schema
  auto max_id = file_footer_.types_size();
  CBDB_CHECK(max_id > 0, cbdb::CException::ExType::kExTypeInvalidORCFormat);

  const pax::orc::proto::Type &type = file_footer_.types(0);
  // There is an assumption here: for all pg tables, the outermost structure
  // should be Type_Kind_STRUCT
  CBDB_CHECK(type.kind() == pax::orc::proto::Type_Kind_STRUCT,
             cbdb::CException::ExType::kExTypeInvalidORCFormat);
  CBDB_CHECK(type.subtypes_size() == max_id - 1,
             cbdb::CException::ExType::kExTypeInvalidORCFormat);

  for (int j = 0; j < type.subtypes_size(); ++j) {
    int sub_type_id = static_cast<int>(type.subtypes(j)) + 1;
    const pax::orc::proto::Type &sub_type = file_footer_.types(sub_type_id);
    // should allow struct contain struct
    // but not support yet
    CBDB_CHECK(sub_type.kind() != pax::orc::proto::Type_Kind_STRUCT,
               cbdb::CException::ExType::kExTypeInvalidORCFormat);

    column_types_.emplace_back(sub_type.kind());
  }

  // Build stripe row offset array
  size_t cur_stripe_row_offset = 0;
  for (size_t i = 0; i < num_of_stripes_; i++) {
    stripe_row_offsets_.emplace_back(cur_stripe_row_offset);
    cur_stripe_row_offset += file_footer_.stripes(i).numberofrows();
  }
}

void OrcFormatReader::Close() { file_->Close(); }

size_t OrcFormatReader::GetStripeNums() const { return num_of_stripes_; }

size_t OrcFormatReader::GetStripeNumberOfRows(size_t stripe_index) {
  Assert(stripe_index < GetStripeNums());
  return file_footer_.stripes(static_cast<int>(stripe_index)).numberofrows();
}

size_t OrcFormatReader::GetStripeOffset(size_t stripe_index) {
  Assert(stripe_index < GetStripeNums());
  return stripe_row_offsets_[stripe_index];
}

// FIXME(jiaqizho): move method to higher level
static bool ProjShouldReadAll(const bool *const proj_map, size_t proj_len) {
  if (!proj_map) {
    return true;
  }

  for (size_t i = 0; i < proj_len; i++) {
    if (!proj_map[i]) {
      return false;
    }
  }

  return true;
}

pax::orc::proto::StripeFooter OrcFormatReader::ReadStripeFooter(
    DataBuffer<char> *data_buffer, size_t sf_length, size_t sf_offset,
    size_t sf_data_len) {
  pax::orc::proto::StripeFooter stripe_footer;

  Assert(data_buffer->Capacity() >= (sf_length - sf_data_len));
  file_->PReadN(data_buffer->GetBuffer(), sf_length - sf_data_len,
                sf_offset + sf_data_len);
  SeekableInputStream input_stream(data_buffer->GetBuffer(),
                                   sf_length - sf_data_len);
  if (!stripe_footer.ParseFromZeroCopyStream(&input_stream)) {
    // fail to do memory io with protobuf
    CBDB_RAISE(cbdb::CException::ExType::kExTypeIOError);
  }

  return stripe_footer;
}

pax::orc::proto::StripeFooter OrcFormatReader::ReadStripeFooter(
    DataBuffer<char> *data_buffer, size_t stripe_index) {
  size_t sf_data_len;
  size_t sf_offset;
  size_t sf_length;
  pax::orc::proto::StripeInformation stripe_info;

  Assert(stripe_index < GetStripeNums());

  stripe_info = file_footer_.stripes(static_cast<int>(stripe_index));

  sf_data_len = stripe_info.datalength();
  sf_offset = stripe_info.offset();
  sf_length = stripe_info.footerlength();

  Assert(data_buffer->IsMemTakeOver());
  Assert(data_buffer->Used() == 0);

  if (data_buffer->Capacity() < (sf_length - sf_data_len)) {
    data_buffer->ReSize(sf_length - sf_data_len);
  }

  return ReadStripeFooter(data_buffer, sf_length, sf_offset, sf_data_len);
}

pax::orc::proto::StripeFooter OrcFormatReader::ReadStripeWithProjection(
    DataBuffer<char> *data_buffer,
    const ::pax::orc::proto::StripeInformation &stripe_info,
    const bool *const proj_map, size_t proj_len) {
  pax::orc::proto::StripeFooter stripe_footer;
  size_t stripe_footer_data_len;
  size_t stripe_footer_offset;
  size_t stripe_footer_length;

  size_t streams_index = 0;
  uint64_t batch_len = 0;
  uint64_t batch_offset = 0;
  size_t index = 0;

  stripe_footer_data_len = stripe_info.datalength();
  stripe_footer_offset = stripe_info.offset();
  stripe_footer_length = stripe_info.footerlength();

  /* Check all column projection is true.
   * If no need do column projection, read all
   * buffer(data + stripe footer) from stripe and decode stripe footer.
   */
  if (ProjShouldReadAll(proj_map, proj_len)) {
    file_->PReadN(data_buffer->GetBuffer(), stripe_footer_length,
                  stripe_footer_offset);
    SeekableInputStream input_stream(
        data_buffer->GetBuffer() + stripe_footer_data_len,
        stripe_footer_length - stripe_footer_data_len);
    if (!stripe_footer.ParseFromZeroCopyStream(&input_stream)) {
      // fail to do memory io with protobuf
      CBDB_RAISE(cbdb::CException::ExType::kExTypeIOError);
    }

    return stripe_footer;
  }

  /* If need do column projection here
   * Then read stripe footer and decode it before read data part
   */
  stripe_footer =
      ReadStripeFooter(data_buffer, stripe_footer_length, stripe_footer_offset,
                       stripe_footer_data_len);
  data_buffer->BrushBackAll();

  batch_offset = stripe_footer_offset;

  while (index < column_types_.size()) {
    // Current column have been skipped
    // Move `batch_offset` and `streams_index` to the right position
    if (!proj_map[index]) {
      index++;

      const pax::orc::proto::Stream *n_stream = nullptr;
      do {
        n_stream = &stripe_footer.streams(streams_index++);
        batch_offset += n_stream->length();
      } while (n_stream->kind() !=
               ::pax::orc::proto::Stream_Kind::Stream_Kind_DATA);

      continue;
    }

    batch_len = 0;

    /* Current column should be read
     * In this case, did a greedy algorithm to combine io: while
     * the current column is being read, it is necessary
     * to ensure that subsequent columns will be read in the same io.
     *
     * So in `do...while`, only the `batch_size` which io needs to read
     * is calculated, until meet a column which needs to be skipped.
     */
    do {
      bool has_null = stripe_info.colstats(index).hasnull();
      if (has_null) {
        const pax::orc::proto::Stream &non_null_stream =
            stripe_footer.streams(streams_index++);
        batch_len += non_null_stream.length();
      }

      const pax::orc::proto::Stream *len_or_data_stream =
          &stripe_footer.streams(streams_index++);
      batch_len += len_or_data_stream->length();

      if (len_or_data_stream->kind() ==
          ::pax::orc::proto::Stream_Kind::Stream_Kind_LENGTH) {
        len_or_data_stream = &stripe_footer.streams(streams_index++);
        batch_len += len_or_data_stream->length();
      }
    } while ((++index) < column_types_.size() && proj_map[index]);

    file_->PReadN(data_buffer->GetAvailableBuffer(), batch_len, batch_offset);
    data_buffer->Brush(batch_len);
    batch_offset += batch_len;
  }

  return stripe_footer;
}

template <typename T>
static PaxColumn *BuildEncodingColumn(
    DataBuffer<char> *data_buffer, const pax::orc::proto::Stream &data_stream,
    const ColumnEncoding &data_encoding, bool is_vec) {
  uint32 not_null_rows = 0;
  uint64 column_data_len = 0;
  DataBuffer<T> *column_data_buffer = nullptr;

  Assert(data_stream.kind() == pax::orc::proto::Stream_Kind_DATA);

  not_null_rows = static_cast<uint32>(data_stream.column());
  column_data_len = static_cast<uint64>(data_stream.length());

  column_data_buffer = PAX_NEW<DataBuffer<T>>(
      reinterpret_cast<T *>(data_buffer->GetAvailableBuffer()), column_data_len,
      false, false);

  column_data_buffer->BrushAll();
  data_buffer->Brush(column_data_len);

  PaxDecoder::DecodingOption decoding_option;
  decoding_option.column_encode_type = data_encoding.kind();
  decoding_option.is_sign = true;
  decoding_option.compress_level = data_encoding.compress_lvl();

  size_t alloc_size = 0;

  if (data_encoding.kind() !=
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    alloc_size = data_encoding.length();
  }

  if (is_vec) {
    auto pax_column =
        traits::ColumnOptCreateTraits<PaxVecEncodingColumn, T>::create_decoding(
            alloc_size, decoding_option);
    pax_column->Set(column_data_buffer, (size_t)not_null_rows);
    return pax_column;
  } else {
    AssertImply(data_encoding.kind() ==
                    ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED,
                not_null_rows == column_data_buffer->GetSize());
    AssertImply(data_encoding.kind() !=
                    ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED,
                data_encoding.length() / sizeof(T) == not_null_rows);
    auto pax_column =
        traits::ColumnOptCreateTraits<PaxEncodingColumn, T>::create_decoding(
            alloc_size, decoding_option);
    pax_column->Set(column_data_buffer);
    return pax_column;
  }

  Assert(false);
}

static PaxColumn *BuildEncodingVecNonFixedColumn(
    DataBuffer<char> *data_buffer, const pax::orc::proto::Stream &data_stream,
    const pax::orc::proto::Stream &len_stream,
    const ColumnEncoding &data_encoding) {
  uint32 not_null_rows = 0;
  uint64 column_lens_len = 0;
  uint64 column_data_len = 0;
  DataBuffer<int32> *column_offset_buffer = nullptr;
  DataBuffer<char> *column_data_buffer = nullptr;
  PaxVecNonFixedColumn *pax_column = nullptr;

  auto total_rows = static_cast<uint32>(len_stream.column());
  not_null_rows = static_cast<uint32>(data_stream.column());
  column_data_len = static_cast<uint64>(data_stream.length());
  column_lens_len = static_cast<uint64>(len_stream.length());

  Assert(column_lens_len >= ((total_rows + 1) * sizeof(int32)));
  column_offset_buffer = PAX_NEW<DataBuffer<int32>>(
      reinterpret_cast<int32 *>(data_buffer->GetAvailableBuffer()),
      column_lens_len, false, false);

  column_offset_buffer->Brush((total_rows + 1) * sizeof(int32));
  // at lease 2
  Assert(column_offset_buffer->GetSize() >= 2);
  data_buffer->Brush(column_lens_len);
  column_data_buffer = PAX_NEW<DataBuffer<char>>(
      data_buffer->GetAvailableBuffer(), column_data_len, false, false);

  if (data_encoding.kind() ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    column_data_buffer->Brush(
        (*column_offset_buffer)[column_offset_buffer->GetSize() - 1]);
    data_buffer->Brush(column_data_len);

    pax_column = traits::ColumnCreateTraits2<PaxVecNonFixedColumn>::create(0);
  } else {
    data_buffer->Brush(column_data_len);
    column_data_buffer->BrushAll();

    PaxDecoder::DecodingOption decoding_option;
    decoding_option.column_encode_type = data_encoding.kind();
    decoding_option.is_sign = true;
    decoding_option.compress_level = data_encoding.compress_lvl();

    pax_column =
        traits::ColumnOptCreateTraits2<PaxVecNonFixedEncodingColumn>::  //
        create_decoding(data_encoding.length(), std::move(decoding_option));
  }
  pax_column->Set(column_data_buffer, column_offset_buffer, column_data_len,
                  not_null_rows);
  return pax_column;
}

static PaxColumn *BuildEncodingNonFixedColumn(
    DataBuffer<char> *data_buffer, const pax::orc::proto::Stream &data_stream,
    const pax::orc::proto::Stream &len_stream,
    const ColumnEncoding &data_encoding) {
  uint32 column_lens_size = 0;
  uint64 column_lens_len = 0;
  uint64 column_data_len = 0;
  DataBuffer<int32> *column_len_buffer = nullptr;
  DataBuffer<char> *column_data_buffer = nullptr;
  PaxNonFixedColumn *pax_column = nullptr;

  column_lens_size = static_cast<uint32>(len_stream.column());
  column_lens_len = static_cast<uint64>(len_stream.length());

  column_len_buffer = PAX_NEW<DataBuffer<int32>>(
      reinterpret_cast<int32 *>(data_buffer->GetAvailableBuffer()),
      column_lens_len, false, false);

  Assert(column_lens_len >= column_lens_size * sizeof(int32));
  column_len_buffer->Brush(column_lens_size * sizeof(int32));
  data_buffer->Brush(column_lens_len);

  column_data_len = data_stream.length();

#ifdef ENABLE_DEBUG
  if (data_encoding.kind() ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    size_t segs_size = 0;
    for (size_t i = 0; i < column_len_buffer->GetSize(); i++) {
      segs_size += (*column_len_buffer)[i];
    }
    Assert(column_data_len == segs_size);
  }
#endif

  column_data_buffer = PAX_NEW<DataBuffer<char>>(
      data_buffer->GetAvailableBuffer(), column_data_len, false, false);
  column_data_buffer->BrushAll();
  data_buffer->Brush(column_data_len);

  Assert(static_cast<uint32>(data_stream.column()) == column_lens_size);

  if (data_encoding.kind() ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    Assert(column_data_len == column_data_buffer->GetSize());
    pax_column = traits::ColumnCreateTraits2<PaxNonFixedColumn>::create(0);
  } else {
    PaxDecoder::DecodingOption decoding_option;
    decoding_option.column_encode_type = data_encoding.kind();
    decoding_option.is_sign = true;
    decoding_option.compress_level = data_encoding.compress_lvl();

    pax_column = traits::ColumnOptCreateTraits2<
        PaxNonFixedEncodingColumn>::create_decoding(data_encoding.length(),
                                                    std::move(decoding_option));
  }

  // current memory will be freed in pax_columns->data_
  pax_column->Set(column_data_buffer, column_len_buffer, column_data_len);
  return pax_column;
}

// TODO(jiaqizho): add args buffer
// which can read from a prev read buffer
PaxColumns *OrcFormatReader::ReadStripe(size_t group_index, bool *proj_map,
                                        size_t proj_len) {
  auto stripe_info = file_footer_.stripes(static_cast<int>(group_index));
  auto pax_columns = PAX_NEW<PaxColumns>();
  DataBuffer<char> *data_buffer = nullptr;
  pax::orc::proto::StripeFooter stripe_footer;
  size_t streams_index = 0;
  size_t streams_size = 0;
  size_t encoding_kinds_size = 0;

  pax_columns->AddRows(stripe_info.numberofrows());

  if (unlikely(stripe_info.footerlength() == 0)) {
    return pax_columns;
  }

  if (reused_buffer_) {
    Assert(reused_buffer_->Capacity() >= 4);
    Assert(reused_buffer_->Used() == 0);
    if (reused_buffer_->Available() < stripe_info.footerlength()) {
      reused_buffer_->ReSize(
          reused_buffer_->Used() + stripe_info.footerlength(), 1.5);
    }
    data_buffer = PAX_NEW<DataBuffer<char>>(
        reused_buffer_->GetBuffer(), reused_buffer_->Capacity(), false, false);

  } else {
    data_buffer = PAX_NEW<DataBuffer<char>>(stripe_info.footerlength());
  }
  pax_columns->Set(data_buffer);
  pax_columns->SetStorageFormat(is_vec_
                                    ? PaxStorageFormat::kTypeStorageOrcVec
                                    : PaxStorageFormat::kTypeStorageOrcNonVec);

  /* `ReadStripeWithProjection` will read the column memory which filter by
   * `proj_map`, and initialize `stripe_footer`
   *
   * Notice that: should catch `kExTypeIOError` then delete pax columns
   * But for now we will destroy memory context if exception happen.
   * And we don't have a decision that should we use `try...catch` at yet,
   * so it's ok that we just no catch here.
   */
  stripe_footer =
      ReadStripeWithProjection(data_buffer, stripe_info, proj_map, proj_len);

  streams_size = stripe_footer.streams_size();
  encoding_kinds_size = stripe_footer.pax_col_encodings_size();

  if (unlikely(streams_size == 0 && column_types_.empty())) {
    return pax_columns;
  }

  data_buffer->BrushBackAll();

  AssertImply(proj_len != 0, column_types_.size() <= proj_len);
  Assert(encoding_kinds_size <= column_types_.size());

  for (size_t index = 0; index < column_types_.size(); index++) {
    /* Skip read current column, just move `streams_index` after
     * `Stream_Kind_DATA` but still need append nullptr into `PaxColumns` to
     * make sure sizeof pax_columns eq with column number
     */
    if (proj_map && !proj_map[index]) {
      const pax::orc::proto::Stream *n_stream = nullptr;
      do {
        n_stream = &stripe_footer.streams(streams_index++);
      } while (n_stream->kind() !=
               ::pax::orc::proto::Stream_Kind::Stream_Kind_DATA);

      pax_columns->Append(nullptr);
      continue;
    }

    Bitmap8 *non_null_bitmap = nullptr;
    bool has_null = stripe_info.colstats(index).hasnull();
    if (has_null) {
      const pax::orc::proto::Stream &non_null_stream =
          stripe_footer.streams(streams_index++);
      auto bm_nbytes = static_cast<uint32>(non_null_stream.length());
      auto bm_bytes =
          reinterpret_cast<uint8 *>(data_buffer->GetAvailableBuffer());

      Assert(non_null_stream.kind() == pax::orc::proto::Stream_Kind_PRESENT);
      non_null_bitmap = PAX_NEW<Bitmap8>(BitmapRaw<uint8>(bm_bytes, bm_nbytes),
                                         BitmapTpl<uint8>::ReadOnlyRefBitmap);
      data_buffer->Brush(bm_nbytes);
    }

    switch (column_types_[index]) {
      case (pax::orc::proto::Type_Kind::Type_Kind_STRING): {
        const pax::orc::proto::Stream &len_stream =
            stripe_footer.streams(streams_index++);
        const pax::orc::proto::Stream &data_stream =
            stripe_footer.streams(streams_index++);
        const ColumnEncoding &data_encoding =
            stripe_footer.pax_col_encodings(index);

        Assert(len_stream.kind() == pax::orc::proto::Stream_Kind_LENGTH);
        Assert(data_stream.kind() == pax::orc::proto::Stream_Kind_DATA);

        pax_columns->Append(
            is_vec_ ? BuildEncodingVecNonFixedColumn(data_buffer, data_stream,
                                                     len_stream, data_encoding)
                    : BuildEncodingNonFixedColumn(data_buffer, data_stream,
                                                  len_stream, data_encoding));
        break;
      }
      case (pax::orc::proto::Type_Kind::Type_Kind_BOOLEAN):
      case (pax::orc::proto::Type_Kind::Type_Kind_BYTE):
        pax_columns->Append(BuildEncodingColumn<int8>(
            data_buffer, stripe_footer.streams(streams_index++),
            stripe_footer.pax_col_encodings(index), is_vec_));
        break;
      case (pax::orc::proto::Type_Kind::Type_Kind_SHORT):
        pax_columns->Append(BuildEncodingColumn<int16>(
            data_buffer, stripe_footer.streams(streams_index++),
            stripe_footer.pax_col_encodings(index), is_vec_));
        break;
      case (pax::orc::proto::Type_Kind::Type_Kind_INT): {
        pax_columns->Append(BuildEncodingColumn<int32>(
            data_buffer, stripe_footer.streams(streams_index++),
            stripe_footer.pax_col_encodings(index), is_vec_));
        break;
      }
      case (pax::orc::proto::Type_Kind::Type_Kind_LONG): {
        pax_columns->Append(BuildEncodingColumn<int64>(
            data_buffer, stripe_footer.streams(streams_index++),
            stripe_footer.pax_col_encodings(index), is_vec_));
        break;
      }
      default:
        // should't be here
        Assert(!"should't be here, non-implemented type");
        break;
    }

    // fill nulls data buffer
    Assert(pax_columns->GetColumns() > 0);
    auto last_column = (*pax_columns)[pax_columns->GetColumns() - 1];
    last_column->SetRows(stripe_info.numberofrows());
    if (has_null) {
      Assert(non_null_bitmap);
      last_column->SetBitmap(non_null_bitmap);
    }
  }

#ifdef ENABLE_DEBUG

  auto storage_tyep_verify = is_vec_ ? PaxStorageFormat::kTypeStorageOrcVec
                                     : PaxStorageFormat::kTypeStorageOrcNonVec;

  Assert(storage_tyep_verify == pax_columns->GetStorageFormat());
  for (size_t index = 0; index < column_types_.size(); index++) {
    auto column_verify = (*pax_columns)[index];
    if (column_verify) {
      Assert(storage_tyep_verify == column_verify->GetStorageFormat());
    }
  }

#endif

  Assert(streams_size == streams_index);
  return pax_columns;
}

}  // namespace pax
