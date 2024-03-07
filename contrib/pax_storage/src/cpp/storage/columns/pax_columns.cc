#include "storage/columns/pax_columns.h"

#include <cstring>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "comm/pax_memory.h"
#include "storage/columns/pax_column_traits.h"

namespace pax {

template <typename N>
static PaxColumn *CreateCommColumn(bool is_vec,
                                   const PaxEncoder::EncodingOption &opts) {
  return is_vec
             ? (PaxColumn *)traits::ColumnOptCreateTraits<
                   PaxVecEncodingColumn, N>::create_encoding(DEFAULT_CAPACITY,
                                                             opts)
             : (PaxColumn *)traits::ColumnOptCreateTraits<
                   PaxEncodingColumn, N>::create_encoding(DEFAULT_CAPACITY,
                                                          opts);
}

PaxColumns::PaxColumns(const std::vector<pax::orc::proto::Type_Kind> &types,
                       const std::vector<std::tuple<ColumnEncoding_Kind, int>>
                           &column_encoding_types,
                       const PaxStorageFormat &storage_format)
    : row_nums_(0), storage_format_(storage_format) {
  Assert(columns_.empty());
  Assert(types.size() == column_encoding_types.size());
  data_ = PAX_NEW<DataBuffer<char>>(0);
  auto is_vec = storage_format_ == PaxStorageFormat::kTypeStorageOrcVec;

  for (size_t i = 0; i < types.size(); i++) {
    auto type = types[i];

    PaxEncoder::EncodingOption encoding_option;
    encoding_option.column_encode_type = std::get<0>(column_encoding_types[i]);
    encoding_option.is_sign = true;
    encoding_option.compress_level = std::get<1>(column_encoding_types[i]);

    switch (type) {
      case (pax::orc::proto::Type_Kind::Type_Kind_STRING): {
        encoding_option.is_sign = false;
        columns_.emplace_back(
            is_vec ? (PaxColumn *)traits::ColumnOptCreateTraits2<
                         PaxVecNonFixedEncodingColumn>::
                         create_encoding(DEFAULT_CAPACITY,
                                         std::move(encoding_option))
                   : (PaxColumn *)traits::ColumnOptCreateTraits2<
                         PaxNonFixedEncodingColumn>::
                         create_encoding(DEFAULT_CAPACITY,
                                         std::move(encoding_option)));
        break;
      }
      case (pax::orc::proto::Type_Kind::Type_Kind_BOOLEAN):
      case (pax::orc::proto::Type_Kind::Type_Kind_BYTE):  // len 1 integer
        columns_.emplace_back(
            CreateCommColumn<int8>(is_vec, std::move(encoding_option)));
        break;
      case (pax::orc::proto::Type_Kind::Type_Kind_SHORT):  // len 2 integer
        columns_.emplace_back(
            CreateCommColumn<int16>(is_vec, std::move(encoding_option)));
        break;
      case (pax::orc::proto::Type_Kind::Type_Kind_INT):  // len 4 integer
        columns_.emplace_back(
            CreateCommColumn<int32>(is_vec, std::move(encoding_option)));
        break;
      case (pax::orc::proto::Type_Kind::Type_Kind_LONG):  // len 8 integer
        columns_.emplace_back(
            CreateCommColumn<int64>(is_vec, std::move(encoding_option)));
        break;
      default:
        // TODO(jiaqizho): support other column type
        // but now should't be here
        Assert(!"non-implemented column type");
        break;
    }
  }
}

PaxColumns::PaxColumns()
    : row_nums_(0), storage_format_(PaxStorageFormat::kTypeStorageOrcNonVec) {
  data_ = PAX_NEW<DataBuffer<char>>(0);
}

PaxColumns::~PaxColumns() {
  // Notice that: the resources freed here,
  // must transform owner in `PaxColumns::Merge`
  for (auto column : columns_) {
    PAX_DELETE(column);
  }
  for (auto holder : data_holder_) {
    PAX_DELETE(holder);
  }
  PAX_DELETE(data_);
}

void PaxColumns::SetStorageFormat(PaxStorageFormat format) {
  storage_format_ = format;
}

PaxStorageFormat PaxColumns::GetStorageFormat() const {
  return storage_format_;
}

void PaxColumns::Merge(PaxColumns *columns) {
  Assert(GetColumns() == columns->GetColumns());
  Assert(GetRows() == columns->GetRows());
  Assert(columns->data_holder_.empty());

  for (size_t i = 0; i < columns->GetColumns(); i++) {
    AssertImply(columns_[i], !columns->columns_[i]);
    if (!columns_[i] && columns->columns_[i]) {
      columns_[i] = columns->columns_[i];
      columns->columns_[i] = nullptr;
    }
  }

  if (columns->data_) {
    data_holder_.emplace_back(columns->data_);
    columns->data_ = nullptr;
  }

  PAX_DELETE(columns);
}

PaxColumn *PaxColumns::operator[](uint64 i) { return columns_[i]; }

void PaxColumns::Append(PaxColumn *column) { columns_.emplace_back(column); }

void PaxColumns::Append(char * /*buffer*/, size_t /*size*/) {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

void PaxColumns::Set(DataBuffer<char> *data) {
  Assert(data_->GetBuffer() == nullptr);

  PAX_DELETE(data_);
  data_ = data;
}

size_t PaxColumns::GetNonNullRows() const {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

int32 PaxColumns::GetTypeLength() const {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

size_t PaxColumns::PhysicalSize() const {
  size_t total_size = 0;
  for (auto column : columns_) {
    if (column) total_size += column->PhysicalSize();
  }
  return total_size;
}

int64 PaxColumns::GetOriginLength() const {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

size_t PaxColumns::GetColumns() const { return columns_.size(); }

std::pair<char *, size_t> PaxColumns::GetBuffer() {
  PaxColumns::ColumnStreamsFunc column_streams_func_null;
  PaxColumns::ColumnEncodingFunc column_encoding_func_null;
  auto data_buffer =
      GetDataBuffer(column_streams_func_null, column_encoding_func_null);
  return std::make_pair(data_buffer->GetBuffer(), data_buffer->Used());
}

std::pair<char *, size_t> PaxColumns::GetBuffer(size_t position) {
  if (position >= GetColumns()) {
    CBDB_RAISE(cbdb::CException::ExType::kExTypeOutOfRange);
  }
  if (columns_[position]) {
    return columns_[position]->GetBuffer();
  } else {
    return std::make_pair(nullptr, 0);
  }
}

std::pair<char *, size_t> PaxColumns::GetRangeBuffer(size_t /*start_pos*/,
                                                     size_t /*len*/) {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

size_t PaxColumns::AlignSize(size_t buf_len, size_t len, size_t align_size) {
  if ((buf_len + len) % align_size != 0) {
    auto align_buf_len = TYPEALIGN(align_size, (buf_len + len));
    Assert(align_buf_len - buf_len > len);
    len = align_buf_len - buf_len;
  }
  return len;
}

DataBuffer<char> *PaxColumns::GetDataBuffer(
    const ColumnStreamsFunc &column_streams_func,
    const ColumnEncodingFunc &column_encoding_func) {
  size_t buffer_len = 0;

  if (data_->GetBuffer() != nullptr) {
    // warning here: better not call GetDataBuffer twice
    // memcpy will happen in GetDataBuffer
    data_->Clear();
  }

#ifdef ENABLE_DEBUG
  auto storage_format = GetStorageFormat();
  for (auto column : columns_) {
    AssertImply(column, column->GetStorageFormat() == storage_format);
  }
#endif

  if (COLUMN_STORAGE_FORMAT_IS_VEC(this)) {
    buffer_len =
        MeasureVecDataBuffer(column_streams_func, column_encoding_func);
    data_->Set(reinterpret_cast<char *>(cbdb::Palloc(buffer_len)), buffer_len,
               0);
    CombineVecDataBuffer();
  } else {
    buffer_len =
        MeasureOrcDataBuffer(column_streams_func, column_encoding_func);
    data_->Set(reinterpret_cast<char *>(cbdb::Palloc(buffer_len)), buffer_len,
               0);
    CombineOrcDataBuffer();
  }

  Assert(data_->Used() == buffer_len);
  Assert(data_->Available() == 0);
  return data_;
}

size_t PaxColumns::MeasureVecDataBuffer(
    const ColumnStreamsFunc &column_streams_func,
    const ColumnEncodingFunc &column_encoding_func) {
  size_t buffer_len = 0;
  for (auto column : columns_) {
    if (!column) {
      continue;
    }

    size_t total_rows = column->GetRows();
    size_t non_null_rows = column->GetNonNullRows();

    // has null will generate a bitmap in current stripe
    if (column->HasNull()) {
      auto bm = column->GetBitmap();
      Assert(bm);
      size_t bm_length = bm->MinimalStoredBytes(total_rows);

      bm_length = TYPEALIGN(MEMORY_ALIGN_SIZE, bm_length);
      buffer_len += bm_length;
      column_streams_func(pax::orc::proto::Stream_Kind_PRESENT, total_rows,
                          bm_length);
    }

    switch (column->GetPaxColumnTypeInMem()) {
      case kTypeNonFixed: {
        size_t offsets_size =
            TYPEALIGN(MEMORY_ALIGN_SIZE, (total_rows + 1) * sizeof(int32));
        buffer_len += offsets_size;
        column_streams_func(pax::orc::proto::Stream_Kind_LENGTH, total_rows,
                            offsets_size);

        auto data_length = column->GetBuffer().second;
        if (column->GetEncodingType() ==
            ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
          data_length = TYPEALIGN(MEMORY_ALIGN_SIZE, data_length);
        }

        buffer_len += data_length;
        column_streams_func(pax::orc::proto::Stream_Kind_DATA, non_null_rows,
                            data_length);

        break;
      }
      case kTypeFixed: {
        auto data_length = column->GetBuffer().second;
        if (column->GetEncodingType() ==
            ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
          data_length = TYPEALIGN(MEMORY_ALIGN_SIZE, data_length);
        }

        buffer_len += data_length;
        column_streams_func(pax::orc::proto::Stream_Kind_DATA, non_null_rows,
                            data_length);
        break;
      }
      case kTypeInvalid:
      default: {
        CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
        break;
      }
    }

    AssertImply(column->GetEncodingType() !=
                    ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED,
                column->GetOriginLength() >= 0);

    column_encoding_func(
        column->GetEncodingType(), column->GetCompressLevel(),
        (column->GetEncodingType() !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED)
            ? TYPEALIGN(MEMORY_ALIGN_SIZE, column->GetOriginLength())
            : column->GetOriginLength());
  }
  return buffer_len;
}

size_t PaxColumns::MeasureOrcDataBuffer(
    const ColumnStreamsFunc &column_streams_func,
    const ColumnEncodingFunc &column_encoding_func) {
  size_t buffer_len = 0;

  for (auto column : columns_) {
    if (!column) {
      continue;
    }

    // has null will generate a bitmap in current stripe
    if (column->HasNull()) {
      auto bm = column->GetBitmap();
      Assert(bm);
      size_t bm_length = bm->MinimalStoredBytes(column->GetRows());
      buffer_len += bm_length;
      column_streams_func(pax::orc::proto::Stream_Kind_PRESENT,
                          column->GetRows(), bm_length);
    }

    size_t column_size = column->GetNonNullRows();

    switch (column->GetPaxColumnTypeInMem()) {
      case kTypeNonFixed: {
        size_t lengths_size = column_size * sizeof(int32);

        if ((buffer_len + lengths_size) % column->GetAlignSize() != 0) {
          auto align_buffer_len =
              TYPEALIGN(column->GetAlignSize(), (buffer_len + lengths_size));
          Assert(align_buffer_len - buffer_len > lengths_size);
          lengths_size = align_buffer_len - buffer_len;
        }

        buffer_len += lengths_size;
        column_streams_func(pax::orc::proto::Stream_Kind_LENGTH, column_size,
                            lengths_size);

        auto length_data = column->GetBuffer().second;
        buffer_len += length_data;

        column_streams_func(pax::orc::proto::Stream_Kind_DATA, column_size,
                            length_data);

        break;
      }
      case kTypeFixed: {
        auto length_data = column->GetBuffer().second;
        buffer_len += length_data;
        column_streams_func(pax::orc::proto::Stream_Kind_DATA, column_size,
                            length_data);

        break;
      }
      case kTypeInvalid:
      default: {
        CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
        break;
      }
    }

    column_encoding_func(column->GetEncodingType(), column->GetCompressLevel(),
                         column->GetOriginLength());
  }
  return buffer_len;
}

void PaxColumns::CombineVecDataBuffer() {
  char *buffer = nullptr;
  size_t buffer_len = 0;

  auto fill_padding_buffer = [](PaxColumn *column,
                                DataBuffer<char> *data_buffer,
                                size_t buffer_len, size_t align) {
    Assert(data_buffer);

    if (column && (column->GetEncodingType() !=
                   ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED)) {
      return;
    }

    auto gap_size = TYPEALIGN(align, buffer_len) - buffer_len;
    if (!gap_size) {
      return;
    }

    data_buffer->WriteZero(gap_size);
    data_buffer->Brush(gap_size);
  };

  for (auto column : columns_) {
    if (!column) {
      continue;
    }

    if (column->HasNull()) {
      auto bm = column->GetBitmap();
      auto nbytes = bm->MinimalStoredBytes(column->GetRows());
      Assert(nbytes <= bm->Raw().size);

      Assert(data_->Available() >= nbytes);
      data_->Write(reinterpret_cast<char *>(bm->Raw().bitmap), nbytes);
      data_->Brush(nbytes);

      fill_padding_buffer(nullptr, data_, nbytes, MEMORY_ALIGN_SIZE);
    }

    switch (column->GetPaxColumnTypeInMem()) {
      case kTypeNonFixed: {
        auto no_fixed_column = reinterpret_cast<PaxVecNonFixedColumn *>(column);
        auto offset_data_buffer = no_fixed_column->GetOffsetBuffer(true);

        Assert(data_->Available() >= offset_data_buffer->Used());
        data_->Write((char *)offset_data_buffer->GetBuffer(),
                     offset_data_buffer->Used());
        data_->Brush(offset_data_buffer->Used());

        fill_padding_buffer(nullptr, data_, offset_data_buffer->Used(),
                            MEMORY_ALIGN_SIZE);

        std::tie(buffer, buffer_len) = column->GetBuffer();
        Assert(data_->Available() >= buffer_len);
        data_->Write(buffer, buffer_len);
        data_->Brush(buffer_len);

        fill_padding_buffer(no_fixed_column, data_, buffer_len,
                            MEMORY_ALIGN_SIZE);

        break;
      }
      case kTypeFixed: {
        std::tie(buffer, buffer_len) = column->GetBuffer();

        Assert(data_->Available() >= buffer_len);
        data_->Write(buffer, buffer_len);
        data_->Brush(buffer_len);

        fill_padding_buffer(column, data_, buffer_len, MEMORY_ALIGN_SIZE);
        break;
      }
      case kTypeInvalid:
      default:
        break;
    }
  }
}

void PaxColumns::CombineOrcDataBuffer() {
  char *buffer = nullptr;
  size_t buffer_len = 0;

  for (auto column : columns_) {
    if (!column) {
      continue;
    }

    if (column->HasNull()) {
      auto bm = column->GetBitmap();
      auto nbytes = bm->MinimalStoredBytes(column->GetRows());
      Assert(nbytes <= bm->Raw().size);

      data_->Write(reinterpret_cast<char *>(bm->Raw().bitmap), nbytes);
      data_->Brush(nbytes);
    }

    switch (column->GetPaxColumnTypeInMem()) {
      case kTypeNonFixed: {
        auto no_fixed_column = reinterpret_cast<PaxNonFixedColumn *>(column);
        auto length_data_buffer = no_fixed_column->GetLengthBuffer();

        auto lengths_size = length_data_buffer->Used();
        auto current_buffer_len = data_->Used();
        if ((current_buffer_len + lengths_size) % column->GetAlignSize() != 0) {
          auto align_buffer_len = TYPEALIGN(
              column->GetAlignSize(), (current_buffer_len + lengths_size));
          Assert(align_buffer_len - current_buffer_len > lengths_size);
          lengths_size = align_buffer_len - current_buffer_len;
        }

        memcpy(data_->GetAvailableBuffer(), length_data_buffer->GetBuffer(),
               length_data_buffer->Used());
        data_->Brush(length_data_buffer->Used());

        Assert(lengths_size >= length_data_buffer->Used());
        if (lengths_size > length_data_buffer->Used()) {
          auto padding = lengths_size - length_data_buffer->Used();
          data_->WriteZero(padding);
          data_->Brush(padding);
        }

        std::tie(buffer, buffer_len) = column->GetBuffer();
        data_->Write(buffer, buffer_len);
        data_->Brush(buffer_len);

        break;
      }
      case kTypeFixed: {
        std::tie(buffer, buffer_len) = column->GetBuffer();
        data_->Write(buffer, buffer_len);
        data_->Brush(buffer_len);
        break;
      }
      case kTypeInvalid:
      default:
        break;
    }
  }
}

std::pair<Datum, bool> GetColumnValue(PaxColumns *columns, size_t column_index,
                                      size_t row_index) {
  Assert(column_index < columns->GetColumns());
  Assert(row_index < columns->GetRows());

  Datum datum = 0;
  auto column = (*columns)[column_index];
  char *buffer;
  size_t buffer_len;

  Assert(column);
  if (column->HasNull()) {
    auto bitmap = column->GetBitmap();
    if (!bitmap->Test(row_index)) return {datum, true};
  }
  auto nonnulls = column->GetRangeNonNullRows(0, row_index);
  column->GetBuffer();
  std::tie(buffer, buffer_len) = column->GetBuffer(nonnulls);
  switch (column->GetPaxColumnTypeInMem()) {
    case kTypeNonFixed:
      datum = PointerGetDatum(buffer);
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
    default:
      Assert(!"should't be here, non-implemented column type in memory");
      break;
  }
  return {datum, false};
}
}  //  namespace pax
