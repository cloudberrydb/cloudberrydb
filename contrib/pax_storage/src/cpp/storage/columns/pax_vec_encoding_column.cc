#include "storage/columns/pax_vec_encoding_column.h"

namespace pax {

template <typename T>
PaxVecEncodingColumn<T>::PaxVecEncodingColumn(
    uint32 capacity, const PaxEncoder::EncodingOption &encoding_option)
    : PaxVecCommColumn<T>(capacity),
      encoder_options_(encoding_option),
      encoder_(nullptr),
      decoder_(nullptr),
      shared_data_(nullptr),
      compressor_(nullptr),
      compress_route_(true) {
  PaxVecEncodingColumn<T>::InitEncoder();
}

template <typename T>
PaxVecEncodingColumn<T>::PaxVecEncodingColumn(
    uint32 capacity, const PaxDecoder::DecodingOption &decoding_option)
    : PaxVecCommColumn<T>(capacity),
      encoder_(nullptr),
      decoder_options_{decoding_option},
      decoder_(nullptr),
      shared_data_(nullptr),
      compressor_(nullptr),
      compress_route_(false) {
  PaxVecEncodingColumn<T>::InitDecoder();
}

template <typename T>
PaxVecEncodingColumn<T>::~PaxVecEncodingColumn() {
  delete encoder_;
  delete decoder_;
  delete shared_data_;
  delete compressor_;
}

template <typename T>
void PaxVecEncodingColumn<T>::InitEncoder() {
  if (encoder_options_.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED) {
    encoder_options_.column_encode_type = GetDefaultColumnType();
  }

  PaxColumn::encoded_type_ = encoder_options_.column_encode_type;

  encoder_ = PaxEncoder::CreateStreamingEncoder(encoder_options_);
  if (encoder_) {
    return;
  }

  compressor_ = PaxCompressor::CreateBlockCompressor(PaxColumn::encoded_type_);
  if (compressor_) {
    return;
  }

  PaxColumn::encoded_type_ =
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED;
}

template <typename T>
void PaxVecEncodingColumn<T>::InitDecoder() {
  Assert(decoder_options_.column_encode_type !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);
  PaxColumn::encoded_type_ = decoder_options_.column_encode_type;

  decoder_ = PaxDecoder::CreateDecoder<T>(decoder_options_);
  if (decoder_) {
    // init the shared_data_ with the buffer from PaxVecCommColumn<T>::data_
    // cause decoder_ need a DataBuffer<char> * as dst buffer
    shared_data_ = new DataBuffer<char>(*PaxVecCommColumn<T>::data_);
    decoder_->SetDataBuffer(shared_data_);
    return;
  }

  compressor_ = PaxCompressor::CreateBlockCompressor(PaxColumn::encoded_type_);
}

template <typename T>
void PaxVecEncodingColumn<T>::Set(DataBuffer<T> *data, size_t non_null_rows) {
  PaxColumn::non_null_rows_ = non_null_rows;
  if (decoder_) {
    // should not decoding null
    if (data->Used() != 0) {
      Assert(shared_data_);
      decoder_->SetSrcBuffer(data->Start(), data->Used());
      decoder_->Decoding(nullptr, 0);
      PaxVecCommColumn<T>::data_->Brush(shared_data_->Used());
    }

    Assert(!data->IsMemTakeOver());
    delete data;
  } else if (compressor_) {
    if (data->Used() != 0) {
      // should not init `shared_data_`, direct uncompress to `data_`
      Assert(!shared_data_);
      size_t d_size = compressor_->Decompress(
          PaxVecCommColumn<T>::data_->Start(),
          PaxVecCommColumn<T>::data_->Capacity(), data->Start(), data->Used());
      if (compressor_->IsError(d_size)) {
        // log error with `compressor_->ErrorName(d_size)`
        CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError);
      }

      PaxVecCommColumn<T>::data_->Brush(d_size);
    }

    Assert(!data->IsMemTakeOver());
    delete data;
  } else {
    PaxVecCommColumn<T>::Set(data, non_null_rows);
  }
}

template <typename T>
std::pair<char *, size_t> PaxVecEncodingColumn<T>::GetBuffer() {
  if (compress_route_) {
    // already done with decoding/compress
    if (shared_data_) {
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }

    // no data for encoding
    if (PaxVecCommColumn<T>::data_->Used() == 0) {
      return PaxVecCommColumn<T>::GetBuffer();
    }

    if (encoder_) {
      // changed streaming encode to blocking encode
      // because we still need store a origin data in `PaxVecCommColumn<T>`
      auto origin_data_buffer = PaxVecCommColumn<T>::data_;

      shared_data_ = new DataBuffer<char>(origin_data_buffer->Used());
      encoder_->SetDataBuffer(shared_data_);
      for (size_t i = 0; i < origin_data_buffer->GetSize(); i++) {
        encoder_->Append((*origin_data_buffer)[i]);
      }
      encoder_->Flush();
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    } else if (compressor_) {
      size_t bound_size =
          compressor_->GetCompressBound(PaxVecCommColumn<T>::data_->Used());
      shared_data_ = new DataBuffer<char>(bound_size);

      size_t c_size = compressor_->Compress(
          shared_data_->Start(), shared_data_->Capacity(),
          PaxVecCommColumn<T>::data_->Start(),
          PaxVecCommColumn<T>::data_->Used(), encoder_options_.compress_level);

      if (compressor_->IsError(c_size)) {
        // log error with `compressor_->ErrorName(c_size)`
        CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError);
      }

      shared_data_->Brush(c_size);
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }

    // no encoding here, fall through
  }

  return PaxVecCommColumn<T>::GetBuffer();
}

template <typename T>
int64 PaxVecEncodingColumn<T>::GetOriginLength() const {
  return encoder_options_.column_encode_type ==
                 ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED
             ? NO_ENCODE_ORIGIN_LEN
             : PaxVecCommColumn<T>::data_->Used();
}

template <typename T>
size_t PaxVecEncodingColumn<T>::PhysicalSize() const {
  if (shared_data_) {
    return shared_data_->Used();
  }

  return PaxVecCommColumn<T>::PhysicalSize();
}

template <typename T>
size_t PaxVecEncodingColumn<T>::GetAlignSize() const {
  if (encoder_options_.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    return PaxColumn::GetAlignSize();
  }

  return PAX_DATA_NO_ALIGN;
}

template <typename T>
ColumnEncoding_Kind PaxVecEncodingColumn<T>::GetDefaultColumnType() {
  return ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  // TODO(jiaqizho): after support DELTA encoding
  // return sizeof(T) >= 4 ? ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2
  //                       :
  //                       ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA;
}

template class PaxVecEncodingColumn<int8>;
template class PaxVecEncodingColumn<int16>;
template class PaxVecEncodingColumn<int32>;
template class PaxVecEncodingColumn<int64>;

PaxVecNonFixedEncodingColumn::PaxVecNonFixedEncodingColumn(
    uint32 capacity, const PaxEncoder::EncodingOption &encoder_options)
    : PaxVecNonFixedColumn(capacity),
      encoder_options_(encoder_options),
      compressor_(nullptr),
      compress_route_(true),
      shared_data_(nullptr) {
  if (encoder_options.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED) {
    encoder_options_.column_encode_type = ColumnEncoding_Kind_COMPRESS_ZSTD;
  }

  PaxColumn::encoded_type_ = encoder_options_.column_encode_type;
  compressor_ = PaxCompressor::CreateBlockCompressor(PaxColumn::encoded_type_);
  if (!compressor_) {
    PaxColumn::encoded_type_ =
        ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED;
  }
}

PaxVecNonFixedEncodingColumn::PaxVecNonFixedEncodingColumn(
    uint32 capacity, const PaxDecoder::DecodingOption &decoding_option)
    : PaxVecNonFixedColumn(capacity),
      decoder_options_(decoding_option),
      compressor_(nullptr),
      compress_route_(false),
      shared_data_(nullptr) {
  Assert(decoder_options_.column_encode_type !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);
  PaxColumn::encoded_type_ = decoder_options_.column_encode_type;
  compressor_ = PaxCompressor::CreateBlockCompressor(PaxColumn::encoded_type_);
}

PaxVecNonFixedEncodingColumn::~PaxVecNonFixedEncodingColumn() {
  delete compressor_;
  delete shared_data_;
}

void PaxVecNonFixedEncodingColumn::Set(DataBuffer<char> *data,
                                       DataBuffer<int32> *offsets,
                                       size_t total_size,
                                       size_t non_null_rows) {
  PaxColumn::non_null_rows_ = non_null_rows;
  if (compressor_) {
    Assert(!compress_route_);

    // still need update origin logic
    delete offsets_;
    offsets_ = offsets;

    if (data->Used() != 0) {
      auto d_size = compressor_->Decompress(
          PaxVecNonFixedColumn::data_->Start(),
          PaxVecNonFixedColumn::data_->Capacity(), data->Start(), data->Used());
      if (compressor_->IsError(d_size)) {
        CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError);
      }
      PaxVecNonFixedColumn::data_->Brush(d_size);
    }

    Assert(!data->IsMemTakeOver());
    delete data;
  } else {
    PaxVecNonFixedColumn::Set(data, offsets_, total_size, non_null_rows);
  }
}

std::pair<char *, size_t> PaxVecNonFixedEncodingColumn::GetBuffer() {
  if (compressor_ && compress_route_) {
    // already compressed
    if (shared_data_) {
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }

    // do compressed
    if (PaxVecNonFixedColumn::data_->Used() == 0) {
      return PaxVecNonFixedColumn::GetBuffer();
    }

    size_t bound_size =
        compressor_->GetCompressBound(PaxVecNonFixedColumn::data_->Used());
    shared_data_ = new DataBuffer<char>(bound_size);

    auto c_size = compressor_->Compress(
        shared_data_->Start(), shared_data_->Capacity(),
        PaxVecNonFixedColumn::data_->Start(),
        PaxVecNonFixedColumn::data_->Used(), encoder_options_.compress_level);

    if (compressor_->IsError(c_size)) {
      // log error with `compressor_->ErrorName(d_size)`
      CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError);
    }

    shared_data_->Brush(c_size);
    return std::make_pair(shared_data_->Start(), shared_data_->Used());
  }

  // no compress or uncompressed
  return PaxVecNonFixedColumn::GetBuffer();
}

int64 PaxVecNonFixedEncodingColumn::GetOriginLength() const {
  return compressor_ ? PaxVecNonFixedColumn::data_->Used()
                     : NO_ENCODE_ORIGIN_LEN;
}

size_t PaxVecNonFixedEncodingColumn::GetAlignSize() const {
  if (encoder_options_.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    return PaxColumn::GetAlignSize();
  }

  return PAX_DATA_NO_ALIGN;
}

}  // namespace pax