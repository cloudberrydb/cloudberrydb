#include "storage/columns/pax_encoding_non_fixed_column.h"

#include "comm/pax_memory.h"
#include "storage/pax_defined.h"

namespace pax {

PaxNonFixedEncodingColumn::PaxNonFixedEncodingColumn(
    uint32 data_capacity, uint32 lengths_capacity,
    const PaxEncoder::EncodingOption &encoder_options)
    : PaxNonFixedColumn(data_capacity, lengths_capacity),
      encoder_options_(encoder_options),
      compressor_(nullptr),
      compress_route_(true),
      shared_data_(nullptr),
      lengths_compressor_(nullptr),
      shared_lengths_data_(nullptr) {
  if (encoder_options.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED) {
    encoder_options_.column_encode_type = ColumnEncoding_Kind_COMPRESS_ZSTD;
  }
  PaxColumn::SetEncodeType(encoder_options_.column_encode_type);
  PaxColumn::SetCompressLevel(encoder_options_.compress_level);

  compressor_ =
      PaxCompressor::CreateBlockCompressor(PaxColumn::GetEncodingType());
  if (!compressor_) {
    PaxColumn::SetEncodeType(
        ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED);
    PaxColumn::SetCompressLevel(0);
  }

  Assert(encoder_options_.lengths_encode_type !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);
  lengths_compressor_ = PaxCompressor::CreateBlockCompressor(
      encoder_options_.lengths_encode_type);
  SetLengthsEncodeType(encoder_options_.lengths_encode_type);
  SetLengthsCompressLevel(encoder_options_.lengths_compress_level);
}

PaxNonFixedEncodingColumn::PaxNonFixedEncodingColumn(
    uint32 data_capacity, uint32 lengths_capacity,
    const PaxDecoder::DecodingOption &decoding_option)
    : PaxNonFixedColumn(data_capacity, lengths_capacity),
      decoder_options_(decoding_option),
      compressor_(nullptr),
      compress_route_(false),
      shared_data_(nullptr),
      lengths_compressor_(nullptr),
      shared_lengths_data_(nullptr) {
  Assert(decoder_options_.column_encode_type !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);
  PaxColumn::SetEncodeType(decoder_options_.column_encode_type);
  PaxColumn::SetCompressLevel(decoder_options_.compress_level);
  compressor_ =
      PaxCompressor::CreateBlockCompressor(PaxColumn::GetEncodingType());

  Assert(decoder_options_.lengths_encode_type !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);
  lengths_compressor_ = PaxCompressor::CreateBlockCompressor(
      decoder_options_.lengths_encode_type);
  SetLengthsEncodeType(decoder_options_.lengths_encode_type);
  SetLengthsCompressLevel(decoder_options_.lengths_compress_level);
}

PaxNonFixedEncodingColumn::~PaxNonFixedEncodingColumn() {
  PAX_DELETE(compressor_);
  PAX_DELETE(shared_data_);
  PAX_DELETE(lengths_compressor_);
  PAX_DELETE(shared_lengths_data_);
}

void PaxNonFixedEncodingColumn::Set(DataBuffer<char> *data,
                                    DataBuffer<int32> *lengths,
                                    size_t total_size) {
  Assert(data && lengths);
  auto data_decompress = [&]() {
    Assert(!compress_route_);
    Assert(compressor_);

    if (data->Used() != 0) {
      auto d_size = compressor_->Decompress(
          PaxNonFixedColumn::data_->Start(),
          PaxNonFixedColumn::data_->Capacity(), data->Start(), data->Used());
      if (compressor_->IsError(d_size)) {
        // log error with `compressor_->ErrorName(d_size)`
        CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError);
      }
      PaxNonFixedColumn::data_->Brush(d_size);
    }

    PAX_DELETE(data);
  };

  auto lengths_decompress = [&]() {
    Assert(!compress_route_);
    Assert(lengths_compressor_);

    if (lengths->Used() != 0) {
      auto d_size = lengths_compressor_->Decompress(
          PaxNonFixedColumn::lengths_->Start(),
          PaxNonFixedColumn::lengths_->Capacity(), lengths->Start(),
          lengths->Used());
      if (lengths_compressor_->IsError(d_size)) {
        CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError);
      }
      PaxNonFixedColumn::lengths_->Brush(d_size);
    }

    PAX_DELETE(lengths);

    BuildOffsets();
  };

  if (compressor_ && lengths_compressor_) {
    data_decompress();
    lengths_decompress();

    estimated_size_ = total_size;
  } else if (compressor_ && !lengths_compressor_) {
    data_decompress();

    PAX_DELETE(lengths_);
    lengths_ = lengths;
    BuildOffsets();

    estimated_size_ = total_size;
  } else if (!compressor_ && lengths_compressor_) {
    PAX_DELETE(data_);
    data_ = data;

    lengths_decompress();

    estimated_size_ = total_size;
  } else {  // (!compressor_ && !lengths_compressor_)
    PaxNonFixedColumn::Set(data, lengths, total_size);
  }
}

std::pair<char *, size_t> PaxNonFixedEncodingColumn::GetBuffer() {
  if (compressor_ && compress_route_) {
    // already compressed
    if (shared_data_) {
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }

    // do compressed
    if (PaxNonFixedColumn::data_->Used() == 0) {
      return PaxNonFixedColumn::GetBuffer();
    }

    size_t bound_size =
        compressor_->GetCompressBound(PaxNonFixedColumn::data_->Used());
    shared_data_ = PAX_NEW<DataBuffer<char>>(bound_size);

    auto c_size = compressor_->Compress(
        shared_data_->Start(), shared_data_->Capacity(),
        PaxNonFixedColumn::data_->Start(), PaxNonFixedColumn::data_->Used(),
        encoder_options_.compress_level);

    if (compressor_->IsError(c_size)) {
      // log error with `compressor_->ErrorName(d_size)`
      CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError);
    }

    shared_data_->Brush(c_size);
    return std::make_pair(shared_data_->Start(), shared_data_->Used());
  }

  // no compress or uncompressed
  return PaxNonFixedColumn::GetBuffer();
}

std::pair<char *, size_t> PaxNonFixedEncodingColumn::GetLengthBuffer() {
  if (lengths_compressor_) {
    if (shared_lengths_data_) {
      return std::make_pair(shared_lengths_data_->Start(),
                            shared_lengths_data_->Used());
    }

    if (PaxNonFixedColumn::lengths_->Used() == 0) {
      return PaxNonFixedColumn::GetLengthBuffer();
    }

    size_t bound_size = lengths_compressor_->GetCompressBound(
        PaxNonFixedColumn::lengths_->Used());
    shared_lengths_data_ = PAX_NEW<DataBuffer<char>>(bound_size);

    auto d_size = lengths_compressor_->Compress(
        shared_lengths_data_->Start(), shared_lengths_data_->Capacity(),
        PaxNonFixedColumn::lengths_->Start(),
        PaxNonFixedColumn::lengths_->Used(), encoder_options_.compress_level);

    if (lengths_compressor_->IsError(d_size)) {
      CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError);
    }

    shared_lengths_data_->Brush(d_size);
    return std::make_pair(shared_lengths_data_->Start(),
                          shared_lengths_data_->Used());
  }

  // no compress or uncompressed
  return PaxNonFixedColumn::GetLengthBuffer();
}

int64 PaxNonFixedEncodingColumn::GetOriginLength() const {
  return PaxNonFixedColumn::data_->Used();
}

size_t PaxNonFixedEncodingColumn::GetAlignSize() const {
  if (encoder_options_.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    return PaxColumn::GetAlignSize();
  }

  return PAX_DATA_NO_ALIGN;
}

}  // namespace pax
