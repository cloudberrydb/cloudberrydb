#include "storage/columns/pax_encoding_non_fixed_column.h"

#include "comm/pax_memory.h"
#include "storage/pax_defined.h"

namespace pax {

PaxNonFixedEncodingColumn::PaxNonFixedEncodingColumn(
    uint32 capacity, const PaxEncoder::EncodingOption &encoder_options)
    : PaxNonFixedColumn(capacity),
      encoder_options_(encoder_options),
      compressor_(nullptr),
      compress_route_(true),
      shared_data_(nullptr) {
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
}

PaxNonFixedEncodingColumn::PaxNonFixedEncodingColumn(
    uint32 capacity, const PaxDecoder::DecodingOption &decoding_option)
    : PaxNonFixedColumn(capacity),
      decoder_options_(decoding_option),
      compressor_(nullptr),
      compress_route_(false),
      shared_data_(nullptr) {
  Assert(decoder_options_.column_encode_type !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);
  PaxColumn::SetEncodeType(decoder_options_.column_encode_type);
  PaxColumn::SetCompressLevel(decoder_options_.compress_level);
  compressor_ =
      PaxCompressor::CreateBlockCompressor(PaxColumn::GetEncodingType());
}

PaxNonFixedEncodingColumn::~PaxNonFixedEncodingColumn() {
  PAX_DELETE(compressor_);
  PAX_DELETE(shared_data_);
}

void PaxNonFixedEncodingColumn::Set(DataBuffer<char> *data,
                                    DataBuffer<int32> *lengths,
                                    size_t total_size) {
  if (compressor_) {
    Assert(!compress_route_);

    // still need update origin logic
    PAX_DELETE(lengths_);
    estimated_size_ = total_size;
    lengths_ = lengths;
    BuildOffsets();

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
  } else {
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

int64 PaxNonFixedEncodingColumn::GetOriginLength() const {
  return compressor_ ? PaxNonFixedColumn::data_->Used() : NO_ENCODE_ORIGIN_LEN;
}

size_t PaxNonFixedEncodingColumn::GetAlignSize() const {
  if (encoder_options_.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    return PaxColumn::GetAlignSize();
  }

  return PAX_DATA_NO_ALIGN;
}

}  // namespace pax
