#pragma once

#include "storage/columns/pax_compress.h"
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_encoding.h"
#include "storage/columns/pax_vec_column.h"

namespace pax {

template <typename T>
class PaxVecEncodingColumn : public PaxVecCommColumn<T> {
 public:
  PaxVecEncodingColumn(uint32 capacity,
                       const PaxEncoder::EncodingOption &encoding_option);

  PaxVecEncodingColumn(uint32 capacity,
                       const PaxDecoder::DecodingOption &decoding_option);

  ~PaxVecEncodingColumn() override;

  void Set(DataBuffer<T> *data, size_t non_null_rows) override;

  std::pair<char *, size_t> GetBuffer() override;

  int64 GetOriginLength() const override;

  size_t PhysicalSize() const override;

  size_t GetAlignSize() const override;

 protected:
  void InitEncoder();

  void InitDecoder();

  ColumnEncoding_Kind GetDefaultColumnType();

 protected:
  PaxEncoder::EncodingOption encoder_options_;
  PaxEncoder *encoder_;

  PaxDecoder::DecodingOption decoder_options_;
  PaxDecoder *decoder_;
  DataBuffer<char> *shared_data_;

  PaxCompressor *compressor_;
  bool compress_route_;
};

extern template class PaxVecEncodingColumn<int8>;
extern template class PaxVecEncodingColumn<int16>;
extern template class PaxVecEncodingColumn<int32>;
extern template class PaxVecEncodingColumn<int64>;

class PaxVecNonFixedEncodingColumn : public PaxVecNonFixedColumn {
 public:
  PaxVecNonFixedEncodingColumn(
      uint32 capacity, const PaxEncoder::EncodingOption &encoder_options);

  PaxVecNonFixedEncodingColumn(
      uint32 capacity, const PaxDecoder::DecodingOption &decoding_option);

  ~PaxVecNonFixedEncodingColumn() override;

  void Set(DataBuffer<char> *data, DataBuffer<int32> *offsets,
           size_t total_size, size_t non_null_rows) override;

  std::pair<char *, size_t> GetBuffer() override;

  int64 GetOriginLength() const override;

  size_t GetAlignSize() const override;

 protected:
  PaxEncoder::EncodingOption encoder_options_;
  PaxDecoder::DecodingOption decoder_options_;

  PaxCompressor *compressor_;
  bool compress_route_;
  DataBuffer<char> *shared_data_;
};

}  // namespace pax
