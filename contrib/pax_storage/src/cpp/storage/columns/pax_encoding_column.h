#pragma once
#include "storage/columns/pax_columns.h"
#include "storage/columns/pax_compress.h"
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_encoding.h"

namespace pax {

template <typename T>
class PaxEncodingColumn : public PaxCommColumn<T> {
 public:
  PaxEncodingColumn(uint32 capacity,
                    const PaxEncoder::EncodingOption &encoding_option);

  PaxEncodingColumn(uint32 capacity,
                    const PaxDecoder::DecodingOption &decoding_option);

  ~PaxEncodingColumn() override;

  void Set(DataBuffer<T> *data) override;

  std::pair<char *, size_t> GetBuffer() override;

  int64 GetOriginLength() const override;

  size_t PhysicalSize() const override;

  size_t GetAlignSize() const override;

 protected:
  void InitEncoder();

  void InitDecoder();

  virtual ColumnEncoding_Kind GetDefaultColumnType();

 protected:
  PaxEncoder::EncodingOption encoder_options_;
  PaxEncoder *encoder_;

  PaxDecoder::DecodingOption decoder_options_;
  PaxDecoder *decoder_;
  DataBuffer<char> *shared_data_;

  PaxCompressor *compressor_;
  bool compress_route_;
};

extern template class PaxEncodingColumn<int8>;
extern template class PaxEncodingColumn<int16>;
extern template class PaxEncodingColumn<int32>;
extern template class PaxEncodingColumn<int64>;

}  // namespace pax
