#pragma once
#include "storage/columns/pax_columns.h"
#include "storage/columns/pax_compress.h"
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_encoding.h"

namespace pax {
class PaxNonFixedEncodingColumn : public PaxNonFixedColumn {
 public:
  PaxNonFixedEncodingColumn(uint32 data_capacity, uint32 lengths_capacity,
                            const PaxEncoder::EncodingOption &encoder_options);

  PaxNonFixedEncodingColumn(uint32 data_capacity, uint32 lengths_capacity,
                            const PaxDecoder::DecodingOption &decoding_option);

  ~PaxNonFixedEncodingColumn() override;

  void Set(DataBuffer<char> *data, DataBuffer<int32> *lengths,
           size_t total_size) override;

  std::pair<char *, size_t> GetBuffer() override;

  std::pair<char *, size_t> GetLengthBuffer() override;

  int64 GetOriginLength() const override;

  size_t GetAlignSize() const override;

  // The reason why `PaxNonFixedEncodingColumn` not override the
  // method `GetRangeBuffer` and `GetNonNullRows` is that
  // `PaxNonFixedEncodingColumn` don't have any streaming encoding, also
  // `shared_data_` will own the same buffer with `PaxNonFixedColumn::data_`.

 protected:
  PaxEncoder::EncodingOption encoder_options_;
  PaxDecoder::DecodingOption decoder_options_;

  PaxCompressor *compressor_;
  bool compress_route_;
  DataBuffer<char> *shared_data_;

  PaxCompressor *lengths_compressor_;
  DataBuffer<char> *shared_lengths_data_;
};

}  // namespace pax
