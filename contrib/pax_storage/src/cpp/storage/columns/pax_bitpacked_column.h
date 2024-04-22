#pragma once
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_encoding.h"
#include "storage/columns/pax_encoding_column.h"

namespace pax {

class PaxBitPackedColumn final : public PaxEncodingColumn<int8> {
 public:
  PaxBitPackedColumn(uint32 capacity,
                     const PaxEncoder::EncodingOption &encoder_options)
      : PaxEncodingColumn(capacity, encoder_options) {}

  PaxBitPackedColumn(uint32 capacity,
                     const PaxDecoder::DecodingOption &decoding_options)
      : PaxEncodingColumn(capacity, decoding_options) {}

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override {
    return PaxColumnTypeInMem::kTypeBitPacked;
  }
};

}  // namespace pax
