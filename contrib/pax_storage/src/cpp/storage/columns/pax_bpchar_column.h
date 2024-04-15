#pragma once
#include "storage/columns/pax_encoding_non_fixed_column.h"

namespace pax {

class PaxBpCharColumn final : public PaxNonFixedEncodingColumn {
 public:
  PaxBpCharColumn(uint32 capacity,
                  const PaxEncoder::EncodingOption &encoder_options)
      : PaxNonFixedEncodingColumn(capacity, encoder_options) {}

  PaxBpCharColumn(uint32 capacity,
                  const PaxDecoder::DecodingOption &decoding_option)
      : PaxNonFixedEncodingColumn(capacity, decoding_option) {}

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override {
    return PaxColumnTypeInMem::kTypeBpChar;
  }
};

}  // namespace pax
