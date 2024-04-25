#pragma once
#include "storage/columns/pax_encoding_non_fixed_column.h"

namespace pax {

class PaxBpCharColumn final : public PaxNonFixedEncodingColumn {
 public:
  PaxBpCharColumn(uint32 data_capacity, uint32 length_capacity,
                  const PaxEncoder::EncodingOption &encoder_options)
      : PaxNonFixedEncodingColumn(data_capacity, length_capacity,
                                  encoder_options) {}

  PaxBpCharColumn(uint32 data_capacity, uint32 length_capacity,
                  const PaxDecoder::DecodingOption &decoding_option)
      : PaxNonFixedEncodingColumn(data_capacity, length_capacity,
                                  decoding_option) {}

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override {
    return PaxColumnTypeInMem::kTypeBpChar;
  }
};

}  // namespace pax
