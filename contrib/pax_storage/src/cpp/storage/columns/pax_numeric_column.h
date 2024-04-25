#pragma once
#include "storage/columns/pax_encoding_non_fixed_column.h"

namespace pax {

class PaxPgNumericColumn final : public PaxNonFixedEncodingColumn {
 public:
  PaxPgNumericColumn(uint32 capacity, uint32 lengths_capacity,
                     const PaxEncoder::EncodingOption &encoder_options)
      : PaxNonFixedEncodingColumn(capacity, lengths_capacity, encoder_options) {
  }

  PaxPgNumericColumn(uint32 capacity, uint32 lengths_capacity,
                     const PaxDecoder::DecodingOption &decoding_option)
      : PaxNonFixedEncodingColumn(capacity, lengths_capacity, decoding_option) {
  }

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override {
    return PaxColumnTypeInMem::kTypeDecimal;
  }
};

}  // namespace pax
