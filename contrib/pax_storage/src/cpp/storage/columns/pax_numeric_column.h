#pragma once
#include "storage/columns/pax_encoding_non_fixed_column.h"

namespace pax {

class PaxPgNumericColumn final : public PaxNonFixedEncodingColumn {
 public:
  PaxPgNumericColumn(uint32 capacity,
                     const PaxEncoder::EncodingOption &encoder_options)
      : PaxNonFixedEncodingColumn(capacity, encoder_options) {}

  PaxPgNumericColumn(uint32 capacity,
                     const PaxDecoder::DecodingOption &decoding_option)
      : PaxNonFixedEncodingColumn(capacity, decoding_option) {}

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override {
    return PaxColumnTypeInMem::kTypeDecimal;
  }
};

}  // namespace pax
