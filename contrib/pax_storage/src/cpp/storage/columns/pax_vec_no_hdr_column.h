#pragma once
#include "storage/columns/pax_vec_encoding_column.h"

namespace pax {

class PaxVecNoHdrColumn final : public PaxVecNonFixedEncodingColumn {
 public:
  PaxVecNoHdrColumn(uint32 data_capacity, uint32 length_capacity,
                    const PaxEncoder::EncodingOption &encoder_options)
      : PaxVecNonFixedEncodingColumn(data_capacity, length_capacity,
                                     encoder_options) {}

  PaxVecNoHdrColumn(uint32 data_capacity, uint32 length_capacity,
                    const PaxDecoder::DecodingOption &decoding_option)
      : PaxVecNonFixedEncodingColumn(data_capacity, length_capacity,
                                     decoding_option) {}

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override {
    return PaxColumnTypeInMem::kTypeVecNoHeader;
  }
};

}  // namespace pax
