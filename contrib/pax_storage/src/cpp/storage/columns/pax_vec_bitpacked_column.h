#pragma once
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_encoding.h"
#include "storage/columns/pax_vec_encoding_column.h"

namespace pax {

// TODO(gongxun):
// we need to overload the Append implementation to store the boolean type in
// bit-pakced format.
class PaxVecBitPackedColumn final : public PaxVecEncodingColumn<int8> {
 public:
  PaxVecBitPackedColumn(uint32 capacity,
                        const PaxEncoder::EncodingOption &encoding_option)
      : PaxVecEncodingColumn(capacity, encoding_option) {}

  PaxVecBitPackedColumn(uint32 capacity,
                        const PaxDecoder::DecodingOption &decoding_option)
      : PaxVecEncodingColumn(capacity, decoding_option) {}

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const {
    return PaxColumnTypeInMem::kTypeBitPacked;
  }
};
}  // namespace pax
