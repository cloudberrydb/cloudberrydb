#pragma once
#include "storage/columns/pax_vec_encoding_column.h"

namespace pax {

class PaxVecBpCharColumn final : public PaxVecNonFixedEncodingColumn {
 public:
  PaxVecBpCharColumn(uint32 capacity, uint32 lengths_capacity,
                     const PaxEncoder::EncodingOption &encoder_options);

  PaxVecBpCharColumn(uint32 capacity, uint32 lengths_capacity,
                     const PaxDecoder::DecodingOption &decoding_option);

  ~PaxVecBpCharColumn();

  void Append(char *buffer, size_t size) override;

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override;

  std::pair<char *, size_t> GetBuffer(size_t position) override;

 private:
  int64 number_of_char_;
  std::vector<char *> bpchar_holder_;
};

}  // namespace pax
