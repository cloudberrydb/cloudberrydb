#pragma once
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_encoding.h"
#include "storage/columns/pax_vec_encoding_column.h"

namespace pax {

class PaxVecBitPackedColumn final : public PaxVecEncodingColumn<int8> {
 public:
  PaxVecBitPackedColumn(uint32 capacity,
                        const PaxEncoder::EncodingOption &encoding_option);

  PaxVecBitPackedColumn(uint32 capacity,
                        const PaxDecoder::DecodingOption &decoding_option);

  void Set(DataBuffer<int8> *data, size_t non_null_rows) override;

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const;

  void AppendNull() override;

  void Append(char *buffer, size_t size) override;

  std::pair<char *, size_t> GetBuffer(size_t position) override;

 private:
  void CheckExpandBitmap();

 private:
  BitmapRaw<uint8> bitmap_raw_;
  DataBuffer<bool> *flat_buffer_;
};
}  // namespace pax
