#pragma once
#include "storage/columns/pax_encoding_column.h"

namespace pax {

class PaxShortNumericColumn final : public PaxEncodingColumn<int8> {
 public:
  PaxShortNumericColumn(uint32 capacity,
                        const PaxEncoder::EncodingOption &encoding_option);

  PaxShortNumericColumn(uint32 capacity,
                        const PaxDecoder::DecodingOption &decoding_option);

  void Set(DataBuffer<int8> *data, size_t non_null_rows);

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override;

  ~PaxShortNumericColumn() override;

  void Append(char *buffer, size_t size) override;

  std::pair<char *, size_t> GetBuffer(size_t position) override;

  std::pair<char *, size_t> GetRangeBuffer(size_t start_pos,
                                           size_t len) override;

  DataBuffer<int8> *GetDataBuffer();

  int32 GetTypeLength() const override;

 private:
  ColumnEncoding_Kind GetDefaultColumnType() override;

 private:
  bool already_combined_;
  uint32 width_;

  std::vector<Numeric> numeric_holder_;
};

}  // namespace pax