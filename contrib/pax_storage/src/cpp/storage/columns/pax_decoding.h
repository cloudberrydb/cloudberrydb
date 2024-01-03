#pragma once

#include <stddef.h>
#include <stdint.h>

#include "comm/cbdb_wrappers.h"
#include "storage/columns/pax_encoding_utils.h"
#include "storage/pax_buffer.h"

namespace pax {

class PaxDecoder {
 public:
  struct DecodingOption {
    ColumnEncoding_Kind column_encode_type;
    bool is_sign;

    DecodingOption()
        : column_encode_type(
              ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED),
          is_sign(true) {}
  };

  explicit PaxDecoder(const DecodingOption &decoder_options);

  virtual ~PaxDecoder() = default;

  virtual PaxDecoder *SetSrcBuffer(char *data, size_t data_len) = 0;

  virtual PaxDecoder *SetDataBuffer(DataBuffer<char> *result_buffer) = 0;

  virtual size_t Next(const char *not_null) = 0;

  virtual size_t Decoding() = 0;

  virtual size_t Decoding(const char *not_null, size_t not_null_len) = 0;

  virtual const char *GetBuffer() const = 0;

  virtual size_t GetBufferSize() const = 0;

  template <typename T>
  static PaxDecoder *CreateDecoder(const DecodingOption &decoder_options);

 protected:
  const DecodingOption &decoder_options_;
};

extern template PaxDecoder *PaxDecoder::CreateDecoder<int64>(
    const DecodingOption &);
extern template PaxDecoder *PaxDecoder::CreateDecoder<int32>(
    const DecodingOption &);
extern template PaxDecoder *PaxDecoder::CreateDecoder<int16>(
    const DecodingOption &);
extern template PaxDecoder *PaxDecoder::CreateDecoder<int8>(
    const DecodingOption &);

}  // namespace pax
