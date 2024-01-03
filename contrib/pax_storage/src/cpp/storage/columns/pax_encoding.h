#pragma once

#include <stddef.h>
#include <stdint.h>

#include <map>

#include "comm/cbdb_wrappers.h"
#include "storage/columns/pax_encoding_utils.h"
#include "storage/pax_buffer.h"

namespace pax {

class PaxEncoder {
 public:
  struct EncodingOption {
    ColumnEncoding_Kind column_encode_type;
    bool is_sign;
    int compress_level;

    EncodingOption()
        : column_encode_type(
              ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED),
          is_sign(true),
          compress_level(0) {}
  };

 public:
  explicit PaxEncoder(const EncodingOption &encoder_options);

  void SetDataBuffer(DataBuffer<char> *result_buffer);

  virtual ~PaxEncoder() = default;

  virtual void Append(int64 data) = 0;

  virtual void Flush() = 0;

  virtual char *GetBuffer() const;

  virtual size_t GetBufferSize() const;

  /**
   * steaming encoder
   *
   * streaming means it need hold two DataBuffers
   * - one of DataBuffer used to temp save buffer
   * - one of DataBuffer used to keep result
   *
   * compared with the block method, streaming can reduce one memory copy
   */
  static PaxEncoder *CreateStreamingEncoder(
      const EncodingOption &encoder_options);

 protected:
  const EncodingOption &encoder_options_;
  DataBuffer<char> *result_buffer_;
};

}  // namespace pax
