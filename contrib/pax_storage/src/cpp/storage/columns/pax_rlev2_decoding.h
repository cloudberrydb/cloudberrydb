#pragma once

#include <stddef.h>
#include <stdint.h>

#include "comm/cbdb_wrappers.h"
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_encoding_utils.h"
#include "storage/pax_buffer.h"

namespace pax {

template <typename T>
class PaxOrcDecoder final : public PaxDecoder {
 public:
  explicit PaxOrcDecoder(const PaxDecoder::DecodingOption &encoder_options);

  ~PaxOrcDecoder() override;

  PaxDecoder *SetSrcBuffer(char *data, size_t data_len) override;

  PaxDecoder *SetDataBuffer(DataBuffer<char> *result_buffer) override;

  const char *GetBuffer() const override;

  size_t GetBufferSize() const override;

  size_t Next(const char *not_null) override;

  size_t Decoding() override;

  size_t Decoding(const char *not_null, size_t not_null_len) override;

 private:
  uint64 NextShortRepeats(TreatedDataBuffer<int64> *data_buffer, T *data,
                          uint64 offset, const char *not_null);
  uint64 NextDirect(TreatedDataBuffer<int64> *data_buffer, T *data,
                    uint64 offset, const char *not_null);
  uint64 NextPatched(TreatedDataBuffer<int64> *data_buffer, T *data,
                     uint64 offset, const char *not_null);
  uint64 NextDelta(TreatedDataBuffer<int64> *data_buffer, T *data,
                   uint64 offset, const char *not_null);

 private:
  TreatedDataBuffer<int64> *data_buffer_;
  // Used to fill null field
  DataBuffer<int64> *copy_data_buffer_;
  // Used by PATCHED_BASE
  DataBuffer<int64> *unpacked_data_;
  // result buffer
  DataBuffer<char> *result_buffer_;
};

extern template class PaxOrcDecoder<int64>;
extern template class PaxOrcDecoder<int32>;
extern template class PaxOrcDecoder<int16>;
extern template class PaxOrcDecoder<int8>;

#ifdef RUN_GTEST
template <typename T>
void ReadLongs(TreatedDataBuffer<int64> *data_buffer, T *data, uint64 offset,
               uint64 len, uint64 fbs, uint32 *bits_left);
#endif

}  // namespace pax
