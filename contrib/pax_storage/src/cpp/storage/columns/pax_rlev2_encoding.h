#pragma once

#include <stddef.h>
#include <stdint.h>

#include "comm/cbdb_wrappers.h"
#include "storage/columns/pax_encoding.h"
#include "storage/columns/pax_encoding_utils.h"
#include "storage/pax_buffer.h"

namespace pax {

class PaxOrcEncoder final : public PaxEncoder {
 public:
  explicit PaxOrcEncoder(const EncodingOption &encoder_options);

  ~PaxOrcEncoder() override;

  void Append(int64 data) override;

  void Flush() override;

 private:
  struct EncoderContext {
    bool is_sign;

    // repeat lengths
    uint16 fixed_len;

    // non-repeat lengths
    uint16 var_len;

    int64 prev_delta;
    int64 current_delta;

    EncoderContext();
    ~EncoderContext();

    struct DeltaContext;
    struct DirectContext;
    struct PatchBaseContext;

    struct DeltaContext *delta_ctx;
    struct DirectContext *direct_ctx;
    struct PatchBaseContext *pb_ctx;

    inline void ResetDirectCtx() const;
    inline void ResetDeltaCtx() const;
    inline void ResetPbCtx() const;

   private:
    char *internal_buffer_;
  };

  enum EncoderStatus {
    // current encoder have been flushed or no init
    kInvalid = 0,
    // no elements in buffer, accept the first element
    kInit,
    // 1 element in buffer, accept the second element
    kTwoElements,
    // at lease 2 elements in buffer
    kUntreated,
    // at lease `ORC_MIN_REPEAT` repeating elements change to non-repeating
    // elements
    kUntreatedDiscontinuous,
    // non-repeating elements change to `ORC_MIN_REPEAT` repeating elements
    kTreatPrevBuffer,
    // flush all buffer
    kFlush,
    // treat the non-repeating buffer which is before repeating datas
    kDetermineFlushPrevBuffer,
    // treat the buffer which belongs to the Short-Repeat rule
    kTreatShortRepeat,
    // treat the buffer which can deal with other types
    kTreatDirect,
    // treat the buffer which belongs to the Patched-Base rule
    kTreatPatchedBase,
    // treat the buffer which belongs to the Delta rule
    kTreatDelta,
    // done with treat or flush buffer
    kTreatDone,
    // all done, will change to invalid
    kFinish,
  };

 private:
  void AppendInternal(int64 data, bool is_flush);

  void AppendData(int64 data);

  void SwitchStatusTo(EncoderStatus new_status);

  void TreatShortRepeat();

  void TreatDirect();

  void PreparePatchedBlob();

  void TreatPatchedBase();

  bool TreatDelta();

 private:
  EncoderContext encoder_context_;
  UntreatedDataBuffer<int64> *data_buffer_;
  DataBuffer<int64> *zigzag_buffer_;
  EncoderStatus status_;
};

#ifdef RUN_GTEST
void WriteLongs(DataBuffer<char> *data_buffer, const int64 *input,
                uint32 offset, size_t len, uint32 bits);
#endif

}  // namespace pax
