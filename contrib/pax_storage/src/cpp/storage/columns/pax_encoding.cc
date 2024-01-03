#include "storage/columns/pax_encoding.h"

#include <utility>

#include "storage/columns/pax_rlev2_encoding.h"

namespace pax {

PaxEncoder *PaxEncoder::CreateStreamingEncoder(
    const EncodingOption &encoder_options) {
  PaxEncoder *encoder = nullptr;
  switch (encoder_options.column_encode_type) {
    case ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2: {
      encoder = new PaxOrcEncoder(std::move(encoder_options));
      break;
    }
    case ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA: {
      // TODO(jiaqizho): support direct delta encoding
      // not support yet, then direct return a nullptr(means no encoding)
      break;
    }
    case ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED: {
      CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
    }
    // two cases here:
    //  - `encoded type` is not a encoding type.
    //  - `encoded type` is the no_encoding type.
    default: {
      // do nothing
    }
  }

  return encoder;
}

PaxEncoder::PaxEncoder(const EncodingOption &encoder_options)
    : encoder_options_(encoder_options), result_buffer_(nullptr) {}

void PaxEncoder::SetDataBuffer(DataBuffer<char> *result_buffer) {
  Assert(!result_buffer_ && result_buffer);
  Assert(result_buffer->IsMemTakeOver());
  result_buffer_ = result_buffer;
}

char *PaxEncoder::GetBuffer() const { return result_buffer_->GetBuffer(); }

size_t PaxEncoder::GetBufferSize() const { return result_buffer_->Used(); }

}  // namespace pax
