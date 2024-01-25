#include "storage/columns/pax_decoding.h"

#include "comm/pax_memory.h"
#include "storage/columns/pax_rlev2_decoding.h"

namespace pax {

template <typename T>
PaxDecoder *PaxDecoder::CreateDecoder(const DecodingOption &decoder_options) {
  PaxDecoder *decoder = nullptr;
  switch (decoder_options.column_encode_type) {
    case ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED: {
      // do nothing
      break;
    }
    case ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2: {
      decoder = PAX_NEW<PaxOrcDecoder<T>>(decoder_options);
      break;
    }
    case ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA: {
      /// TODO(jiaqizho) support it
      break;
    }
    case ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED: {
      CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
    }
    case ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZSTD:
    case ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZLIB:
    default: {
      // do nothing
    }
  }

  return decoder;
}

template PaxDecoder *PaxDecoder::CreateDecoder<int64>(const DecodingOption &);
template PaxDecoder *PaxDecoder::CreateDecoder<int32>(const DecodingOption &);
template PaxDecoder *PaxDecoder::CreateDecoder<int16>(const DecodingOption &);
template PaxDecoder *PaxDecoder::CreateDecoder<int8>(const DecodingOption &);

PaxDecoder::PaxDecoder(const DecodingOption &decoder_options)
    : decoder_options_(decoder_options) {}

}  // namespace pax
