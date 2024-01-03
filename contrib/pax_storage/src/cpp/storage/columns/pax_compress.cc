#include "storage/columns/pax_compress.h"

#include "comm/cbdb_wrappers.h"
#include "zlib.h"  // NOLINT
#include "zstd.h"  // NOLINT

namespace pax {

PaxCompressor *PaxCompressor::CreateBlockCompressor(
    const ColumnEncoding_Kind kind) {
  PaxCompressor *compressor = nullptr;
  switch (kind) {
    case ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZSTD: {
      compressor = new PaxZSTDCompressor();
      break;
    }
    case ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZLIB: {
      compressor = new PaxZlibCompressor();
      break;
    }
    case ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED: {
      CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
    }
    // two cases here:
    //  - `encoded type` is not a compress type.
    //  - `encoded type` is the no_encoding type.
    default: {
      // do nothing
    }
  }

  return compressor;
}

PaxCompressor::PaxCompressorType PaxZSTDCompressor::GetCompressorType() const {
  return PaxCompressorType::kTypeBlock;
}

bool PaxZSTDCompressor::ShouldAlignBuffer() const { return false; }

size_t PaxZSTDCompressor::GetCompressBound(size_t src_len) {
  Assert(src_len > 0);
  return ZSTD_compressBound(src_len);
}

size_t PaxZSTDCompressor::Compress(void *dst_buff, size_t dst_cap,
                                   void *src_buff, size_t src_len, int lvl) {
  Assert(dst_buff);
  Assert(dst_cap > 0);
  Assert(src_buff);
  Assert(src_len > 0);

  return ZSTD_compress(dst_buff, dst_cap, src_buff, src_len, lvl);
}

size_t PaxZSTDCompressor::GetDecompressSize(const void *src_buff,
                                            size_t src_len) {
  Assert(src_buff);
  Assert(src_len > 0);

  return ZSTD_getFrameContentSize(src_buff, src_len);
}

size_t PaxZSTDCompressor::Decompress(void *dst_buff, size_t dst_len,
                                     void *src_buff, size_t src_len) {
  Assert(dst_buff);
  Assert(dst_len > 0);
  Assert(src_buff);
  Assert(src_len > 0);

  return ZSTD_decompress(dst_buff, dst_len, src_buff, src_len);
}

bool PaxZSTDCompressor::IsError(size_t code) { return ZSTD_isError(code); }

const char *PaxZSTDCompressor::ErrorName(size_t code) {
  return ZSTD_getErrorName(code);
}

PaxCompressor::PaxCompressorType PaxZlibCompressor::GetCompressorType() const {
  return PaxCompressorType::kTypeStreaming;
}

bool PaxZlibCompressor::ShouldAlignBuffer() const { return false; }

size_t PaxZlibCompressor::GetCompressBound(size_t src_len) {
  return compressBound(src_len);
}

size_t PaxZlibCompressor::Compress(void *dst_buff, size_t dst_cap,
                                   void *src_buff, size_t src_len, int lvl) {
  z_stream c_stream; /* compression stream */
  int err;

  c_stream.zalloc = reinterpret_cast<alloc_func>(0);
  c_stream.zfree = reinterpret_cast<free_func>(0);
  c_stream.opaque = reinterpret_cast<voidpf>(0);

  err = deflateInit(&c_stream, lvl);
  if (err != Z_OK) {
    goto error;
  }

  c_stream.next_in = reinterpret_cast<Bytef *>(src_buff);
  c_stream.next_out = reinterpret_cast<Bytef *>(dst_buff);

  while (c_stream.total_in != src_len && c_stream.total_out < dst_cap) {
    /* force small buffers */
    c_stream.avail_in = c_stream.avail_out = 1;
    err = deflate(&c_stream, Z_NO_FLUSH);
    if (err != Z_OK) {
      goto error;
    }
  }

  while (true) {
    c_stream.avail_out = 1;
    err = deflate(&c_stream, Z_FINISH);
    if (err == Z_STREAM_END) break;
    if (err != Z_OK) {
      goto error;
    }
  }

  err = deflateEnd(&c_stream);
  if (err != Z_OK) {
    goto error;
  }

  return c_stream.total_out;
error:
  err_msg_ = c_stream.msg;
  return err;
}

size_t PaxZlibCompressor::GetDecompressSize(const void * /*src_buff*/,
                                            size_t /*src_len*/) {
  return -1;
}

size_t PaxZlibCompressor::Decompress(void *dst_buff, size_t dst_cap,
                                     void *src_buff, size_t src_len) {
  int err;
  z_stream d_stream; /* decompression stream */

  d_stream.zalloc = reinterpret_cast<alloc_func>(0);
  d_stream.zfree = reinterpret_cast<free_func>(0);
  d_stream.opaque = reinterpret_cast<voidpf>(0);

  d_stream.next_in = reinterpret_cast<Bytef *>(src_buff);
  d_stream.avail_in = 0;
  d_stream.next_out = reinterpret_cast<Bytef *>(dst_buff);

  err = inflateInit(&d_stream);
  if (err != Z_OK) {
    goto error;
  }

  while (d_stream.total_out < dst_cap && d_stream.total_in < src_len) {
    d_stream.avail_in = d_stream.avail_out = 1; /* force small buffers */
    err = inflate(&d_stream, Z_NO_FLUSH);
    if (err == Z_STREAM_END) break;
    if (err != Z_OK) {
      goto error;
    }
  }

  err = inflateEnd(&d_stream);
  if (err != Z_OK) {
    goto error;
  }

  return d_stream.total_out;
error:
  err_msg_ = d_stream.msg;
  return err;
}

bool PaxZlibCompressor::IsError(size_t code) {
  return code != Z_OK && !err_msg_.empty();
}

const char *PaxZlibCompressor::ErrorName(size_t /*code*/) {
  return err_msg_.c_str();
}

}  // namespace pax
