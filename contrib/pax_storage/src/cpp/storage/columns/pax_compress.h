#pragma once

#include <stddef.h>

#include <string>

#include "storage/columns/pax_encoding_utils.h"

namespace pax {

class PaxCompressor {
 public:
  enum PaxCompressorType {
    kTypeInvalid = 1,    //
    kTypeStreaming = 2,  //
    kTypeBlock = 3
  };

  PaxCompressor() = default;

  virtual ~PaxCompressor() = default;

  virtual bool ShouldAlignBuffer() const = 0;

  virtual PaxCompressorType GetCompressorType() const = 0;

  virtual size_t GetCompressBound(size_t src_len) = 0;

  virtual size_t Compress(void *dst_buff, size_t dst_cap, void *src_buff,
                          size_t src_len, int lvl) = 0;

  virtual size_t GetDecompressSize(const void *src_buff, size_t src_len) = 0;

  virtual size_t Decompress(void *dst_buff, size_t dst_len, void *src_buff,
                            size_t src_len) = 0;

  virtual bool IsError(size_t code) = 0;

  virtual const char *ErrorName(size_t code) = 0;

  /**
   * block compress
   *
   * it has similar interface with `CreateStreamingEncoder`
   * but the timing of compression/decompression method calls is different from
   * encoding/decoding.
   */
  static PaxCompressor *CreateBlockCompressor(ColumnEncoding_Kind kind);
};

class PaxZSTDCompressor final : public PaxCompressor {
 public:
  PaxZSTDCompressor() = default;

  ~PaxZSTDCompressor() override = default;

  PaxCompressor::PaxCompressorType GetCompressorType() const override;

  bool ShouldAlignBuffer() const override;

  size_t GetCompressBound(size_t src_len) override;

  size_t Compress(void *dst_buff, size_t dst_cap, void *src_buff,
                  size_t src_len, int lvl) override;

  size_t GetDecompressSize(const void *src_buff, size_t src_len) override;

  size_t Decompress(void *dst_buff, size_t dst_len, void *src_buff,
                    size_t src_len) override;

  bool IsError(size_t code) override;

  const char *ErrorName(size_t code) override;
};

class PaxZlibCompressor final : public PaxCompressor {
 public:
  PaxZlibCompressor() = default;

  ~PaxZlibCompressor() override = default;

  PaxCompressor::PaxCompressorType GetCompressorType() const override;

  bool ShouldAlignBuffer() const override;

  size_t GetCompressBound(size_t src_len) override;

  size_t Compress(void *dst_buff, size_t dst_cap, void *src_buff,
                  size_t src_len, int lvl) override;

  size_t GetDecompressSize(const void *src_buff, size_t src_len) override;

  size_t Decompress(void *dst_buff, size_t dst_cap, void *src_buff,
                    size_t src_len) override;

  bool IsError(size_t code) override;

  const char *ErrorName(size_t code) override;

 private:
  std::string err_msg_;
};

}  // namespace pax
