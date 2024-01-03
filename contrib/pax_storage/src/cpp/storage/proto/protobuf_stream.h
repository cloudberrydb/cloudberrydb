#pragma once
#include <google/protobuf/io/zero_copy_stream.h>

#include <sstream>

#include "storage/pax_buffer.h"

namespace pax {

class BufferedOutputStream : public google::protobuf::io::ZeroCopyOutputStream {
 public:
  explicit BufferedOutputStream(uint64 block_size);

  virtual void Set(DataBuffer<char> *data_buffer);

  bool Next(void **buffer, int *size) override;

  void BackUp(int count) override;

  google::protobuf::int64 ByteCount() const override;

  bool WriteAliasedRaw(const void *data, int size) override;

  bool AllowsAliasing() const override;

  virtual uint64 GetSize() const;

  virtual DataBuffer<char> *GetDataBuffer() const;

  virtual void StartBufferOutRecord();

  virtual size_t EndBufferOutRecord();

  virtual void DirectWrite(char *ptr, size_t size);

 private:
  size_t last_used_ = 0;
  DataBuffer<char> *data_buffer_;
  uint64 block_size_;
};

class SeekableInputStream : public google::protobuf::io::ZeroCopyInputStream {
 public:
  SeekableInputStream(char *data_buffer, uint64 length);

  bool Next(const void **buffer, int *size) override;

  void BackUp(int count) override;

  bool Skip(int count) override;

  google::protobuf::int64 ByteCount() const override;

 private:
  DataBuffer<char> data_buffer_;
};

}  // namespace pax
