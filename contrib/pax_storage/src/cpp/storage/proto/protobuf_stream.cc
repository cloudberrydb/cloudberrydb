#include "storage/proto/protobuf_stream.h"

#include "exceptions/CException.h"

namespace pax {

BufferedOutputStream::BufferedOutputStream(uint64 block_size)
    : data_buffer_(nullptr), block_size_(block_size) {}

void BufferedOutputStream::Set(DataBuffer<char> *data_buffer) {
  Assert(data_buffer && !data_buffer_);
  data_buffer_ = data_buffer;
}

bool BufferedOutputStream::Next(void **buffer, int *size) {
  Assert(data_buffer_);
  uint64 old_capacity = data_buffer_->Capacity();
  uint64 new_capacity = data_buffer_->Capacity();

  while (new_capacity < data_buffer_->Used() + block_size_) {
    if (new_capacity == 0) {
      new_capacity += block_size_;
    } else {
      new_capacity += data_buffer_->Capacity();
    }
  }

  if (new_capacity == old_capacity) {  // No resize
    *size = block_size_;
  } else {
    data_buffer_->ReSize(new_capacity);
    *size = static_cast<int>(new_capacity - old_capacity);
  }

  *buffer = data_buffer_->GetBuffer() + data_buffer_->Used();

  data_buffer_->Brush(*size);
  return true;
}

void BufferedOutputStream::BackUp(int count) {
  if (count >= 0) {
    Assert(data_buffer_);
    if (static_cast<size_t>(count) > data_buffer_->Used()) {
      CBDB_RAISE(cbdb::CException::ExType::kExTypeIOError);
    }
    data_buffer_->BrushBack(count);
  }
}

google::protobuf::int64 BufferedOutputStream::ByteCount() const {
  Assert(data_buffer_);
  return static_cast<google::protobuf::int64>(data_buffer_->Used());
}

bool BufferedOutputStream::WriteAliasedRaw(const void * /*data*/,
                                           int /*size*/) {
  return false;
}

bool BufferedOutputStream::AllowsAliasing() const { return false; }

uint64 BufferedOutputStream::GetSize() const {
  Assert(data_buffer_);
  return data_buffer_->Used();
}

DataBuffer<char> *BufferedOutputStream::GetDataBuffer() const {
  return data_buffer_;
}

void BufferedOutputStream::StartBufferOutRecord() {
  Assert(data_buffer_);
  last_used_ = data_buffer_->Used();
}

size_t BufferedOutputStream::EndBufferOutRecord() {
  Assert(data_buffer_);
  return data_buffer_->Used() - last_used_;
}

void BufferedOutputStream::DirectWrite(char *ptr, size_t size) {
  Assert(data_buffer_);
  if (data_buffer_->Available() < size) {
    data_buffer_->ReSize(data_buffer_->Capacity() + size);
  }
  data_buffer_->Write(ptr, size);
  data_buffer_->Brush(size);
}

SeekableInputStream::SeekableInputStream(char *data_buffer, uint64 length)
    : data_buffer_(data_buffer, length, true, false) {}

bool SeekableInputStream::Next(const void **buffer, int *size) {
  if (data_buffer_.Available() > 0) {
    *buffer = data_buffer_.Position();
    *size = static_cast<int>(data_buffer_.Available());
    data_buffer_.BrushAll();
    return true;
  }
  *size = 0;
  return false;
}

void SeekableInputStream::BackUp(int count) {
  if (count >= 0) {
    if (static_cast<size_t>(count) > data_buffer_.Used()) {
      CBDB_RAISE(cbdb::CException::ExType::kExTypeIOError);
    }
    data_buffer_.BrushBack(count);
  }
}

bool SeekableInputStream::Skip(int count) {
  if (count >= 0) {
    auto unsigned_count = static_cast<uint64>(count);
    if (unsigned_count + data_buffer_.Used() <= data_buffer_.Capacity()) {
      data_buffer_.Brush(unsigned_count);
      return true;
    } else {
      data_buffer_.BrushAll();
    }
  }
  return false;
}

google::protobuf::int64 SeekableInputStream::ByteCount() const {
  return static_cast<google::protobuf::int64>(data_buffer_.Used());
}

}  // namespace pax
