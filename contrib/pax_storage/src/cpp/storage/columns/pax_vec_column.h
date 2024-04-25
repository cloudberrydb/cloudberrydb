#pragma once
#include "storage/columns/pax_column.h"

namespace pax {

template <typename T>
class PaxVecCommColumn : public PaxColumn {
 public:
  explicit PaxVecCommColumn(uint32 capacity);

  ~PaxVecCommColumn() override;

  PaxVecCommColumn();

  virtual void Set(DataBuffer<T> *data, size_t non_null_rows);

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override;

  PaxStorageFormat GetStorageFormat() const override;

  void Append(char *buffer, size_t size) override;

  void AppendNull() override;

  std::pair<char *, size_t> GetBuffer() override;

  std::pair<char *, size_t> GetBuffer(size_t position) override;

  std::pair<char *, size_t> GetRangeBuffer(size_t start_pos,
                                           size_t len) override;

  size_t PhysicalSize() const override;

  int64 GetOriginLength() const override;

  int64 GetLengthsOriginLength() const override;

  int32 GetTypeLength() const override;

  // directly pass the buffer to vec
  DataBuffer<T> *GetDataBuffer();

 protected:
  void AppendInternal(char *buffer, size_t size);

 protected:  // NOLINT
  DataBuffer<T> *data_;
};

extern template class PaxVecCommColumn<char>;
extern template class PaxVecCommColumn<int8>;
extern template class PaxVecCommColumn<int16>;
extern template class PaxVecCommColumn<int32>;
extern template class PaxVecCommColumn<int64>;
extern template class PaxVecCommColumn<float>;
extern template class PaxVecCommColumn<double>;

class PaxVecNonFixedColumn : public PaxColumn {
 public:
  explicit PaxVecNonFixedColumn(uint32 data_capacity, uint32 lengths_capacity);

  PaxVecNonFixedColumn();

  ~PaxVecNonFixedColumn() override;

  virtual void Set(DataBuffer<char> *data, DataBuffer<int32> *offsets,
                   size_t total_size, size_t non_null_rows);

  void Append(char *buffer, size_t size) override;

  void AppendNull() override;

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override;

  PaxStorageFormat GetStorageFormat() const override;

  size_t PhysicalSize() const override;

  int64 GetOriginLength() const override;

  int64 GetLengthsOriginLength() const override;

  int32 GetTypeLength() const override;

  std::pair<char *, size_t> GetBuffer() override;

  std::pair<char *, size_t> GetBuffer(size_t position) override;

  std::pair<char *, size_t> GetRangeBuffer(size_t start_pos,
                                           size_t len) override;

  virtual std::pair<char *, size_t> GetOffsetBuffer(bool append_last);

  // directly pass the buffer to vec
  DataBuffer<char> *GetDataBuffer();
#ifndef RUN_GTEST
 protected:
#endif
  void AppendLastOffset();

  DataBuffer<int32> *GetOffsetDataBuffer() { return offsets_; }

 protected:
  size_t estimated_size_;
  DataBuffer<char> *data_;
  DataBuffer<int32> *offsets_;

  // used in `kTypeStoragePorcVec`
  int32 next_offsets_;
};

}  // namespace pax
