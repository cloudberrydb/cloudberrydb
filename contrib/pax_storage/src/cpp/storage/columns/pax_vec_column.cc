#include "storage/columns/pax_vec_column.h"

#include "comm/pax_memory.h"

namespace pax {

template <typename T>
PaxVecCommColumn<T>::PaxVecCommColumn(uint32 capacity) {
  data_ = PAX_NEW<DataBuffer<T>>(
      TYPEALIGN(MEMORY_ALIGN_SIZE, capacity * sizeof(T)));
}

template <typename T>
PaxVecCommColumn<T>::~PaxVecCommColumn() {
  PAX_DELETE(data_);
}

template <typename T>  // NOLINT: redirect constructor
PaxVecCommColumn<T>::PaxVecCommColumn() : PaxVecCommColumn(DEFAULT_CAPACITY) {}

template <typename T>
void PaxVecCommColumn<T>::Set(DataBuffer<T> *data, size_t non_null_rows) {
  PAX_DELETE(data_);

  data_ = data;
  non_null_rows_ = non_null_rows;
}

template <typename T>
void PaxVecCommColumn<T>::AppendInternal(char *buffer, size_t size) {  // NOLINT
  auto buffer_t = reinterpret_cast<T *>(buffer);
  Assert(size % sizeof(T) == 0);
  Assert(data_->Capacity() >= sizeof(T));

  if (data_->Available() == 0) {
    data_->ReSize(data_->Capacity() * 2);
  }

  data_->Write(buffer_t, sizeof(T));
  data_->Brush(sizeof(T));
}

template <typename T>
void PaxVecCommColumn<T>::Append(char *buffer, size_t size) {
  PaxColumn::Append(buffer, size);
  AppendInternal(buffer, size);
}

static char null_buffer[sizeof(int64)] = {0};

template <typename T>
void PaxVecCommColumn<T>::AppendNull() {
  PaxColumn::AppendNull();
  static_assert(sizeof(T) <= sizeof(int64), "invalid append null");
  AppendInternal(null_buffer, sizeof(T));
}

template <typename T>
PaxColumnTypeInMem PaxVecCommColumn<T>::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::kTypeFixed;
}

template <typename T>
PaxStorageFormat PaxVecCommColumn<T>::GetStorageFormat() const {
  return PaxStorageFormat::kTypeStoragePorcVec;
}

template <typename T>
size_t PaxVecCommColumn<T>::PhysicalSize() const {
  return data_->Used();
}

template <typename T>
int64 PaxVecCommColumn<T>::GetOriginLength() const {
  return data_->Used();
}

template <typename T>
int64 PaxVecCommColumn<T>::GetLengthsOriginLength() const {
  return 0;
}

template <typename T>
int32 PaxVecCommColumn<T>::GetTypeLength() const {
  return sizeof(T);
}

template <typename T>
std::pair<char *, size_t> PaxVecCommColumn<T>::GetBuffer() {
  return std::make_pair(data_->Start(), data_->Used());
}

template <typename T>
std::pair<char *, size_t> PaxVecCommColumn<T>::GetBuffer(size_t position) {
  CBDB_CHECK(position < GetRows(), cbdb::CException::ExType::kExTypeOutOfRange);
  return std::make_pair(data_->Start() + (sizeof(T) * position), sizeof(T));
}

template <typename T>
std::pair<char *, size_t> PaxVecCommColumn<T>::GetRangeBuffer(size_t start_pos,
                                                              size_t len) {
  CBDB_CHECK((start_pos + len) <= GetRows(),
             cbdb::CException::ExType::kExTypeOutOfRange);
  return std::make_pair(data_->Start() + (sizeof(T) * start_pos),
                        sizeof(T) * len);
}

template <typename T>
DataBuffer<T> *PaxVecCommColumn<T>::GetDataBuffer() {
  return data_;
}

template class PaxVecCommColumn<char>;
template class PaxVecCommColumn<int8>;
template class PaxVecCommColumn<int16>;
template class PaxVecCommColumn<int32>;
template class PaxVecCommColumn<int64>;
template class PaxVecCommColumn<float>;
template class PaxVecCommColumn<double>;

PaxVecNonFixedColumn::PaxVecNonFixedColumn(uint32 data_capacity,
                                           uint32 lengths_capacity)
    : estimated_size_(0),
      data_(PAX_NEW<DataBuffer<char>>(
          TYPEALIGN(MEMORY_ALIGN_SIZE, data_capacity))),
      offsets_(PAX_NEW<DataBuffer<int32>>(lengths_capacity)),
      next_offsets_(0) {
  Assert(data_capacity % sizeof(int64) == 0);
}

PaxVecNonFixedColumn::PaxVecNonFixedColumn()
    : PaxVecNonFixedColumn(DEFAULT_CAPACITY, DEFAULT_CAPACITY) {}

PaxVecNonFixedColumn::~PaxVecNonFixedColumn() {
  PAX_DELETE(data_);
  PAX_DELETE(offsets_);
}

void PaxVecNonFixedColumn::Set(DataBuffer<char> *data,
                               DataBuffer<int32> *offsets, size_t total_size,
                               size_t non_null_rows) {
  PAX_DELETE(data_);
  PAX_DELETE(offsets_);
  Assert(data && offsets);

  estimated_size_ = total_size;
  data_ = data;
  offsets_ = offsets;
  non_null_rows_ = non_null_rows;
  next_offsets_ = -1;
}

void PaxVecNonFixedColumn::Append(char *buffer, size_t size) {
  PaxColumn::Append(buffer, size);
  // vec format will remove the val header
  // so we don't need do align with the datum

  Assert(data_->Capacity() > 0);
  if (data_->Available() < size) {
    data_->ReSize(data_->Used() + size, 2);
  }

  estimated_size_ += size;
  data_->Write(buffer, size);
  data_->Brush(size);

  Assert(offsets_->Capacity() >= sizeof(int32));
  if (offsets_->Available() == 0) {
    offsets_->ReSize(offsets_->Used() + sizeof(int32), 2);
  }

  Assert(next_offsets_ != -1);
  offsets_->Write(next_offsets_);
  offsets_->Brush(sizeof(next_offsets_));
  next_offsets_ += size;
}

void PaxVecNonFixedColumn::AppendNull() {
  PaxColumn::AppendNull();
  Assert(offsets_->Capacity() >= sizeof(int32));
  if (offsets_->Available() == 0) {
    offsets_->ReSize(offsets_->Capacity() * 2);
  }

  Assert(next_offsets_ != -1);
  offsets_->Write(next_offsets_);
  offsets_->Brush(sizeof(next_offsets_));
}

void PaxVecNonFixedColumn::AppendLastOffset() {
  Assert(next_offsets_ != -1);
  Assert(offsets_->Capacity() >= sizeof(int32));
  if (offsets_->Available() == 0) {
    offsets_->ReSize(offsets_->Capacity() + sizeof(int32));
  }
  offsets_->Write(next_offsets_);
  offsets_->Brush(sizeof(next_offsets_));
  next_offsets_ = -1;
}

std::pair<char *, size_t> PaxVecNonFixedColumn::GetOffsetBuffer(
    bool append_last) {
  if (append_last) {
    AppendLastOffset();
  }

  return std::make_pair((char *)offsets_->GetBuffer(), offsets_->Used());
}

PaxColumnTypeInMem PaxVecNonFixedColumn::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::kTypeNonFixed;
}

PaxStorageFormat PaxVecNonFixedColumn::GetStorageFormat() const {
  return PaxStorageFormat::kTypeStoragePorcVec;
}

std::pair<char *, size_t> PaxVecNonFixedColumn::GetBuffer() {
  return std::make_pair(data_->GetBuffer(), data_->Used());
}

size_t PaxVecNonFixedColumn::PhysicalSize() const { return estimated_size_; }

int64 PaxVecNonFixedColumn::GetOriginLength() const { return data_->Used(); }

int64 PaxVecNonFixedColumn::GetLengthsOriginLength() const {
  Assert(next_offsets_ == -1);
  return offsets_->Used();
}

int32 PaxVecNonFixedColumn::GetTypeLength() const { return -1; }

std::pair<char *, size_t> PaxVecNonFixedColumn::GetBuffer(size_t position) {
  CBDB_CHECK(position < offsets_->GetSize(),
             cbdb::CException::ExType::kExTypeOutOfRange);
  // This situation happend when writing
  // The `offsets_` have not fill the last one
  if (unlikely(position == offsets_->GetSize() - 1)) {
    if (null_bitmap_ && null_bitmap_->Test(position)) {
      return std::make_pair(nullptr, 0);
    }
    return std::make_pair(data_->GetBuffer() + (*offsets_)[position],
                          next_offsets_);
  }

  auto start_offset = (*offsets_)[position];
  auto last_offset = (*offsets_)[position + 1];

  if (start_offset == last_offset) {
    return std::make_pair(nullptr, 0);
  }

  return std::make_pair(data_->GetBuffer() + start_offset,
                        last_offset - start_offset);
}

std::pair<char *, size_t> PaxVecNonFixedColumn::GetRangeBuffer(size_t start_pos,
                                                               size_t len) {
  CBDB_CHECK((start_pos + len) <= (offsets_->GetSize() - 1) && len >= 0,
             cbdb::CException::ExType::kExTypeOutOfRange);

  auto start_offset = (*offsets_)[start_pos];
  auto last_offset = (*offsets_)[start_pos + len];
  // all null here
  if (start_offset == last_offset) {
    return std::make_pair(data_->GetBuffer() + start_offset, 0);
  }

  return std::make_pair(data_->GetBuffer() + start_offset,
                        last_offset - start_offset);
}

DataBuffer<char> *PaxVecNonFixedColumn::GetDataBuffer() { return data_; }

}  // namespace pax
