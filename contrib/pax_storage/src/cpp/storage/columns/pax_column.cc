#include "storage/columns/pax_column.h"

#include <cstring>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "storage/columns/pax_column_traits.h"
#include "storage/pax_defined.h"

namespace pax {

PaxColumn::PaxColumn()
    : null_bitmap_(nullptr),
      total_rows_(0),
      non_null_rows_(0),
      encoded_type_(ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED),
      compress_level_(0),
      type_align_size_(PAX_DATA_NO_ALIGN) {}

PaxColumn::~PaxColumn() { PAX_DELETE(null_bitmap_); }

PaxColumnTypeInMem PaxColumn::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::kTypeInvalid;
}

bool PaxColumn::HasNull() { return null_bitmap_ != nullptr; }

bool PaxColumn::AllNull() const {
  return null_bitmap_ && null_bitmap_->Empty();
}

void PaxColumn::SetBitmap(Bitmap8 *null_bitmap) {
  Assert(!null_bitmap_);
  null_bitmap_ = null_bitmap;
}

size_t PaxColumn::GetRows() const { return total_rows_; }

size_t PaxColumn::GetNonNullRows() const { return non_null_rows_; }

void PaxColumn::SetRows(size_t total_rows) { total_rows_ = total_rows; }

size_t PaxColumn::GetRangeNonNullRows(size_t start_pos, size_t len) {
  CBDB_CHECK((start_pos + len) <= GetRows(),
             cbdb::CException::ExType::kExTypeOutOfRange);
  if (!null_bitmap_) return len;
  if (len == 0) {
    return 0;
  }
  return null_bitmap_->CountBits(start_pos, start_pos + len - 1);
}

void PaxColumn::CreateNulls(size_t cap) {
  Assert(!null_bitmap_);
  null_bitmap_ = PAX_NEW<Bitmap8>(cap);
  null_bitmap_->SetN(total_rows_);
}

void PaxColumn::AppendNull() {
  if (!null_bitmap_) {
    CreateNulls(DEFAULT_CAPACITY);
  }
  null_bitmap_->Clear(total_rows_);
  ++total_rows_;
}

void PaxColumn::Append(char * /*buffer*/, size_t /*size*/) {
  if (null_bitmap_) null_bitmap_->Set(total_rows_);
  ++total_rows_;
  ++non_null_rows_;
}

size_t PaxColumn::GetAlignSize() const { return type_align_size_; }

void PaxColumn::SetAlignSize(size_t align_size) {
  Assert(align_size > 0 && (align_size & (align_size - 1)) == 0);
  type_align_size_ = align_size;
}

template <typename T>
PaxCommColumn<T>::PaxCommColumn(uint32 capacity) {
  data_ = PAX_NEW<DataBuffer<T>>(capacity * sizeof(T));
}

template <typename T>
PaxCommColumn<T>::~PaxCommColumn() {
  PAX_DELETE(data_);
}

template <typename T>  // NOLINT: redirect constructor
PaxCommColumn<T>::PaxCommColumn() : PaxCommColumn(DEFAULT_CAPACITY) {}

template <typename T>
void PaxCommColumn<T>::Set(DataBuffer<T> *data) {
  PAX_DELETE(data_);

  data_ = data;
}

template <typename T>
void PaxCommColumn<T>::Append(char *buffer, size_t size) {
  PaxColumn::Append(buffer, size);
  auto buffer_t = reinterpret_cast<T *>(buffer);

  // TODO(jiaqizho): Is it necessary to support multiple buffer insertions for
  // bulk insert push to mirco partition?
  Assert(size == sizeof(T));
  Assert(data_->Capacity() >= sizeof(T));

  if (data_->Available() == 0) {
    data_->ReSize(data_->Used() + size, 2);
  }

  data_->Write(buffer_t, sizeof(T));
  data_->Brush(sizeof(T));
}

template <typename T>
PaxStorageFormat PaxCommColumn<T>::GetStorageFormat() const {
  return PaxStorageFormat::kTypeStorageOrcNonVec;
}

template <typename T>
PaxColumnTypeInMem PaxCommColumn<T>::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::kTypeFixed;
}

template <typename T>
size_t PaxCommColumn<T>::GetNonNullRows() const {
  return data_->Used() / sizeof(T);
}

template <typename T>
size_t PaxCommColumn<T>::PhysicalSize() const {
  return data_->Used();
}

template <typename T>
int64 PaxCommColumn<T>::GetOriginLength() const {
  return NO_ENCODE_ORIGIN_LEN;
}

template <typename T>
int32 PaxCommColumn<T>::GetTypeLength() const {
  return sizeof(T);
}

template <typename T>
std::pair<char *, size_t> PaxCommColumn<T>::GetBuffer() {
  return std::make_pair(data_->Start(), data_->Used());
}

template <typename T>
std::pair<char *, size_t> PaxCommColumn<T>::GetBuffer(size_t position) {
  CBDB_CHECK(position < GetNonNullRows(),
             cbdb::CException::ExType::kExTypeOutOfRange);
  return std::make_pair(data_->Start() + (sizeof(T) * position), sizeof(T));
}

template <typename T>
std::pair<char *, size_t> PaxCommColumn<T>::GetRangeBuffer(size_t start_pos,
                                                           size_t len) {
  CBDB_CHECK((start_pos + len) <= GetNonNullRows(),
             cbdb::CException::ExType::kExTypeOutOfRange);
  return std::make_pair(data_->Start() + (sizeof(T) * start_pos),
                        sizeof(T) * len);
}

template class PaxCommColumn<char>;
template class PaxCommColumn<int8>;
template class PaxCommColumn<int16>;
template class PaxCommColumn<int32>;
template class PaxCommColumn<int64>;
template class PaxCommColumn<float>;
template class PaxCommColumn<double>;

PaxNonFixedColumn::PaxNonFixedColumn(uint32 capacity) : estimated_size_(0) {
  data_ = PAX_NEW<DataBuffer<char>>(capacity * sizeof(char));
  lengths_ = PAX_NEW<DataBuffer<int32>>(capacity * sizeof(char));
}

PaxNonFixedColumn::PaxNonFixedColumn() : PaxNonFixedColumn(DEFAULT_CAPACITY) {}

PaxNonFixedColumn::~PaxNonFixedColumn() {
  PAX_DELETE(data_);
  PAX_DELETE(lengths_);
}

void PaxNonFixedColumn::Set(DataBuffer<char> *data, DataBuffer<int32> *lengths,
                            size_t total_size) {
  PAX_DELETE(data_);
  PAX_DELETE(lengths_);

  estimated_size_ = total_size;
  data_ = data;
  lengths_ = lengths;
  BuildOffsets();
}

void PaxNonFixedColumn::BuildOffsets() {
  offsets_.clear();
  for (size_t i = 0; i < lengths_->GetSize(); i++) {
    offsets_.emplace_back(i == 0 ? 0 : offsets_[i - 1] + (*lengths_)[i - 1]);
  }
}

void PaxNonFixedColumn::Append(char *buffer, size_t size) {
  size_t origin_size;
  origin_size = size;

  if (!COLUMN_STORAGE_FORMAT_IS_VEC(this)) {
    Assert(likely(reinterpret_cast<char *> MAXALIGN(data_->Position()) ==
                  data_->Position()));
    size = MAXALIGN(size);
  }

  PaxColumn::Append(buffer, origin_size);
  if (data_->Available() < size) {
    data_->ReSize(data_->Used() + size, 2);
  }

  if (lengths_->Available() == 0) {
    lengths_->ReSize(lengths_->Used() + sizeof(int32), 2);
  }

  estimated_size_ += size;
  data_->Write(buffer, origin_size);
  data_->Brush(size);

  lengths_->Write(reinterpret_cast<int32 *>(&size), sizeof(int32));
  lengths_->Brush(sizeof(int32));

  offsets_.emplace_back(offsets_.empty()
                            ? 0
                            : offsets_[offsets_.size() - 1] +
                                  (*lengths_)[offsets_.size() - 1]);
  Assert(offsets_.size() == lengths_->GetSize());
}

DataBuffer<int32> *PaxNonFixedColumn::GetLengthBuffer() const {
  return lengths_;
}

PaxColumnTypeInMem PaxNonFixedColumn::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::kTypeNonFixed;
}

PaxStorageFormat PaxNonFixedColumn::GetStorageFormat() const {
  return PaxStorageFormat::kTypeStorageOrcNonVec;
}

std::pair<char *, size_t> PaxNonFixedColumn::GetBuffer() {
  return std::make_pair(data_->GetBuffer(), data_->Used());
}

size_t PaxNonFixedColumn::GetNonNullRows() const { return lengths_->GetSize(); }

size_t PaxNonFixedColumn::PhysicalSize() const { return estimated_size_; }

int64 PaxNonFixedColumn::GetOriginLength() const {
  return NO_ENCODE_ORIGIN_LEN;
}

int32 PaxNonFixedColumn::GetTypeLength() const { return -1; }

std::pair<char *, size_t> PaxNonFixedColumn::GetBuffer(size_t position) {
  CBDB_CHECK(position < GetNonNullRows(),
             cbdb::CException::ExType::kExTypeOutOfRange);

  return std::make_pair(data_->GetBuffer() + offsets_[position],
                        (*lengths_)[position]);
}

std::pair<char *, size_t> PaxNonFixedColumn::GetRangeBuffer(size_t start_pos,
                                                            size_t len) {
  CBDB_CHECK((start_pos + len) <= GetNonNullRows() && len >= 0,
             cbdb::CException::ExType::kExTypeOutOfRange);
  size_t range_len = 0;

  for (size_t i = start_pos; i < start_pos + len; i++) {
    range_len += (*lengths_)[i];
  }

  if (GetNonNullRows() == 0) {
    Assert(range_len == 0);
    return std::make_pair(data_->GetBuffer(), 0);
  }

  Assert(start_pos < offsets_.size());
  return std::make_pair(data_->GetBuffer() + offsets_[start_pos], range_len);
}

};  // namespace pax
