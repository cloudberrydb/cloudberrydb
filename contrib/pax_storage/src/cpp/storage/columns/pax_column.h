#pragma once
#include <stddef.h>

#include <cstring>
#include <functional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "comm/bitmap.h"
#include "storage/columns/pax_compress.h"
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_encoding.h"
#include "storage/columns/pax_encoding_utils.h"
#include "storage/pax_buffer.h"
#include "storage/pax_defined.h"
#include "storage/proto/proto_wrappers.h"

namespace pax {

#define DEFAULT_CAPACITY 2048
#define NO_ENCODE_ORIGIN_LEN (-1)

// Used to mapping pg_type
enum PaxColumnTypeInMem {
  kTypeInvalid = 1,
  kTypeFixed = 2,
  kTypeNonFixed = 3,
  kTypeDecimal = 4,
  kTypeBpChar = 5,
};

class PaxColumn {
 public:
  PaxColumn();

  virtual ~PaxColumn();

  // Get the column in memory type
  virtual PaxColumnTypeInMem GetPaxColumnTypeInMem() const;

  // Get column buffer from current column
  virtual std::pair<char *, size_t> GetBuffer() = 0;

  // The interface `GetBuffer(size_t position)` and
  // `GetRangeBuffer(size_t start_pos, size_t len)`
  // will return the different values in different
  // `ColumnStorageType` + `ColumnTypeInMem`
  //
  // Also they should NEVER call in write path with encoding option!!!
  // But without encoding option, still can direct call it.
  //
  // If `storage_type_` is kTypeStorageOrcVec
  // Then data part contains `null field` which means no need use
  // `row index - null counts` to get the data.
  //
  // But If `storage_type_` is not kTypeStorageOrcVec
  // Then position should be `row index - null counts`, because
  // data part will not contains `null field`.
  //
  // Also it is kind different in fixed-length column and non-fixed-length
  // column when `storage_type_` is kTypeStorageOrcVec. For the fixed-length
  // column, If we got a `null field`, then it will return the buffer with zero
  // fill. But in non-fixed-length column, once we got  `null field`, the buffer
  // will be nullptr.
  //
  // A example to explain:
  //  std::tuple<char *, size_t, bool> GetBufferWithNull(
  //     size_t row_index,
  //     size_t null_counts) {
  //
  //    PaxColumn *column = source();
  //    char * buffer = nullptr;
  //    size_t length = 0;
  //    switch (GetPaxColumnTypeInMem()) {
  //      case kTypeFixed: {
  //        if (COLUMN_STORAGE_FORMAT_IS_VEC(column)) {
  //          std::tie(buffer, length) = column->GetBuffer(row_index);
  //          assert(buffer);  // different return in different ColumnTypeInMem
  //          if (!length) {
  //            return {nullptr, 0, true};
  //          }
  //        } else {
  //          std::tie(buffer, length) = column->GetBuffer(
  //            row_index - null_counts);
  //        }
  //        assert(buffer && length);
  //        return {buffer, length, false};
  //      }
  //      case kTypeNonFixed: {
  //        if (COLUMN_STORAGE_FORMAT_IS_VEC(column)) {
  //          std::tie(buffer, length) = column->GetBuffer(row_index);
  //          // different return in different ColumnTypeInMem
  //          assert((!buffer && !length) || (buffer && length));
  //          if (!buffer && !length) {
  //            return {nullptr, 0, true};
  //          }
  //        } else {
  //          std::tie(buffer, length) = column->GetBuffer(
  //            row_index - null_counts);
  //        }
  //        return {buffer, length, false};
  //        break;
  //      }
  //      default:
  //        // nothing
  //    }
  //    // should not react here!
  //    assert(false);
  //   }
  //
  // A simplest example:
  //  std::tuple<char *, size_t, bool> GetBufferWithNull(size_t row_index,
  //  size_t null_counts) {
  //    PaxColumn *column = source();
  //    char * buffer = nullptr;
  //    size_t length = 0;
  //    if (COLUMN_STORAGE_FORMAT_IS_VEC(column)) {
  //      std::tie(buffer, length) = column->GetBuffer(row_index);
  //      if (!length) {
  //        return {nullptr, 0, true};
  //      }
  //    } else {
  //      std::tie(buffer, length) = column->GetBuffer(row_index - null_counts);
  //    }
  //    assert(buffer && length);
  //    return {buffer, length, false};
  //  }
  //
  virtual std::pair<char *, size_t> GetBuffer(size_t position) = 0;

  // Get buffer by range [start_pos, start_pos + len)
  // Should never call in write path with encoding option
  virtual std::pair<char *, size_t> GetRangeBuffer(size_t start_pos,
                                                   size_t len) = 0;

  // Get all rows number(contain null) from column
  virtual size_t GetRows() const;

  // Get rows number(not null) from column
  virtual size_t GetNonNullRows() const;

  // Get all rows number(not null) from column by range [start_pos, start_pos +
  // len)
  virtual size_t GetRangeNonNullRows(size_t start_pos, size_t len);

  // Append new filed into current column
  virtual void Append(char *buffer, size_t size);

  // Append a null filed into last position
  virtual void AppendNull();

  // Estimated memory size from current column
  virtual size_t PhysicalSize() const = 0;

  // Get current storage type
  virtual PaxStorageFormat GetStorageFormat() const = 0;

  // Get the data size without encoding/compress
  virtual int64 GetOriginLength() const = 0;

  // Get the type length, used to identify sub-class
  // - `PaxCommColumn<T>` will return the <T> length
  // - `PaxNonFixedColumn` will return -1
  virtual int32 GetTypeLength() const = 0;

  // Contain null filed or not
  bool HasNull();

  // Are all values null?
  bool AllNull() const;

  // Set null bitmap
  void SetBitmap(Bitmap8 *null_bitmap);

  // Get Bitmap
  Bitmap8 *GetBitmap() { return null_bitmap_; }

  void SetRows(size_t total_rows);

  virtual size_t GetAlignSize() const;

  virtual void SetAlignSize(size_t align_size);

  // Get current encoding type
  inline ColumnEncoding_Kind GetEncodingType() const { return encoded_type_; }

  // Get current compress level
  inline int GetCompressLevel() const { return compress_level_; }

 protected:
  // The encoding option should pass in sub-class
  inline void SetEncodeType(ColumnEncoding_Kind encoding_type) {
    encoded_type_ = encoding_type;
  }

  inline void SetCompressLevel(int compress_level) {
    compress_level_ = compress_level;
  }

 private:
  void CreateNulls(size_t cap);

 protected:
  // null field bit map
  Bitmap8 *null_bitmap_;

  // Writer: write pointer
  // Reader: total rows
  uint32 total_rows_;

  // some of subclass will not implements the not null logic,
  // but can direct get not null rows by data part.
  size_t non_null_rows_;

  // the column encoded type
  ColumnEncoding_Kind encoded_type_;

  // the column compress level
  int compress_level_;

  // data part align size.
  // This field only takes effect when current column is no encoding/compress.
  //
  // About `type_align` in `pg_type` what you need to know:
  // 1. address alignment: the datum which return need alignment with
  // `type_align`
  // 2. datum padding: the datum need padding with `type_align`
  //
  // The align logic in pax:
  // 1. address alignment:
  //    - write will make sure address alignment(data stream) in disk
  //    - `ReadTuple` with/without memcpy should get a alignment datum
  // 2. datum padding: deal it in column `Append`
  size_t type_align_size_;

 private:
  PaxColumn(const PaxColumn &);
  PaxColumn &operator=(const PaxColumn &);
};

template <typename T>
class PaxCommColumn : public PaxColumn {
 public:
  explicit PaxCommColumn(uint32 capacity);

  ~PaxCommColumn() override;

  PaxCommColumn();

  virtual void Set(DataBuffer<T> *data);

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override;

  PaxStorageFormat GetStorageFormat() const override;

  void Append(char *buffer, size_t size) override;

  std::pair<char *, size_t> GetBuffer(size_t position) override;

  std::pair<char *, size_t> GetRangeBuffer(size_t start_pos,
                                           size_t len) override;

  size_t GetNonNullRows() const override;

  size_t PhysicalSize() const override;

  int64 GetOriginLength() const override;

  std::pair<char *, size_t> GetBuffer() override;

  int32 GetTypeLength() const override;

 protected:
  DataBuffer<T> *data_;
};

extern template class PaxCommColumn<char>;
extern template class PaxCommColumn<int8>;
extern template class PaxCommColumn<int16>;
extern template class PaxCommColumn<int32>;
extern template class PaxCommColumn<int64>;
extern template class PaxCommColumn<float>;
extern template class PaxCommColumn<double>;

class PaxNonFixedColumn : public PaxColumn {
 public:
  explicit PaxNonFixedColumn(uint32 capacity);

  PaxNonFixedColumn();

  ~PaxNonFixedColumn() override;

  virtual void Set(DataBuffer<char> *data, DataBuffer<int32> *lengths,
                   size_t total_size);

  void Append(char *buffer, size_t size) override;

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override;

  PaxStorageFormat GetStorageFormat() const override;

  std::pair<char *, size_t> GetBuffer() override;

  size_t PhysicalSize() const override;

  int64 GetOriginLength() const override;

  int32 GetTypeLength() const override;

  std::pair<char *, size_t> GetBuffer(size_t position) override;

  std::pair<char *, size_t> GetRangeBuffer(size_t start_pos,
                                           size_t len) override;

  size_t GetNonNullRows() const override;

  DataBuffer<int32> *GetLengthBuffer() const;

  DataBuffer<int32> *GetOffsetBuffer(bool append_last = false);

 protected:
  void BuildOffsets();

 protected:
  size_t estimated_size_;
  DataBuffer<char> *data_;

  // orc needs to serialize int32 array
  // the length of a single tuple field will not exceed 2GB,
  // so a variable-length element of the lengths stream can use int32
  // to represent the length
  DataBuffer<int32> *lengths_;
  std::vector<uint64> offsets_;
};

};  // namespace pax
