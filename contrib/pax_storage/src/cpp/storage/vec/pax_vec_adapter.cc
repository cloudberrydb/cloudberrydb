#include "storage/vec/pax_vec_adapter.h"

#ifdef VEC_BUILD

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-but-set-variable"

extern "C" {
#include "utils/tuptable_vec.h"  // for vec tuple
}

#pragma GCC diagnostic pop

#include "storage/columns/pax_column_traits.h"
#include "storage/pax_itemptr.h"
#include "storage/vec/arrow_wrapper.h"

/// export interface wrapper of arrow
namespace arrow {

template <typename T>
struct ArrowExportTraits {};

template <typename T>
using Arrow = std::function<Status(const T &, struct ArrowSchema *)>;

template <>
struct ArrowExportTraits<DataType> {
  static Arrow<DataType> export_func;
};

template <>
struct ArrowExportTraits<Field> {
  static Arrow<Field> export_func;
};

template <>
struct ArrowExportTraits<Schema> {
  static Arrow<Schema> export_func;
};

Arrow<DataType> ArrowExportTraits<DataType>::export_func = ExportType;
Arrow<Field> ArrowExportTraits<Field>::export_func = ExportField;
Arrow<Schema> ArrowExportTraits<Schema>::export_func = ExportSchema;

void ExportArrayRelease(ArrowArray *array) {
  // The Exception throw from this call back won't be catch
  // Because caller will call this callback in destructor
  // just let long jump happen
  if (array->children) {
    for (int64_t i = 0; i < array->n_children; i++) {
      if (array->children[i] && array->children[i]->release) {
        array->children[i]->release(array->children[i]);
      }
    }

    pfree(array->children);
  }

  if (array->buffers) {
    for (int64_t i = 0; i < array->n_buffers; i++) {
      if (array->buffers[i]) {
        pfree((void *)array->buffers[i]);
      }
    }

    pfree(array->buffers);
  }

  array->release = NULL;
  if (array->private_data) {
    pfree((ArrowArray *)array->private_data);
  }
};

void ExportArrayNodeDetails(ArrowArray *export_array,
                            const std::shared_ptr<ArrayData> &data,
                            const std::vector<ArrowArray *> &child_array,
                            bool is_child) {
  export_array->length = data->length;
  export_array->null_count = data->null_count;
  export_array->offset = data->offset;

  export_array->n_buffers = static_cast<int64_t>(data->buffers.size());
  export_array->n_children = static_cast<int64_t>(child_array.size());
  export_array->buffers = export_array->n_buffers
                              ? (const void **)cbdb::Palloc0(
                                    export_array->n_buffers * sizeof(void *))
                              : nullptr;

  for (int64_t i = 0; i < export_array->n_buffers; i++) {
    auto buffer = data->buffers[i];
    export_array->buffers[i] = buffer ? buffer->data() : nullptr;
  }

  export_array->children =
      export_array->n_children
          ? (ArrowArray **)cbdb::Palloc0(export_array->n_children *
                                         sizeof(ArrowArray *))
          : nullptr;
  for (int64_t i = 0; i < export_array->n_children; i++) {
    export_array->children[i] = child_array[i];
  }

  export_array->dictionary = nullptr;
  export_array->private_data = is_child ? (void *)export_array : nullptr;
  export_array->release = ExportArrayRelease;
}

static ArrowArray *ExportArrayNode(const std::shared_ptr<ArrayData> &data) {
  ArrowArray *export_array;
  std::vector<ArrowArray *> child_array;

  for (size_t i = 0; i < data->child_data.size(); ++i) {
    child_array.emplace_back(ExportArrayNode(data->child_data[i]));
  }

  export_array = (ArrowArray *)cbdb::Palloc0(sizeof(ArrowArray));
  ExportArrayNodeDetails(export_array, data, child_array, true);
  return export_array;
}

static void ExportArrayRoot(const std::shared_ptr<ArrayData> &data,
                            ArrowArray *export_array) {
  std::vector<ArrowArray *> child_array;

  for (size_t i = 0; i < data->child_data.size(); ++i) {
    child_array.emplace_back(ExportArrayNode(data->child_data[i]));
  }
  Assert(export_array);

  ExportArrayNodeDetails(export_array, data, child_array, false);
}

}  // namespace arrow

namespace pax {

#define DECIMAL_BUFFER_SIZE 16
#define DECIMAL_BUFFER_BITS 128

static void CopyFixedRawBufferWithNull(PaxColumn *column,
                                       const Bitmap8 *visibility_map_bitset,
                                       size_t bitset_index_begin,
                                       size_t range_begin, size_t range_lens,
                                       size_t data_index_begin,
                                       size_t data_range_lens,
                                       DataBuffer<char> *out_data_buffer);

static void CopyFixedBuffer(PaxColumn *column,
                            const Bitmap8 *visibility_map_bitset,
                            size_t bitset_index_begin, size_t range_begin,
                            size_t range_lens, size_t data_index_begin,
                            size_t data_range_lens,
                            DataBuffer<char> *out_data_buffer);

static void CopyNonFixedRawBuffer(PaxColumn *column,
                                  const Bitmap8 *visibility_map_bitset,
                                  size_t bitset_index_begin, size_t range_begin,
                                  size_t range_lens, size_t data_index_begin,
                                  size_t data_range_lens,
                                  DataBuffer<int32> *offset_buffer,
                                  DataBuffer<char> *out_data_buffer,
                                  bool is_bpchar);

static void CopyBitmap(const Bitmap8 *bitmap, size_t range_begin,
                       size_t range_lens, DataBuffer<char> *null_bits_buffer);

static void CopyBitmapToVecBuffer(
    PaxColumn *column, const Bitmap8 *visibility_map_bitset,
    size_t bitset_index_begin, size_t range_begin, size_t range_lens,
    size_t data_range_lens, size_t out_range_lens,
    VecAdapter::VecBatchBuffer *vec_cache_buffer_) {
  //
  if (column->HasNull()) {
    auto null_bits_buffer = &vec_cache_buffer_->null_bits_buffer;
    if (visibility_map_bitset == nullptr) {
      // null length depends on `range_lens`
      auto null_align_bytes =
          TYPEALIGN(MEMORY_ALIGN_SIZE, BITS_TO_BYTES(range_lens));
      Bitmap8 *bitmap = nullptr;
      Assert(!null_bits_buffer->GetBuffer());
      null_bits_buffer->Set((char *)cbdb::Palloc(null_align_bytes),
                            null_align_bytes);
      bitmap = column->GetBitmap();
      Assert(bitmap);
      CopyBitmap(bitmap, range_begin, range_lens, null_bits_buffer);
      vec_cache_buffer_->null_counts = range_lens - data_range_lens;
    } else {
      Bitmap8 *bitmap = nullptr;
      bitmap = column->GetBitmap();
      Assert(bitmap);

      Bitmap8 *null_bitmap = PAX_NEW<Bitmap8>(out_range_lens);
      size_t null_count = 0;
      size_t null_index = 0;
      for (size_t i = range_begin; i < range_begin + range_lens; i++) {
        // only calculate the null bitmap corresponding to the unmarked
        // deleted tuple.
        if (!visibility_map_bitset->Test(i - range_begin +
                                         bitset_index_begin)) {
          // is null
          if (!bitmap->Test(i)) {
            null_count++;
          } else {
            // not null
            null_bitmap->Set(null_index);
          }
          null_index++;
        }
      }

      auto null_bytes =
          TYPEALIGN(MEMORY_ALIGN_SIZE, BITS_TO_BYTES(out_range_lens));
      Assert(!null_bits_buffer->GetBuffer());
      null_bits_buffer->Set((char *)cbdb::Palloc0(null_bytes), null_bytes);
      CopyBitmap(null_bitmap, 0, out_range_lens, null_bits_buffer);
      vec_cache_buffer_->null_counts = null_count;
      CBDB_CHECK(out_range_lens == null_index,
                 cbdb::CException::ExType::kExTypeOutOfRange);
      PAX_DELETE(null_bitmap);
    }
  }
}

static inline void CopyFixedRawBuffer(char *buffer, size_t len,
                                      DataBuffer<char> *data_buffer) {
  data_buffer->Write(buffer, len);
  data_buffer->Brush(len);
}

static void ConvSchemaAndDataToVec(
    Oid pg_type_oid, char *attname, size_t all_nums_of_row,
    VecAdapter::VecBatchBuffer *vec_batch_buffer,
    std::vector<std::shared_ptr<arrow::Field>> &schema_types,
    arrow::ArrayVector &array_vector, std::vector<std::string> &field_names);

VecAdapter::VecBatchBuffer::VecBatchBuffer()
    : vec_buffer(0), null_bits_buffer(0), offset_buffer(0), null_counts(0) {
  SetMemoryTakeOver(true);
};

void VecAdapter::VecBatchBuffer::Reset() {
  // Current `DataBuffer` will not hold the buffers.
  // And the buffers will be trans to `ArrayVector` which will hold it.
  // Released in `release` callback or Memory context reset in `EndScan`
  SetMemoryTakeOver(false);
  vec_buffer.Reset();
  null_bits_buffer.Reset();
  offset_buffer.Reset();
  null_counts = 0;
  SetMemoryTakeOver(true);
}

void VecAdapter::VecBatchBuffer::SetMemoryTakeOver(bool take) {
  vec_buffer.SetMemTakeOver(take);
  null_bits_buffer.SetMemTakeOver(take);
  offset_buffer.SetMemTakeOver(take);
}

void CopyFixedRawBufferWithNull(PaxColumn *column,
                                const Bitmap8 *visibility_map_bitset,
                                size_t bitset_index_begin, size_t range_begin,
                                size_t range_lens, size_t data_index_begin,
                                size_t data_range_lens,
                                DataBuffer<char> *out_data_buffer) {
  char *buffer;
  size_t buffer_len;

  std::tie(buffer, buffer_len) =
      column->GetRangeBuffer(data_index_begin, data_range_lens);

  auto null_bitmap = column->GetBitmap();
  size_t non_null_offset = 0;
  size_t type_len = column->GetTypeLength();
  for (size_t i = range_begin; i < (range_begin + range_lens); i++) {
    // filted by row_filter or bloom_filter
    if (visibility_map_bitset &&
        visibility_map_bitset->Test(i - range_begin + bitset_index_begin)) {
      if (null_bitmap->Test(i)) {
        non_null_offset += type_len;
      }
      continue;
    }
    if (null_bitmap->Test(i)) {
      out_data_buffer->Write(buffer + non_null_offset, type_len);
      non_null_offset += type_len;
    }
    out_data_buffer->Brush(type_len);
  }
}

void CopyFixedBuffer(PaxColumn *column, const Bitmap8 *visibility_map_bitset,
                     size_t bitset_index_begin, size_t range_begin,
                     size_t range_lens, size_t data_index_begin,
                     size_t data_range_lens,
                     DataBuffer<char> *out_data_buffer) {
  if (column->HasNull()) {
    CopyFixedRawBufferWithNull(
        column, visibility_map_bitset, bitset_index_begin, range_begin,
        range_lens, data_index_begin, data_range_lens, out_data_buffer);
  } else {
    char *buffer;
    size_t buffer_len;
    std::tie(buffer, buffer_len) =
        column->GetRangeBuffer(data_index_begin, data_range_lens);

    if (visibility_map_bitset == nullptr) {
      CopyFixedRawBuffer(buffer, buffer_len, out_data_buffer);
    } else {
      size_t non_null_offset = 0;
      size_t type_len = column->GetTypeLength();
      for (size_t i = range_begin; i < (range_begin + range_lens); i++) {
        if (visibility_map_bitset &&
            visibility_map_bitset->Test(i - range_begin + bitset_index_begin)) {
          non_null_offset += type_len;
          continue;
        }
        out_data_buffer->Write(buffer + non_null_offset, type_len);
        out_data_buffer->Brush(type_len);
        non_null_offset += type_len;
      }
    }
  }
}

void CopyNonFixedRawBuffer(PaxColumn *column,
                           const Bitmap8 *visibility_map_bitset,
                           size_t bitset_index_begin, size_t range_begin,
                           size_t range_lens, size_t data_index_begin,
                           size_t data_range_lens,
                           DataBuffer<int32> *offset_buffer,
                           DataBuffer<char> *out_data_buffer, bool is_bpchar) {
  size_t dst_offset = out_data_buffer->Used();
  char *buffer = nullptr;
  size_t buffer_len = 0;

  auto null_bitmap = column->GetBitmap();
  size_t non_null_offset = 0;

  for (size_t i = range_begin; i < (range_begin + range_lens); i++) {
    if (visibility_map_bitset &&
        visibility_map_bitset->Test(i - range_begin + bitset_index_begin)) {
      // tuples that are marked for deletion also need to calculate
      // non-null-offset, which is used to calculate the address of valid
      // data. null_bitmap: 0 represents null, 1 represents non-null
      if (!null_bitmap || null_bitmap->Test(i)) {
        non_null_offset++;
      }
      continue;
    }

    if (null_bitmap && !null_bitmap->Test(i)) {
      offset_buffer->Write(dst_offset);
      offset_buffer->Brush(sizeof(int32));

    } else {
      struct varlena *vl, *tunpacked;
      size_t read_len = 0;
      char *read_data;

      std::tie(buffer, buffer_len) =
          column->GetBuffer(data_index_begin + non_null_offset);

      vl = (struct varlena *)(buffer);

      tunpacked = cbdb::PgDeToastDatum(vl);
      Assert((Pointer)vl == (Pointer)tunpacked);

      read_len = VARSIZE_ANY_EXHDR(tunpacked);
      read_data = VARDATA_ANY(tunpacked);

      // In vec, bpchar not allow store empty char after the actual characters
      if (is_bpchar) {
        read_len = bpchartruelen(read_data, read_len);
      }

      out_data_buffer->Write(read_data, read_len);
      out_data_buffer->Brush(read_len);

      offset_buffer->Write(dst_offset);
      offset_buffer->Brush(sizeof(int32));

      dst_offset += read_len;
      non_null_offset++;
    }
  }

  offset_buffer->Write(dst_offset);
  offset_buffer->Brush(sizeof(int32));

  AssertImply(visibility_map_bitset == nullptr,
              non_null_offset == data_range_lens);
  AssertImply(visibility_map_bitset, non_null_offset <= data_range_lens);

  // if not the marking deletion，the non_null_offset is equal to the
  // data_range_lens; when there is a Visibility Map, part of the data is
  // invalid. Non_null_offset may be less than the data_range_lens.
  if (visibility_map_bitset == nullptr) {
    CBDB_CHECK(non_null_offset == data_range_lens,
               cbdb::CException::ExType::kExTypeOutOfRange);
  }
}

static void CopyBitmap(const Bitmap8 *bitmap, size_t range_begin,
                       size_t range_lens, DataBuffer<char> *null_bits_buffer) {
  // VEC_BATCH_LENGTH must align with 8
  // So the `range_begin % 8` must be 0
  static_assert(VEC_BATCH_LENGTH % 8 == 0, "Assumption is broken.");
  Assert(range_begin % 8 == 0);

  auto null_buffer = reinterpret_cast<char *>(bitmap->Raw().bitmap);
  auto write_size = BITS_TO_BYTES(range_lens);
  auto bitmap_raw_size = bitmap->Raw().size;

  if ((range_begin / 8) >= bitmap_raw_size) {  // all nulls in current range
    null_bits_buffer->WriteZero(write_size);
    null_bits_buffer->Brush(write_size);
  } else {
    auto remain_size = bitmap_raw_size - (range_begin / 8);
    if (remain_size >= write_size) {  // full bitmap in current range
      null_bits_buffer->Write(null_buffer + range_begin / 8, write_size);
      null_bits_buffer->Brush(write_size);
    } else {  // part of non-null range with a continuous all nulls range
      auto write_size_gap = write_size - remain_size;
      null_bits_buffer->Write(null_buffer + range_begin / 8, remain_size);
      null_bits_buffer->Brush(remain_size);
      null_bits_buffer->WriteZero(write_size_gap);
      null_bits_buffer->Brush(write_size_gap);
    }
  }
}

static std::tuple<std::shared_ptr<arrow::Buffer>,
                  std::shared_ptr<arrow::Buffer>,
                  std::shared_ptr<arrow::Buffer>>
ConvToVecBuffer(VecAdapter::VecBatchBuffer *vec_batch_buffer) {
  std::shared_ptr<arrow::Buffer> arrow_buffer = nullptr;
  std::shared_ptr<arrow::Buffer> arrow_null_buffer = nullptr;
  std::shared_ptr<arrow::Buffer> arrow_offset_buffer = nullptr;

  arrow_buffer = std::make_shared<arrow::Buffer>(
      (uint8 *)vec_batch_buffer->vec_buffer.GetBuffer(),
      (int64)vec_batch_buffer->vec_buffer.Capacity());

  Assert(vec_batch_buffer->vec_buffer.Capacity() % MEMORY_ALIGN_SIZE == 0);

  if (vec_batch_buffer->null_bits_buffer.GetBuffer()) {
    arrow_null_buffer = std::make_shared<arrow::Buffer>(
        (uint8 *)vec_batch_buffer->null_bits_buffer.GetBuffer(),
        (int64)vec_batch_buffer->null_bits_buffer.Capacity());

    Assert(vec_batch_buffer->null_bits_buffer.Capacity() % MEMORY_ALIGN_SIZE ==
           0);
  }

  if (vec_batch_buffer->offset_buffer.GetBuffer()) {
    arrow_offset_buffer = std::make_shared<arrow::Buffer>(
        (uint8 *)vec_batch_buffer->offset_buffer.GetBuffer(),
        (int64)vec_batch_buffer->offset_buffer.Capacity());
    Assert(vec_batch_buffer->offset_buffer.Capacity() % MEMORY_ALIGN_SIZE == 0);
  }
  return std::make_tuple(arrow_buffer, arrow_null_buffer, arrow_offset_buffer);
}

template <typename ArrayType, typename Tuple, std::size_t... I>
static inline std::shared_ptr<ArrayType> makeSharedArrayImpl(
    Tuple &&tuple, std::index_sequence<I...>) {
  return std::make_shared<ArrayType>(
      std::get<I>(std::forward<Tuple>(tuple))...);
}

template <typename ArrayType, typename... Args>
static inline std::shared_ptr<ArrayType> makeSharedArray(Args &&...args) {
  return makeSharedArrayImpl<ArrayType>(
      std::forward_as_tuple(std::forward<Args>(args)...),
      std::make_index_sequence<sizeof...(Args)>{});
}

template <typename ArrayType, typename... Args>
static void ConvArrowSchemaAndBuffer(
    const std::string &field_name, std::shared_ptr<arrow::DataType> data_type,
    VecAdapter::VecBatchBuffer *vec_batch_buffer, size_t all_nums_of_row,
    std::vector<std::shared_ptr<arrow::Field>> &schema_types,
    arrow::ArrayVector &array_vector, std::vector<std::string> &field_names,
    Args &&...args) {
  std::shared_ptr<arrow::Buffer> arrow_buffer;
  std::shared_ptr<arrow::Buffer> arrow_null_buffer;

  auto arrow_buffers = ConvToVecBuffer(vec_batch_buffer);
  arrow_buffer = std::get<0>(arrow_buffers);
  arrow_null_buffer = std::get<1>(arrow_buffers);

  schema_types.emplace_back(arrow::field(field_name, data_type));

  // if C++17 or later, we can use `std::apply` to simplify the code
  auto array = makeSharedArray<ArrayType>(
      std::forward<Args>(args)..., all_nums_of_row, arrow_buffer,
      arrow_null_buffer, vec_batch_buffer->null_counts);

  array_vector.emplace_back(array);
  field_names.emplace_back(field_name);
}

static void ConvSchemaAndDataToVec(
    Oid pg_type_oid, char *attname, size_t all_nums_of_row,
    VecAdapter::VecBatchBuffer *vec_batch_buffer,
    std::vector<std::shared_ptr<arrow::Field>> &schema_types,
    arrow::ArrayVector &array_vector, std::vector<std::string> &field_names) {
  switch (pg_type_oid) {
    case BOOLOID: {
      ConvArrowSchemaAndBuffer<arrow::BooleanArray>(
          std::string(attname), arrow::boolean(), vec_batch_buffer,
          all_nums_of_row, schema_types, array_vector, field_names);
      break;
    }
    case CHAROID: {
      ConvArrowSchemaAndBuffer<arrow::Int8Array>(
          std::string(attname), arrow::int8(), vec_batch_buffer,
          all_nums_of_row, schema_types, array_vector, field_names);
      break;
    }
    case TIMEOID:
    case TIMESTAMPOID:
    case TIMESTAMPTZOID: {
      ConvArrowSchemaAndBuffer<arrow::TimestampArray>(
          std::string(attname), arrow::timestamp(arrow::TimeUnit::MICRO),
          vec_batch_buffer, all_nums_of_row, schema_types, array_vector,
          field_names, arrow::timestamp(arrow::TimeUnit::MICRO));
      break;
    }
    case TIDOID:
    case INT8OID: {
      ConvArrowSchemaAndBuffer<arrow::Int64Array>(
          std::string(attname), arrow::int64(), vec_batch_buffer,
          all_nums_of_row, schema_types, array_vector, field_names);
      break;
    }
    case INT2OID: {
      ConvArrowSchemaAndBuffer<arrow::Int16Array>(
          std::string(attname), arrow::int16(), vec_batch_buffer,
          all_nums_of_row, schema_types, array_vector, field_names);
      break;
    }
    case DATEOID: {
      ConvArrowSchemaAndBuffer<arrow::Date32Array>(
          std::string(attname), arrow::date32(), vec_batch_buffer,
          all_nums_of_row, schema_types, array_vector, field_names);
      break;
    }
    case INT4OID: {
      ConvArrowSchemaAndBuffer<arrow::Int32Array>(
          std::string(attname), arrow::int32(), vec_batch_buffer,
          all_nums_of_row, schema_types, array_vector, field_names);
      break;
    }
    case BPCHAROID:
    case VARCHAROID:
    case TEXTOID: {
      std::shared_ptr<arrow::Buffer> arrow_buffer;
      std::shared_ptr<arrow::Buffer> arrow_null_buffer;
      std::shared_ptr<arrow::Buffer> arrow_offset_buffer;

      auto arrow_buffers = ConvToVecBuffer(vec_batch_buffer);
      arrow_buffer = std::get<0>(arrow_buffers);
      arrow_null_buffer = std::get<1>(arrow_buffers);
      arrow_offset_buffer = std::get<2>(arrow_buffers);

      schema_types.emplace_back(
          arrow::field(std::string(attname), arrow::utf8()));
      auto array = std::make_shared<arrow::StringArray>(
          all_nums_of_row, arrow_offset_buffer, arrow_buffer, arrow_null_buffer,
          vec_batch_buffer->null_counts);

      array_vector.emplace_back(array);
      field_names.emplace_back(std::string(std::string(attname)));
      break;
    }
    case FLOAT4OID: {
      ConvArrowSchemaAndBuffer<arrow::FloatArray>(
          std::string(attname), arrow::float32(), vec_batch_buffer,
          all_nums_of_row, schema_types, array_vector, field_names);
      break;
    }
    case FLOAT8OID: {
      ConvArrowSchemaAndBuffer<arrow::DoubleArray>(
          std::string(attname), arrow::float64(), vec_batch_buffer,
          all_nums_of_row, schema_types, array_vector, field_names);
      break;
    }
    case NUMERICOID: {
      std::string field_name = std::string(attname);
      std::shared_ptr<arrow::Buffer> arrow_buffer;
      std::shared_ptr<arrow::Buffer> arrow_null_buffer;
      std::shared_ptr<arrow::DataType> data_type;

      data_type = arrow::decimal128(-1, -1);

      auto arrow_buffers = ConvToVecBuffer(vec_batch_buffer);
      arrow_buffer = std::get<0>(arrow_buffers);
      arrow_null_buffer = std::get<1>(arrow_buffers);
      Assert(std::get<2>(arrow_buffers) == nullptr);
      Assert(arrow_buffer);
      AssertImply(vec_batch_buffer->null_counts > 0, arrow_null_buffer);

      schema_types.emplace_back(arrow::field(field_name, data_type));
      std::shared_ptr<arrow::ArrayData> decimal_array_data =
          arrow::ArrayData::Make(
              data_type, all_nums_of_row,
              {arrow_null_buffer, arrow_buffer},  // 16bytes array
              vec_batch_buffer->null_counts);
      auto array = std::make_shared<arrow::Decimal128Array>(decimal_array_data);

      array_vector.emplace_back(array);
      field_names.emplace_back(field_name);
      break;
    }
    case INT2ARRAYOID:
    case INT4ARRAYOID:
    case INT8ARRAYOID:
    case FLOAT4ARRAYOID:
    case FLOAT8ARRAYOID:
    case TEXTARRAYOID:
    case BPCHARARRAYOID: {
      Assert(false);
    }
    case NAMEOID:
    case XIDOID:
    case CIDOID:
    case OIDVECTOROID:
    case JSONOID:
    case OIDOID:
    case REGPROCOID:
    default: {
      Assert(false);
    }
  }
}

VecAdapter::VecAdapter(TupleDesc tuple_desc, bool build_ctid)
    : rel_tuple_desc_(tuple_desc),
      cached_batch_lens_(0),
      vec_cache_buffer_(nullptr),
      vec_cache_buffer_lens_(0),
      process_columns_(nullptr),
      current_cached_pax_columns_index_(0),
      build_ctid_(build_ctid) {
  Assert(rel_tuple_desc_);
};

VecAdapter::~VecAdapter() {
  if (vec_cache_buffer_) {
    for (int i = 0; i < vec_cache_buffer_lens_; i++) {
      vec_cache_buffer_[i].SetMemoryTakeOver(false);
    }
    PAX_DELETE_ARRAY(vec_cache_buffer_);
  }
}

void VecAdapter::SetDataSource(PaxColumns *columns) {
  Assert(columns);
  process_columns_ = columns;
  current_cached_pax_columns_index_ = 0;
  cached_batch_lens_ = 0;
  // FIXME(jiaqizho): should expand vec_cache_buffer_
  // if columns number not match vec_cache_buffer_ will not take care of
  // schema it only handle buffer
  AssertImply(vec_cache_buffer_,
              columns->GetColumns() == (size_t)vec_cache_buffer_lens_);
  if (!vec_cache_buffer_) {
    vec_cache_buffer_ = PAX_NEW_ARRAY<VecBatchBuffer>(columns->GetColumns());
    vec_cache_buffer_lens_ = columns->GetColumns();
  }
}

const TupleDesc VecAdapter::GetRelationTupleDesc() const {
  return rel_tuple_desc_;
}

int VecAdapter::AppendToVecBuffer() {
  PaxColumns *columns;
  PaxColumn *column;
  size_t range_begin = current_cached_pax_columns_index_;
  size_t range_lens = VEC_BATCH_LENGTH;

  columns = process_columns_;
  Assert(cached_batch_lens_ <= VEC_BATCH_LENGTH);

  // There are three cases to direct return
  // 1. no call `DataSource`, then no source setup
  // 2. already have cached vec batch without flush
  // 3. all of data in pax columns have been comsume
  if (!columns || cached_batch_lens_ != 0 ||
      range_begin == columns->GetRows()) {
    return -1;
  }

  Assert(range_begin <= columns->GetRows());

  if (COLUMN_STORAGE_FORMAT_IS_VEC(columns)) {
    // direct redict
    return AppendVecFormat();
  }

  // recompute `range_lens`, if remain data LT `VEC_BATCH_LENGTH`
  // then should reduce `range_lens`
  if ((range_begin + range_lens) > columns->GetRows()) {
    range_lens = columns->GetRows() - range_begin;
  }

  size_t filter_count = GetInvisibleNumber(range_begin, range_lens);

  size_t out_range_lens = range_lens - filter_count;

  if (out_range_lens == 0) {
    current_cached_pax_columns_index_ = range_begin + range_lens;
    return 0;
  }

  for (size_t index = 0; index < columns->GetColumns(); index++) {
    size_t data_index_begin = 0;
    size_t data_range_lens = 0;
    DataBuffer<char> *vec_buffer = nullptr;
    DataBuffer<int32> *offset_buffer = nullptr;

    char *raw_buffer = nullptr;
    size_t buffer_len = 0;
    PaxColumnTypeInMem column_type;

    if ((*columns)[index] == nullptr) {
      continue;
    }

    column = (*columns)[index];
    Assert(index < (size_t)vec_cache_buffer_lens_ && vec_cache_buffer_);

    data_index_begin = column->GetRangeNonNullRows(0, range_begin);
    data_range_lens = column->GetRangeNonNullRows(range_begin, range_lens);

    // data buffer holder
    vec_buffer = &(vec_cache_buffer_[index].vec_buffer);
    offset_buffer = &(vec_cache_buffer_[index].offset_buffer);

    // copy null bitmap
    vec_cache_buffer_[index].null_counts = 0;
    CopyBitmapToVecBuffer(column, micro_partition_visibility_bitmap_,
                          range_begin + micro_partition_row_offset_,
                          range_begin, range_lens, data_range_lens,
                          out_range_lens, &vec_cache_buffer_[index]);

    // copy data
    std::tie(raw_buffer, buffer_len) =
        column->GetRangeBuffer(data_index_begin, data_range_lens);
    column_type = column->GetPaxColumnTypeInMem();

    switch (column_type) {
      case PaxColumnTypeInMem::kTypeBpChar:
      case PaxColumnTypeInMem::kTypeNonFixed: {
        auto raw_data_size = buffer_len - (data_range_lens * VARHDRSZ_SHORT);
        auto align_size = TYPEALIGN(MEMORY_ALIGN_SIZE, raw_data_size);

        auto offset_align_bytes =
            TYPEALIGN(MEMORY_ALIGN_SIZE, (out_range_lens + 1) * sizeof(int32));

        Assert(!vec_buffer->GetBuffer() && !offset_buffer->GetBuffer());
        vec_buffer->Set((char *)cbdb::Palloc(align_size), align_size);

        offset_buffer->Set((char *)cbdb::Palloc0(offset_align_bytes),
                           offset_align_bytes);

        CopyNonFixedRawBuffer(column, micro_partition_visibility_bitmap_,
                              range_begin + micro_partition_row_offset_,
                              range_begin, range_lens, data_index_begin,
                              data_range_lens, offset_buffer, vec_buffer,
                              column_type == PaxColumnTypeInMem::kTypeBpChar);

        break;
      }
      case PaxColumnTypeInMem::kTypeDecimal:
      case PaxColumnTypeInMem::kTypeFixed: {
        Assert(column->GetTypeLength() > 0);
        auto align_size = TYPEALIGN(MEMORY_ALIGN_SIZE,
                                    (out_range_lens * column->GetTypeLength()));
        Assert(!vec_buffer->GetBuffer());

        vec_buffer->Set((char *)cbdb::Palloc(align_size), align_size);
        CopyFixedBuffer(column, micro_partition_visibility_bitmap_,
                        range_begin + micro_partition_row_offset_, range_begin,
                        range_lens, data_index_begin, data_range_lens,
                        vec_buffer);

        break;
      }
      default: {
        CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
      }
    }  // switch column type

  }  // for each column

  current_cached_pax_columns_index_ = range_begin + range_lens;
  cached_batch_lens_ += out_range_lens;

  if (build_ctid_) {
    BuildCtidOffset(range_begin, range_lens);
  }

  return cached_batch_lens_;
}

void VecAdapter::BuildCtidOffset(size_t range_begin, size_t range_lens) {
  auto buffer_len = sizeof(int32) * cached_batch_lens_;
  ctid_offset_in_current_range_ = PAX_NEW<DataBuffer<int32>>(
      (int32 *)cbdb::Palloc(buffer_len), buffer_len, false, false);

  size_t range_row_index = 0;
  for (size_t i = 0; i < cached_batch_lens_; i++) {
    while (micro_partition_visibility_bitmap_ &&
           micro_partition_visibility_bitmap_->Test(range_begin +
                                                    range_row_index) &&
           range_row_index < range_lens) {
      range_row_index++;
    }

    // has loop all visibility map
    if (range_row_index >= range_lens) {
      break;
    }

    (*ctid_offset_in_current_range_)[i] = range_row_index++;
    ctid_offset_in_current_range_->Brush(sizeof(int32));
  }

  //
  Assert(ctid_offset_in_current_range_->GetSize() == cached_batch_lens_);
}

bool VecAdapter::ShouldBuildCtid() const { return build_ctid_; }

void VecAdapter::FullWithCTID(TupleTableSlot *slot,
                              VecBatchBuffer *batch_buffer) {
  auto buffer_len = sizeof(int64) * cached_batch_lens_;
  DataBuffer<int64> ctid_data_buffer((int64 *)cbdb::Palloc(buffer_len),
                                     buffer_len, false, false);

  auto base_offset = GetTupleOffset(slot->tts_tid);

  for (size_t i = 0; i < cached_batch_lens_; i++) {
    SetTupleOffset(&slot->tts_tid,
                   base_offset + (*ctid_offset_in_current_range_)[i]);
    ctid_data_buffer[i] = CTIDToUint64(slot->tts_tid);
  }
  batch_buffer->vec_buffer.Set(ctid_data_buffer.Start(),
                               ctid_data_buffer.Capacity());
  batch_buffer->vec_buffer.SetMemTakeOver(false);
  batch_buffer->vec_buffer.BrushAll();
  PAX_DELETE(ctid_offset_in_current_range_);
}

template <typename T>
static std::pair<bool, size_t> ColumnTransMemory(PaxColumn *column) {
  Assert(column->GetStorageFormat() == PaxStorageFormat::kTypeStoragePorcVec);

  auto vec_column = dynamic_cast<T *>(column);
  auto data_buffer = vec_column->GetDataBuffer();
  if (!data_buffer->IsMemTakeOver()) {
    return {false, 0};
  } else {
    Assert(data_buffer->Capacity() % MEMORY_ALIGN_SIZE == 0);

    data_buffer->SetMemTakeOver(false);
    return {true, data_buffer->Capacity()};
  }
}

bool VecAdapter::AppendVecFormat() {
  PaxColumns *columns;
  PaxColumn *column;

  columns = process_columns_;
  Assert(cached_batch_lens_ == 0);
  Assert(columns->GetRows() <= VEC_BATCH_LENGTH);

  size_t total_rows = columns->GetRows();

  auto null_align_bytes =
      TYPEALIGN(MEMORY_ALIGN_SIZE, BITS_TO_BYTES(total_rows));

  for (size_t index = 0; index < columns->GetColumns(); index++) {
    if ((*columns)[index] == nullptr) {
      continue;
    }

    DataBuffer<char> *vec_buffer = &(vec_cache_buffer_[index].vec_buffer);
    DataBuffer<char> *null_bits_buffer =
        &(vec_cache_buffer_[index].null_bits_buffer);
    DataBuffer<int32> *offset_buffer =
        &(vec_cache_buffer_[index].offset_buffer);

    column = (*columns)[index];
    Assert(index < (size_t)vec_cache_buffer_lens_ && vec_cache_buffer_);

    char *buffer = nullptr;
    size_t buffer_len = 0;
    bool trans_succ = false;
    size_t cap_len = 0;

    vec_cache_buffer_[index].null_counts =
        total_rows - column->GetNonNullRows();

    switch (column->GetPaxColumnTypeInMem()) {
      case PaxColumnTypeInMem::kTypeBpChar: {
        CBDB_RAISE(cbdb::CException::ExType::kExTypeUnImplements);
      }
      case PaxColumnTypeInMem::kTypeNonFixed: {
        Assert(!vec_buffer->GetBuffer());
        Assert(!offset_buffer->GetBuffer());

        std::tie(buffer, buffer_len) = column->GetBuffer();
        std::tie(trans_succ, cap_len) =
            ColumnTransMemory<PaxVecNonFixedColumn>(column);

        if (trans_succ) {
          vec_buffer->Set(buffer, cap_len);
          vec_buffer->BrushAll();
        } else {
          vec_buffer->Set(
              (char *)cbdb::Palloc0(TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len)),
              TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len));
          vec_buffer->Write(buffer, buffer_len);
          vec_buffer->BrushAll();
        }

        auto offset_buffer_from_column =
            dynamic_cast<PaxVecNonFixedColumn *>(column)->GetOffsetBuffer();
        Assert(offset_buffer_from_column);
        buffer = (char *)offset_buffer_from_column->GetBuffer();
        buffer_len = offset_buffer_from_column->Capacity();

        offset_buffer->Set(
            (char *)cbdb::Palloc0(TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len)),
            TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len));
        offset_buffer->Write((int *)buffer, buffer_len);
        offset_buffer->BrushAll();
        break;
      }
      case PaxColumnTypeInMem::kTypeFixed: {
        Assert(!vec_buffer->GetBuffer());
        std::tie(buffer, buffer_len) = column->GetBuffer();

        switch (column->GetTypeLength()) {
          case 1:
            std::tie(trans_succ, cap_len) =
                ColumnTransMemory<PaxVecCommColumn<int8>>(column);
            break;
          case 2:
            std::tie(trans_succ, cap_len) =
                ColumnTransMemory<PaxVecCommColumn<int16>>(column);
            break;
          case 4:
            std::tie(trans_succ, cap_len) =
                ColumnTransMemory<PaxVecCommColumn<int32>>(column);
            break;
          case 8:
            std::tie(trans_succ, cap_len) =
                ColumnTransMemory<PaxVecCommColumn<int64>>(column);
            break;
          default:
            Assert(false);
        }

        if (trans_succ) {
          vec_buffer->Set(buffer, cap_len);
          vec_buffer->BrushAll();
        } else {
          vec_buffer->Set(
              (char *)cbdb::Palloc0(TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len)),
              TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len));
          vec_buffer->Write(buffer, buffer_len);
          vec_buffer->BrushAll();
        }
        break;
      }
      default: {
        CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
      }
    }

    if (column->HasNull()) {
      Bitmap8 *bitmap = nullptr;
      Assert(!null_bits_buffer->GetBuffer());
      null_bits_buffer->Set((char *)cbdb::Palloc(null_align_bytes),
                            null_align_bytes);
      bitmap = column->GetBitmap();
      Assert(bitmap);

      CopyBitmap(bitmap, 0, total_rows, null_bits_buffer);
    }
  }

  current_cached_pax_columns_index_ = total_rows;
  cached_batch_lens_ += total_rows;
  return true;
}

size_t VecAdapter::FlushVecBuffer(TupleTableSlot *slot) {
  // when visibility map is enabled, all rows of a vec batch may be filtered
  // out. this time cached_batch_length is 0
  if (cached_batch_lens_ == 0) {
    return 0;
  }

  std::vector<std::shared_ptr<arrow::Field>> schema_types;
  arrow::ArrayVector array_vector;
  std::vector<std::string> field_names;
  VecTupleTableSlot *vslot = nullptr;
  VecBatchBuffer *vec_batch_buffer = nullptr;
  PaxColumns *columns = nullptr;

  TupleDesc target_desc;

  // column size from current pax columns(which is same size with disk stored)
  // may not equal with `rel_tuple_desc_->natts`, but must LE with
  // `rel_tuple_desc_->natts`
  size_t column_size = 0;
  size_t rc = 0;

  columns = process_columns_;
  Assert(columns);

  vslot = VECSLOT(slot);
  Assert(vslot);

  target_desc = slot->tts_tupleDescriptor;
  column_size = columns->GetColumns();

  Assert(column_size <= (size_t)rel_tuple_desc_->natts);

  // Vec executor is different with cbdb executor
  // if select single column in multi column defined relation
  // then `target_desc->natts` will be one, rather then actually column
  // numbers So we need use `rel_tuple_desc_` which own full relation tuple
  // desc to fill target arrow data
  for (size_t index = 0; index < column_size; index++) {
    auto attr = &rel_tuple_desc_->attrs[index];
    char *column_name = NameStr(attr->attname);

    if ((*columns)[index] == nullptr || attr->attisdropped) {
      continue;
    }

    vec_batch_buffer = &vec_cache_buffer_[index];

    ConvSchemaAndDataToVec(attr->atttypid, column_name, cached_batch_lens_,
                           vec_batch_buffer, schema_types, array_vector,
                           field_names);

    vec_batch_buffer->Reset();
  }

  Assert(schema_types.size() <= (size_t)target_desc->natts);

  // The reason why use we can put null column into `target_desc` is that
  // this situation will only happen when the column is missing in disk.
  // `add column` will make this happen
  // for example
  // 1. CREATE TABLE aa(a int4, b int4) using pax;
  // 2. insert into aa values(...);    // it will generate pax file1 with
  // column a,b
  // 3. alter table aa add c int4;
  // 4. insert into aa values(...);    // it will generate pax file2 with
  // column a,b,c
  // 5. select * from aa;
  //
  // In step5, file1 missing the column c, `schema_types.size()` is 2.
  // So we need full null in it. But in file2, `schema_types.size()` is 3,
  // so do nothing.
  //
  // Notice that: `drop column` will not effect this logic. Because we already
  // deal the `drop column` above(using the relation tuple desc filter the
  // column).
  //
  // A example about `drop column` + `add column`:
  // 1. CREATE TABLE aa(a int4, b int4) using pax;
  // 2. insert into aa values(...);    // it will generate pax file1 with
  // column a,b
  // 3. alter table aa drop b;
  // 4. alter table aa add c int4;
  // 5. insert into aa values(...);    // it will generate pax file2 with
  // column a,c
  // 6. select * from aa; // need column a + column c
  //
  // In step6, file 1 missing the column c, column b in file1 will be filter
  // by `attisdropped` so `schema_types.size()` is 1, we need full null in it.
  // But in file2, `schema_types.size()` is 3, so do nothing.
  auto natts = build_ctid_ ? target_desc->natts - 1 : target_desc->natts;
  Assert((int)schema_types.size() <= natts);
  for (int index = schema_types.size(); index < natts; index++) {
    VecBatchBuffer temp_batch_buffer;
    char *target_column_name = NameStr(target_desc->attrs[index].attname);

    // FIXME(jiaqizho): should setting default value here
    // but missing this part of logic

    // No sure whether can direct add `null()` here
    ConvSchemaAndDataToVec(target_desc->attrs[index].atttypid,
                           target_column_name, cached_batch_lens_,
                           &temp_batch_buffer, schema_types, array_vector,
                           field_names);
  }

  // The CTID will be full with int64(table no(16) + block number(16) +
  // offset(32)) The current value of CTID is accurate, But we cannot get the
  // row data through this CTID. For vectorization, we need to assign CTID
  // datas to the last column of target_list
  if (build_ctid_) {
    Assert((int)schema_types.size() == target_desc->natts - 1);
    VecBatchBuffer ctid_batch_buffer;

    FullWithCTID(slot, &ctid_batch_buffer);
    char *target_column_name =
        NameStr(target_desc->attrs[target_desc->natts - 1].attname);

    ConvSchemaAndDataToVec(target_desc->attrs[target_desc->natts - 1].atttypid,
                           target_column_name, cached_batch_lens_,
                           &ctid_batch_buffer, schema_types, array_vector,
                           field_names);
  }

  Assert(schema_types.size() == (size_t)target_desc->natts);
  Assert(array_vector.size() == schema_types.size());
  Assert(field_names.size() == array_vector.size());

  // `ArrowRecordBatch/ArrowSchema/ArrowArray` alloced by pax memory context.
  // Can not possible to hold the lifecycle of these three objects in pax.
  // It will be freed after memory context reset.
  auto arrow_rb = (ArrowRecordBatch *)cbdb::Palloc0(sizeof(ArrowRecordBatch));

  auto export_status = arrow::ArrowExportTraits<arrow::DataType>::export_func(
      *arrow::struct_(std::move(schema_types)), &arrow_rb->schema);

  CBDB_CHECK(export_status.ok(),
             cbdb::CException::ExType::kExTypeArrowExportError);

  // Don't use the `arrow::ExportArray`
  // Because it will cause memory leak when release call
  // The defualt `release` method won't free the `buffers`,
  // but can free the `private_data` (ExportedArrayPrivateData)
  // After we replace the `release` function. the `private_data` won't be
  // freed.
  auto array = *arrow::StructArray::Make(std::move(array_vector), field_names);
  arrow::ExportArrayRoot(array->data(), &arrow_rb->batch);

  vslot->tts_recordbatch = arrow_rb;

  rc = cached_batch_lens_;
  cached_batch_lens_ = 0;

  return rc;
}

bool VecAdapter::IsInitialized() const { return process_columns_; }

bool VecAdapter::IsEnd() const {
  CBDB_CHECK(process_columns_, cbdb::CException::ExType::kExTypeLogicError);
  return current_cached_pax_columns_index_ == process_columns_->GetRows();
}

}  // namespace pax

#endif  // VEC_BUILD
