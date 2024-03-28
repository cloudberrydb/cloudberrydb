#include "storage/columns/pax_numeric_column.h"

#include "comm/vec_numeric.h"

namespace pax {

#define NUMERIC_SHORT_HDRSZ sizeof(uint16)

PaxShortNumericColumn::PaxShortNumericColumn(
    uint32 capacity, const PaxEncoder::EncodingOption &encoding_option)
    : PaxEncodingColumn(capacity, encoding_option),
      already_combined_(false),
      width_(VEC_SHORT_NUMERIC_STORE_BYTES) {
  Assert(capacity >= VEC_SHORT_NUMERIC_STORE_BYTES);
}

PaxShortNumericColumn::PaxShortNumericColumn(
    uint32 capacity, const PaxDecoder::DecodingOption &decoding_option)
    : PaxEncodingColumn(capacity, decoding_option),
      already_combined_(false),
      width_(VEC_SHORT_NUMERIC_STORE_BYTES) {}

PaxColumnTypeInMem PaxShortNumericColumn::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::kTypeDecimal;
}

void PaxShortNumericColumn::Set(DataBuffer<int8> *data, size_t non_null_rows) {
  PaxColumn::non_null_rows_ = non_null_rows;
  PaxEncodingColumn::Set(data);
}

PaxShortNumericColumn::~PaxShortNumericColumn() {
  for (auto numeric : numeric_holder_) {
    cbdb::Pfree(numeric);
  }
}

void PaxShortNumericColumn::Append(char *buffer, size_t size) {
  PaxColumn::Append(buffer, size);
  struct varlena *vl;
  size_t vl_len;
  Numeric numeric;

  Assert(data_->Capacity() >= width_);
  if (data_->Available() < width_) {
    data_->ReSize(data_->Used() + width_, 2);
  }

  vl = (struct varlena *)DatumGetPointer(buffer);
  vl_len = VARSIZE_ANY_EXHDR(vl);

  numeric = cbdb::DatumToNumeric(PointerGetDatum(buffer));

  int8 *dest_buff = data_->GetAvailableBuffer();
  pg_short_numeric_to_vec_short_numeric(numeric, vl_len, (int64 *)dest_buff,
                                        (int64 *)(dest_buff + sizeof(int64)));
  data_->Brush(width_);
}

std::pair<char *, size_t> PaxShortNumericColumn::GetRangeBuffer(
    size_t start_pos, size_t len) {
  CBDB_CHECK((start_pos + len) <= GetRows(),
             cbdb::CException::ExType::kExTypeOutOfRange);

  return std::make_pair(data_->Start() + (width_ * start_pos), width_ * len);
}

std::pair<char *, size_t> PaxShortNumericColumn::GetBuffer(size_t position) {
  int8 *raw_buffer;
  Datum datum;
  struct varlena *vl;

  CBDB_CHECK(position < GetNonNullRows(),
             cbdb::CException::ExType::kExTypeOutOfRange);

  Assert(data_->Used() > position * width_);

  raw_buffer = data_->GetBuffer() + (position * width_);
  datum = vec_short_numeric_to_datum((int64 *)raw_buffer,
                                     (int64 *)(raw_buffer + sizeof(int64)));
  Assert(datum != 0);

  numeric_holder_.emplace_back(cbdb::DatumToNumeric(datum));
  vl = (struct varlena *)DatumGetPointer(datum);
  return {DatumGetPointer(datum), VARSIZE_ANY_EXHDR(vl) + VARHDRSZ};
}

DataBuffer<int8> *PaxShortNumericColumn::GetDataBuffer() { return data_; }

ColumnEncoding_Kind PaxShortNumericColumn::GetDefaultColumnType() {
  return ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED;
}

int32 PaxShortNumericColumn::GetTypeLength() const {
  return VEC_SHORT_NUMERIC_STORE_BYTES;
}

}  // namespace pax