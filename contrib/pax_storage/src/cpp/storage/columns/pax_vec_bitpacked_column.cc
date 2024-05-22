#include "storage/columns/pax_vec_bitpacked_column.h"

namespace pax {

#define BYTES_TO_BITS 8
#define RESIZE_REQUIRE_BYTES 8

PaxVecBitPackedColumn::PaxVecBitPackedColumn(
    uint32 capacity, const PaxEncoder::EncodingOption &encoding_option)
    : PaxVecEncodingColumn(capacity, encoding_option), flat_buffer_(nullptr) {
  bitmap_raw_.bitmap = (uint8 *)data_->Start();
  bitmap_raw_.size = data_->Capacity() * BYTES_TO_BITS;
}

PaxVecBitPackedColumn::PaxVecBitPackedColumn(
    uint32 capacity, const PaxDecoder::DecodingOption &decoding_option)
    : PaxVecEncodingColumn(capacity, decoding_option), flat_buffer_(nullptr) {}

void PaxVecBitPackedColumn::Set(DataBuffer<int8> *data, size_t non_null_rows) {
  PaxVecEncodingColumn::Set(data, non_null_rows);
  bitmap_raw_.bitmap = (uint8 *)data_->Start();
  bitmap_raw_.size = data_->Used() * BYTES_TO_BITS;
}

void PaxVecBitPackedColumn::CheckExpandBitmap() {
  Assert(data_->Capacity() >= RESIZE_REQUIRE_BYTES);
  if (data_->Available() == 0) {
    data_->ReSize(data_->Used() + RESIZE_REQUIRE_BYTES, 2);
    bitmap_raw_.bitmap = (uint8 *)data_->Start();
    bitmap_raw_.size = data_->Capacity() * BYTES_TO_BITS;
  }
}

void PaxVecBitPackedColumn::AppendNull() {
  PaxColumn::AppendNull();
  CheckExpandBitmap();

  bitmap_raw_.Clear(total_rows_ - 1);
  if (total_rows_ % BYTES_TO_BITS == 1) {
    data_->Brush(1);
  }
}

void PaxVecBitPackedColumn::Append(char *buffer, size_t size) {
  PaxColumn::Append(buffer, size);

  CheckExpandBitmap();

  // check buffer
  if (*buffer) {
    bitmap_raw_.Set(total_rows_ - 1);
  } else {
    bitmap_raw_.Clear(total_rows_ - 1);
  }

  if (total_rows_ % BYTES_TO_BITS == 1) {
    data_->Brush(1);
  }
}

std::pair<char *, size_t> PaxVecBitPackedColumn::GetBuffer(size_t position) {
  CBDB_CHECK(position < GetRows(), cbdb::CException::ExType::kExTypeOutOfRange);
  if (!flat_buffer_) {
    flat_buffer_ = new DataBuffer<bool>(non_null_rows_ * sizeof(bool));
  }
  Assert(flat_buffer_->Available() >= sizeof(bool));
  flat_buffer_->Write(bitmap_raw_.Test(position));
  auto buffer = (char *)flat_buffer_->GetAvailableBuffer();
  flat_buffer_->Brush(sizeof(bool));
  return std::make_pair(buffer, sizeof(bool));
}

PaxColumnTypeInMem PaxVecBitPackedColumn::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::kTypeVecBitPacked;
}

}  // namespace pax
