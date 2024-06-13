#include "storage/columns/pax_vec_bpchar_column.h"

namespace pax {

#define NUMBER_OF_CHAR_KEY "CHAR_N_KEY"
#define NUMBER_OF_CHAR_UNINIT -1

PaxVecBpCharColumn::PaxVecBpCharColumn(
    uint32 capacity, uint32 lengths_capacity,
    const PaxEncoder::EncodingOption &encoder_options)
    : PaxVecNonFixedEncodingColumn(capacity, lengths_capacity, encoder_options),
      number_of_char_(NUMBER_OF_CHAR_UNINIT) {}

PaxVecBpCharColumn::PaxVecBpCharColumn(
    uint32 capacity, uint32 lengths_capacity,
    const PaxDecoder::DecodingOption &decoding_option)
    : PaxVecNonFixedEncodingColumn(capacity, lengths_capacity, decoding_option),
      number_of_char_(NUMBER_OF_CHAR_UNINIT) {}

PaxVecBpCharColumn::~PaxVecBpCharColumn() {
  for (auto bpchar_buff : bpchar_holder_) {
    PAX_DELETE(bpchar_buff);
  }
}

void PaxVecBpCharColumn::Append(char *buffer, size_t size) {
  if (number_of_char_ == NUMBER_OF_CHAR_UNINIT) {
#ifdef ENABLE_DEBUG
    if (HasAttributes()) {
      auto ret = GetAttribute(NUMBER_OF_CHAR_KEY);
      if (ret.second) {
        auto value = atoll(ret.first.c_str());
        Assert(size == (size_t)value);
      }
    }
#endif

    // no need pass the n (which from `char(n)`)
    // we can direct get n from size
    number_of_char_ = size;
    PutAttribute(NUMBER_OF_CHAR_KEY, std::to_string(number_of_char_));
  }
  Assert(size == (size_t)number_of_char_);
  auto real_len = bpchartruelen(buffer, size);
  Assert(real_len <= (int)size);
  PaxVecNonFixedEncodingColumn::Append(buffer, real_len);
}

std::pair<char *, size_t> PaxVecBpCharColumn::GetBuffer(size_t position) {
  CBDB_CHECK(position < offsets_->GetSize(),
             cbdb::CException::ExType::kExTypeOutOfRange);
  if (number_of_char_ == NUMBER_OF_CHAR_UNINIT) {
    auto attr = GetAttribute(NUMBER_OF_CHAR_KEY);
    CBDB_CHECK(attr.second, cbdb::CException::ExType::kExTypeInvalidPORCFormat);
    Assert(attr.first.length() > 0);
    number_of_char_ = atoll(attr.first.c_str());
    CBDB_CHECK(number_of_char_ > 0,
               cbdb::CException::ExType::kExTypeInvalidPORCFormat);
  }

  auto pair = PaxVecNonFixedColumn::GetBuffer(position);
  Assert(pair.second <= (size_t)number_of_char_);
  if (pair.second < (size_t)number_of_char_) {
    auto bpchar_buff = PAX_NEW_ARRAY<char>(number_of_char_);
    memcpy(bpchar_buff, pair.first, pair.second);
    memset(bpchar_buff + pair.second, ' ', number_of_char_ - pair.second);
    bpchar_holder_.emplace_back(bpchar_buff);

    return {bpchar_buff, number_of_char_};
  }

  return pair;
}

PaxColumnTypeInMem PaxVecBpCharColumn::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::kTypeVecBpChar;
}
}  // namespace pax
