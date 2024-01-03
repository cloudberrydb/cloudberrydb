#include "storage/vec/pax_vec_reader.h"

#include "comm/guc.h"
#include "storage/vec/pax_vec_adapter.h"
#ifdef VEC_BUILD

namespace pax {

PaxVecReader::PaxVecReader(MicroPartitionReader *reader, VecAdapter *adapter,
                           PaxFilter *filter)
    : reader_(reader),
      adapter_(adapter),
      working_group_(nullptr),
      current_group_index_(0),
      filter_(filter),
      ctid_offset_(0) {
  Assert(reader && adapter);
}

PaxVecReader::~PaxVecReader() { delete reader_; }

void PaxVecReader::Open(const ReaderOptions &options) {
  reader_->Open(options);
}

void PaxVecReader::Close() { reader_->Close(); }

bool PaxVecReader::ReadTuple(CTupleSlot *cslot) {
  auto desc = adapter_->GetRelationTupleDesc();
retry_read_group:
  if (!working_group_) {
    if (current_group_index_ >= reader_->GetGroupNums()) {
      return false;
    }
    auto group_index = current_group_index_++;
    auto info = reader_->GetGroupStatsInfo(group_index);
    if (filter_ &&
        !filter_->TestScan(*info, desc, PaxFilterStatisticsKind::kGroup)) {
      goto retry_read_group;
    }

    working_group_ = reader_->ReadGroup(group_index);
    adapter_->SetDataSource(working_group_->GetAllColumns());
  }

  if (!adapter_->AppendToVecBuffer()) {
    delete working_group_;
    working_group_ = nullptr;
    goto retry_read_group;
  }

  size_t flush_nums_of_rows = 0;
  if (adapter_->ShouldBuildCtid()) {
    cslot->SetOffset(ctid_offset_);
    flush_nums_of_rows = adapter_->FlushVecBuffer(cslot);
    ctid_offset_ += flush_nums_of_rows;
  } else {
    flush_nums_of_rows = adapter_->FlushVecBuffer(cslot);
  }

  Assert(flush_nums_of_rows);
  return true;
}

bool PaxVecReader::GetTuple(CTupleSlot *slot, size_t row_index) {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

size_t PaxVecReader::GetGroupNums() {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

std::unique_ptr<ColumnStatsProvider> PaxVecReader::GetGroupStatsInfo(
    size_t group_index) {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
  return nullptr;
}

MicroPartitionReader::Group *PaxVecReader::ReadGroup(size_t index) {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

};  // namespace pax

#endif  // VEC_BUILD
