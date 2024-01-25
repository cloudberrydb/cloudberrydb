#include "storage/micro_partition.h"

#include <utility>

#include "comm/pax_memory.h"
#include "storage/pax_filter.h"
#include "storage/pax_itemptr.h"

namespace pax {

CTupleSlot::CTupleSlot(TupleTableSlot *tuple_slot)
    : slot_(tuple_slot), ctid_() {
  ItemPointerSetInvalid(&ctid_);
}

void CTupleSlot::StoreVirtualTuple() {
  slot_->tts_tid = ctid_;
  slot_->tts_flags &= ~TTS_FLAG_EMPTY;
  slot_->tts_nvalid = slot_->tts_tupleDescriptor->natts;
}

#ifndef ENABLE_LOCAL_INDEX
void CTupleSlot::SetTableNo(uint8 table_no) {
  pax::SetTableNo(&ctid_, table_no);
}
#endif

void CTupleSlot::SetBlockNumber(uint32 block_number) {
  pax::SetBlockNumber(&ctid_, block_number);
}
void CTupleSlot::SetOffset(uint32 offset) {
  pax::SetTupleOffset(&ctid_, offset);
}

TupleDesc CTupleSlot::GetTupleDesc() const {
  return slot_->tts_tupleDescriptor;
}

TupleTableSlot *CTupleSlot::GetTupleTableSlot() const { return slot_; }

MicroPartitionWriter::MicroPartitionWriter(const WriterOptions &writer_options)
    : writer_options_(writer_options) {}

MicroPartitionWriter *MicroPartitionWriter::SetWriteSummaryCallback(
    WriteSummaryCallback callback) {
  summary_callback_ = callback;
  return this;
}

MicroPartitionWriter *MicroPartitionWriter::SetStatsCollector(
    MicroPartitionStats *mpstats) {
  Assert(mpstats_ == nullptr);
  mpstats_ = mpstats;
  return this;
}

MicroPartitionReaderProxy::~MicroPartitionReaderProxy() { PAX_DELETE(reader_); }

void MicroPartitionReaderProxy::Open(
    const MicroPartitionReader::ReaderOptions &options) {
  Assert(reader_);
  reader_->Open(options);
}

void MicroPartitionReaderProxy::Close() {
  Assert(reader_);
  reader_->Close();
}

bool MicroPartitionReaderProxy::ReadTuple(CTupleSlot *slot) {
  Assert(reader_);
  return reader_->ReadTuple(slot);
}

bool MicroPartitionReaderProxy::GetTuple(CTupleSlot *slot, size_t row_index) {
  Assert(reader_);
  return reader_->GetTuple(slot, row_index);
}

void MicroPartitionReaderProxy::SetReader(MicroPartitionReader *reader) {
  Assert(reader);
  Assert(!reader_);
  reader_ = reader;
}

size_t MicroPartitionReaderProxy::GetGroupNums() {
  return reader_->GetGroupNums();
}

std::unique_ptr<ColumnStatsProvider>
MicroPartitionReaderProxy::GetGroupStatsInfo(size_t group_index) {
  return std::move(reader_->GetGroupStatsInfo(group_index));
}

MicroPartitionReader::Group *MicroPartitionReaderProxy::ReadGroup(
    size_t index) {
  return reader_->ReadGroup(index);
}

}  // namespace pax
