#include "storage/micro_partition.h"

#include <utility>

#include "comm/pax_memory.h"
#include "storage/pax_filter.h"

namespace pax {

MicroPartitionWriter::MicroPartitionWriter(const WriterOptions &writer_options)
    : writer_options_(writer_options) {}

MicroPartitionWriter *MicroPartitionWriter::SetWriteSummaryCallback(
    WriteSummaryCallback callback) {
  summary_callback_ = callback;
  return this;
}

MicroPartitionWriter *MicroPartitionWriter::SetStatsCollector(
    MicroPartitionStats *mpstats) {
  Assert(mp_stats_ == nullptr);
  mp_stats_ = mpstats;
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

bool MicroPartitionReaderProxy::ReadTuple(TupleTableSlot *slot) {
  Assert(reader_);
  return reader_->ReadTuple(slot);
}

bool MicroPartitionReaderProxy::GetTuple(TupleTableSlot *slot,
                                         size_t row_index) {
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
