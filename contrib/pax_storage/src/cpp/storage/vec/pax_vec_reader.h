#pragma once
#include "storage/micro_partition.h"

#ifdef VEC_BUILD

namespace pax {

class PaxFilter;
class VecAdapter;

class PaxVecReader : public MicroPartitionReader {
 public:
  // If enable read tuple from vec reader,
  // then OrcReader will be hold by PaxVecReader,
  // current MicroPartitionReader lifecycle will be bound to the PaxVecReader)
  PaxVecReader(MicroPartitionReader *reader,
               std::shared_ptr<VecAdapter> adapter, PaxFilter *filter);

  ~PaxVecReader() override;

  void Open(const ReaderOptions &options) override;

  void Close() override;

  bool ReadTuple(TupleTableSlot *slot) override;

  bool GetTuple(TupleTableSlot *slot, size_t row_index) override;

  size_t GetGroupNums() override;

  std::unique_ptr<ColumnStatsProvider> GetGroupStatsInfo(
      size_t group_index) override;

  MicroPartitionReader::Group *ReadGroup(size_t index) override;

 private:
  MicroPartitionReader *reader_;
  std::shared_ptr<VecAdapter> adapter_;

  MicroPartitionReader::Group *working_group_;
  size_t current_group_index_;
  PaxFilter *filter_;

  size_t ctid_offset_;

  // only referenced, owner by caller who constructed ReadOptions
  Bitmap8 *visibility_bitmap_ = nullptr;
};

}  // namespace pax

#endif  // VEC_BUILD
