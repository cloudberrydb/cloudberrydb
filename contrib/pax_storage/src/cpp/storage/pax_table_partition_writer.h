#pragma once
#include <vector>

#include "storage/micro_partition.h"
#include "storage/pax.h"

namespace pax {
class PartitionObject;
class TableParitionWriter : public TableWriter {
 public:
  explicit TableParitionWriter(Relation relation, PartitionObject *part_obj);

  ~TableParitionWriter() override;

  void WriteTuple(CTupleSlot *slot) override;

  void Open() override;

  void Close() override;

#ifndef RUN_GTEST
 private:
#endif
  std::vector<std::vector<size_t>> GetPartitionMergeInfos();

 private:
  PartitionObject *part_obj_;
  MicroPartitionWriter **writers_;
  MicroPartitionStats **mp_stats_;
  size_t *num_tuples_;
  BlockNumber *current_blocknos_;

  int writer_counts_;
};

}  // namespace pax
