#pragma once

#include "storage/micro_partition.h"

namespace pax {
class MicroPartitionRowFilterReader : public MicroPartitionReaderProxy {
 public:
  static MicroPartitionRowFilterReader *New(MicroPartitionReader *reader, PaxFilter *filter);
  MicroPartitionRowFilterReader() = default;
  ~MicroPartitionRowFilterReader() override = default;
  bool ReadTuple(CTupleSlot *slot) override;
  MicroPartitionReader::Group *GetNextGroup(TupleDesc desc);

 private:
  bool TestRowScanInternal(TupleTableSlot *slot, ExprState *estate, AttrNumber attno);

  // filter is referenced only, the reader doesn't own it.
  PaxFilter *filter_ = nullptr;
  size_t row_index_ = 0;
  size_t group_index_ = 0;
  size_t local_index_ = 0;
  MicroPartitionReader::Group *group_ = nullptr;
};
}
