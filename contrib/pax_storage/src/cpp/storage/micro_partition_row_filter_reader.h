#pragma once

#include "storage/micro_partition.h"

namespace pax {
class MicroPartitionRowFilterReader : public MicroPartitionReaderProxy {
 public:
  static MicroPartitionRowFilterReader *New(
      MicroPartitionReader *reader, PaxFilter *filter,
      Bitmap8 *visibility_bitmap = nullptr);
  MicroPartitionRowFilterReader() = default;
  ~MicroPartitionRowFilterReader() override = default;
  bool ReadTuple(TupleTableSlot *slot) override;
  MicroPartitionReader::Group *GetNextGroup(TupleDesc desc);

 private:
  inline void SetVisibilityBitmap(Bitmap8 *visibility_bitmap) {
    micro_partition_visibility_bitmap_ = visibility_bitmap;
  }

  bool TestRowScanInternal(TupleTableSlot *slot, ExprState *estate,
                           AttrNumber attno);

  // filter is referenced only, the reader doesn't own it.
  PaxFilter *filter_ = nullptr;
  size_t current_group_row_index_ = 0;
  size_t group_index_ = 0;
  MicroPartitionReader::Group *group_ = nullptr;
  // only referenced
  const Bitmap8 *micro_partition_visibility_bitmap_;
};
}  // namespace pax
