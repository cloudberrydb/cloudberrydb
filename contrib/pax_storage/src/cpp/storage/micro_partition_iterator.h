#pragma once

#include <utility>

#include "comm/cbdb_api.h"
#include "comm/iterator.h"
#include "storage/micro_partition_metadata.h"

namespace pax {
class MicroPartitionInfoIterator final: public IteratorBase<MicroPartitionMetadata> {
 public:
  static std::unique_ptr<IteratorBase<MicroPartitionMetadata>> New(Relation pax_rel, Snapshot snapshot);

  bool HasNext() override;
  MicroPartitionMetadata Next() override;
  void Rewind() override;

 private:
  MicroPartitionInfoIterator(Relation pax_rel, Snapshot snapshot)
  : pax_rel_(pax_rel), snapshot_(snapshot) {}
  ~MicroPartitionInfoIterator() override;
  void Begin();
  void End();
  MicroPartitionMetadata ToValue(HeapTuple tuple);

  Relation pax_rel_ = nullptr;
  Relation aux_rel_ = nullptr;
  Snapshot snapshot_ = nullptr;
  SysScanDesc desc_ = nullptr;
  HeapTuple tuple_ = nullptr;
};

}  // namespace pax
