#pragma once

#include "comm/cbdb_api.h"

#include <utility>

#include "comm/iterator.h"
#include "storage/micro_partition_metadata.h"

namespace pax {
class MicroPartitionInfoIterator final
: public IteratorBase<MicroPartitionMetadata> {
 public:
  template <typename T, typename... Args>
  friend T *PAX_NEW(Args &&...args);
  static std::unique_ptr<IteratorBase<MicroPartitionMetadata>> New(
      Relation pax_rel, Snapshot snapshot);

  bool HasNext() override;
  MicroPartitionMetadata Next() override;
  void Rewind() override;

 private:
  MicroPartitionInfoIterator(Relation pax_rel, Snapshot snapshot,
                             std::string rel_path);
  ~MicroPartitionInfoIterator() override;
  void Begin();
  void End();
  MicroPartitionMetadata ToValue(HeapTuple tuple);

  std::string rel_path_;
  Relation pax_rel_ = nullptr;
  Relation aux_rel_ = nullptr;
  Snapshot snapshot_ = nullptr;
  SysScanDesc desc_ = nullptr;
  HeapTuple tuple_ = nullptr;
};

}  // namespace pax
