#pragma once

#include "comm/cbdb_api.h"

#include <vector>

namespace pax {
class PartitionObject;
}

struct PartitionRangeExtension {
  struct PartitionBoundSpec spec;
  List *every;
};
namespace paxc {

extern PartitionKey PaxRelationBuildPartitionKey(Relation relation,
                                                 List *partparams_list);
extern bool PartitionCheckBounds(PartitionKey key, int nparts,
                                 PartitionBoundSpec **partboundspecs);
extern List *PaxValidatePartitionRanges(Relation relation, PartitionKey key,
                                        List *raw_partbound_list);

class PartitionObjectInternal {
 public:
  bool Initialize(Relation pax_rel);
  void Release();

  // Get number of partitions, excluding the default partition
  int NumPartitions() const;
  int NumPartitionKeys() const;
  // -1 if default partition, >=0 leaf partition
  int FindPartition(TupleTableSlot *slot);
  PartitionKey GetPartitionKey() { return partition_key_; }
  PartitionDesc GetPartitionDesc() { return partition_desc_; }

 private:
  void InitializeMergeInfo(PartitionKey key, List *partboundspec_list,
                           MemoryContext tmp_ctx, MemoryContext target_ctx);

  friend class pax::PartitionObject;
  Relation pax_rel_ = nullptr;
  PartitionKey partition_key_ = nullptr;
  PartitionDesc partition_desc_ = nullptr;
  int *merge_index_ = nullptr;
  size_t merge_len_ = 0;
  List *partition_bound_spec_ = nullptr;
  MemoryContext mctx_ = nullptr;
};
}  // namespace paxc

namespace pax {
class PartitionObject {
 public:
  bool Initialize(Relation pax_rel);
  void Release();

  PartitionKey GetPartitionKey() { return stub_.GetPartitionKey(); }
  PartitionDesc GetPartitionDesc() { return stub_.GetPartitionDesc(); }

  // Get number of partitions, excluding the default partition
  int NumPartitions() const { return stub_.NumPartitions(); }
  // Get number of the partition keys
  int NumPartitionKeys() const { return stub_.NumPartitionKeys(); }

  // -1 if default partition, >= 0 leaf partition
  int FindPartition(TupleTableSlot *slot);

  std::pair<int *, size_t> GetMergeListInfo();

 private:
  paxc::PartitionObjectInternal stub_;
};

}  // namespace pax
