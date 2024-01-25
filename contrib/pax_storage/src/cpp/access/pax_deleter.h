#pragma once

#include "comm/cbdb_api.h"

#include <map>
#include <memory>
#include <string>

#include "comm/bitmap.h"
#include "comm/pax_memory.h"
#include "storage/pax.h"

namespace pax {
class CPaxDeleter {
 public:
  explicit CPaxDeleter(Relation rel, Snapshot snapshot);
  ~CPaxDeleter() = default;
  static TM_Result DeleteTuple(Relation relation, ItemPointer tid,
                               CommandId cid, Snapshot snapshot,
                               TM_FailureData *tmfd);

  TM_Result MarkDelete(ItemPointer tid);
  void MarkDelete(BlockNumber pax_block_id);
  void ExecDelete();

 private:
  pax_unique_ptr<IteratorBase<MicroPartitionMetadata>> BuildDeleteIterator();

  std::map<std::string, pax_unique_ptr<Bitmap64>> block_bitmap_map_;
  Relation rel_;
  Snapshot snapshot_;
};  // class CPaxDeleter
}  // namespace pax
