#include "access/pax_deleter.h"

#include <string>
#include <utility>
#include <vector>

#include "access/pax_dml_state.h"
#include "catalog/pax_aux_table.h"
#include "comm/singleton.h"
#include "storage/pax_itemptr.h"
#include "storage/paxc_block_map_manager.h"
namespace pax {
CPaxDeleter::CPaxDeleter(Relation rel, Snapshot snapshot)
    : rel_(rel), snapshot_(snapshot) {}

TM_Result CPaxDeleter::DeleteTuple(Relation relation, ItemPointer tid,
                                   CommandId cid, Snapshot snapshot,
                                   TM_FailureData *tmfd) {
  CPaxDeleter *deleter =
      CPaxDmlStateLocal::Instance()->GetDeleter(relation, snapshot);
  // TODO(gongxun): need more graceful way to pass snapshot
  Assert(deleter != nullptr);
  TM_Result result;
  result = deleter->MarkDelete(tid);
  if (result == TM_SelfModified) {
    tmfd->cmax = cid;
  }
  return result;
}
// used for delete tuples
TM_Result CPaxDeleter::MarkDelete(ItemPointer tid) {
  uint32 tuple_offset = pax::GetTupleOffset(*tid);

  std::string block_id = MapToBlockNumber(rel_, *tid);

  if (block_bitmap_map_.find(block_id) == block_bitmap_map_.end()) {
    block_bitmap_map_[block_id] =
        std::unique_ptr<Bitmap64>(new Bitmap64());  // NOLINT
    cbdb::DeleteMicroPartitionEntry(RelationGetRelid(rel_), snapshot_, block_id);
  }
  auto bitmap = block_bitmap_map_[block_id].get();
  if (bitmap->Test(tuple_offset)) {
    return TM_SelfModified;
  }
  bitmap->Set(tuple_offset);
  return TM_Ok;
}

// used for merge remaining partition files, no tuple needs to delete
void CPaxDeleter::MarkDelete(BlockNumber pax_block_id) {
  std::string block_id = std::to_string(pax_block_id);

  if (block_bitmap_map_.find(block_id) == block_bitmap_map_.end()) {
    block_bitmap_map_[block_id] =
        std::unique_ptr<Bitmap64>(new Bitmap64());
    cbdb::DeleteMicroPartitionEntry(RelationGetRelid(rel_), snapshot_, block_id);
  }
}

void CPaxDeleter::ExecDelete() {
  if (block_bitmap_map_.empty()) return;

  TableDeleter table_deleter(rel_, BuildDeleteIterator(),
                             std::move(block_bitmap_map_), snapshot_);
  table_deleter.Delete();
}

std::unique_ptr<IteratorBase<MicroPartitionMetadata>>
CPaxDeleter::BuildDeleteIterator() {
  std::vector<pax::MicroPartitionMetadata> micro_partitions;
  for (auto &it : block_bitmap_map_) {
    std::string block_id = it.first;
    {
      pax::MicroPartitionMetadata meta_info;

      meta_info.SetFileName(cbdb::BuildPaxFilePath(rel_, block_id));
      meta_info.SetMicroPartitionId(std::move(block_id));
      micro_partitions.push_back(std::move(meta_info));
    }
  }
  IteratorBase<MicroPartitionMetadata> *iter =
      new VectorIterator<MicroPartitionMetadata>(std::move(micro_partitions));

  return std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(iter);
}

}  // namespace pax
