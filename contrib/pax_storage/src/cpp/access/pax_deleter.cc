#include "access/pax_deleter.h"

#include <string>
#include <utility>
#include <vector>

#include "access/pax_dml_state.h"
#include "access/paxc_rel_options.h"
#include "catalog/pax_aux_table.h"
#include "comm/singleton.h"
#include "storage/pax_itemptr.h"
namespace pax {
CPaxDeleter::CPaxDeleter(Relation rel, Snapshot snapshot)
    : rel_(rel), snapshot_(snapshot) {
  delete_xid_ = GetCurrentTransactionId();
  Assert(TransactionIdIsValid(delete_xid_));

  use_visimap_ = true;
}

TM_Result CPaxDeleter::DeleteTuple(Relation relation, ItemPointer tid,
                                   CommandId cid, Snapshot snapshot,
                                   TM_FailureData *tmfd) {
  CPaxDeleter *deleter =
      CPaxDmlStateLocal::Instance()->GetDeleter(relation, snapshot);
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
        pax_shared_ptr<Bitmap8>(PAX_NEW<Bitmap8>());  // NOLINT
    if(!use_visimap_) {
      cbdb::DeleteMicroPartitionEntry(RelationGetRelid(rel_), snapshot_,
                                    block_id);
    }
  }
  auto bitmap = block_bitmap_map_[block_id].get();
  if (bitmap->Test(tuple_offset)) {
    return TM_SelfModified;
  }
  bitmap->Set(tuple_offset);
  return TM_Ok;
}

bool CPaxDeleter::IsMarked(ItemPointerData tid) const {
  std::string block_id = MapToBlockNumber(rel_, tid);
  auto it = block_bitmap_map_.find(block_id);

  if (it == block_bitmap_map_.end()) return false;

  const auto &bitmap = it->second;
  uint32 tuple_offset = pax::GetTupleOffset(tid);
  return bitmap->Test(tuple_offset);
}

// used for merge remaining partition files, no tuple needs to delete
void CPaxDeleter::MarkDelete(BlockNumber pax_block_id) {
  std::string block_id = std::to_string(pax_block_id);

  if (block_bitmap_map_.find(block_id) == block_bitmap_map_.end()) {
    block_bitmap_map_[block_id] = pax_shared_ptr<Bitmap8>(PAX_NEW<Bitmap8>());
    cbdb::DeleteMicroPartitionEntry(RelationGetRelid(rel_), snapshot_,
                                    block_id);
  }
}

void CPaxDeleter::ExecDelete() {
  if (block_bitmap_map_.empty()) return;

  TableDeleter table_deleter(rel_, BuildDeleteIterator(), block_bitmap_map_,
                             snapshot_);
  if (use_visimap_) {
    table_deleter.DeleteWithVisibilityMap(delete_xid_);
  } else {
    table_deleter.Delete();
  }
}

pax_unique_ptr<IteratorBase<MicroPartitionMetadata>>
CPaxDeleter::BuildDeleteIterator() {
  std::vector<pax::MicroPartitionMetadata> micro_partitions;
  auto rel_path = cbdb::BuildPaxDirectoryPath(rel_->rd_node, rel_->rd_backend);
  for (auto &it : block_bitmap_map_) {
    std::string block_id = it.first;
    {
      pax::MicroPartitionMetadata meta_info;

      meta_info.SetFileName(cbdb::BuildPaxFilePath(rel_path, block_id));
      meta_info.SetMicroPartitionId(std::move(block_id));
      micro_partitions.push_back(std::move(meta_info));
    }
  }
  IteratorBase<MicroPartitionMetadata> *iter =
      PAX_NEW<VectorIterator<MicroPartitionMetadata>>(
          std::move(micro_partitions));

  return pax_unique_ptr<IteratorBase<MicroPartitionMetadata>>(iter);
}

}  // namespace pax
