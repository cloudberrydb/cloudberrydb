#include "storage/pax_table_partition_writer.h"

#include <cstring>

#include "access/pax_deleter.h"
#include "access/pax_partition.h"
#include "catalog/pax_aux_table.h"
#include "comm/pax_memory.h"
#include "storage/micro_partition_stats.h"

namespace pax {
TableParitionWriter::TableParitionWriter(Relation relation,
                                         PartitionObject *part_obj)
    : TableWriter(relation),
      part_obj_(part_obj),
      writers_(nullptr),
      mp_stats_(nullptr),
      num_tuples_(nullptr),
      current_blocknos_(nullptr),
      writer_counts_(0) {}

TableParitionWriter::~TableParitionWriter() {
  for (int i = 0; i < writer_counts_; i++) {
    PAX_DELETE(writers_[i]);
    PAX_DELETE(mp_stats_[i]);
  }
  PAX_DELETE_ARRAY(writers_);
  PAX_DELETE_ARRAY(mp_stats_);
  PAX_DELETE_ARRAY(num_tuples_);
  PAX_DELETE_ARRAY(current_blocknos_);
}

void TableParitionWriter::WriteTuple(TupleTableSlot *slot) {
  auto part_index = part_obj_->FindPartition(slot);
  Assert(part_index < writer_counts_);
  Assert(strategy_);

  if (part_index == -1) {
    part_index = writer_counts_ - 1;
  }

  if (!writers_[part_index]) {
    Assert(!mp_stats_[part_index]);
    mp_stats_[part_index] = PAX_NEW<MicroPartitionStats>();
    writers_[part_index] = CreateMicroPartitionWriter(mp_stats_[part_index]);

    // insert tuple into the aux table before inserting any tuples.
    current_blocknos_[part_index] = current_blockno_;
    cbdb::InsertMicroPartitionPlaceHolder(RelationGetRelid(relation_),
                                          std::to_string(current_blockno_));
  } else if (strategy_->ShouldSplit(writers_[part_index]->PhysicalSize(),
                                    num_tuples_[part_index])) {
    writers_[part_index]->Close();
    PAX_DELETE(writers_[part_index]);
    writers_[part_index] = CreateMicroPartitionWriter(mp_stats_[part_index]);
    num_tuples_[part_index] = 0;

    // insert tuple into the aux table before inserting any tuples.
    current_blocknos_[part_index] = current_blockno_;
    cbdb::InsertMicroPartitionPlaceHolder(RelationGetRelid(relation_),
                                          std::to_string(current_blockno_));
  }

  writers_[part_index]->WriteTuple(slot);
  num_tuples_[part_index]++;

  SetBlockNumber(&slot->tts_tid, current_blocknos_[part_index]);
}

void TableParitionWriter::Open() {
  // still need init rel_path_, which used to generate next file
  rel_path_ =
      cbdb::BuildPaxDirectoryPath(relation_->rd_node, relation_->rd_backend);
  // 1 for the default parition
  writer_counts_ = part_obj_->NumPartitions() + 1;
  Assert(writer_counts_ > 1);

  writers_ = PAX_NEW_ARRAY<MicroPartitionWriter *>(writer_counts_);
  memset(writers_, 0,
         sizeof(MicroPartitionWriter *) * writer_counts_);  // NOLINT

  mp_stats_ = PAX_NEW_ARRAY<MicroPartitionStats *>(writer_counts_);
  memset(mp_stats_, 0,
         sizeof(MicroPartitionStats *) * writer_counts_);  // NOLINT

  num_tuples_ = PAX_NEW_ARRAY<size_t>(writer_counts_);
  memset(num_tuples_, 0, sizeof(size_t) * writer_counts_);

  current_blocknos_ = PAX_NEW_ARRAY<BlockNumber>(writer_counts_);
}

static inline bool PartIndexIsNear(const int *const inverted_indexes,
                                   int part_counts, int l, int r) {
  Assert(l < part_counts && r < part_counts);
  if (inverted_indexes[l] == -1 || inverted_indexes[r] == -1) {
    return false;
  }

  return inverted_indexes[l] == inverted_indexes[r];
}

static void BuildInvertedPartIndex(
    const std::pair<int *, size_t> &merge_list_info, int *inverted_indexes,
    int part_counts) {
  int *part_range = nullptr;
  size_t part_range_size = 0;
  std::tie(part_range, part_range_size) = merge_list_info;
  int merge_index = 0;
  int nums_of_part = part_range_size / 2;

  Assert(part_range && part_range_size != 0);
  // Must be an even number
  Assert(part_range_size % 2 == 0);

  int i;
  for (i = 0; i < part_counts; i++) {
  retry:
    if (merge_index >= nums_of_part) {
      break;
    }

    auto mid_l = part_range[merge_index * 2];
    auto mid_r = part_range[merge_index * 2 + 1];

    if (i >= mid_l && i <= mid_r) {
      inverted_indexes[i] = merge_index;
    } else if (i > mid_r) {
      merge_index++;
      goto retry;
    } else {  // i < mid_l
      inverted_indexes[i] = -1;
    }
  }

  for (; i < part_counts; i++) {
    inverted_indexes[i] = -1;
  }
}

std::vector<std::vector<size_t>> TableParitionWriter::GetPartitionMergeInfos() {
  int l = 0, r = 1;
  size_t tuples_counts;

  std::vector<std::vector<size_t>> merge_list;
  int part_counts = writer_counts_ - 1;
  std::pair<int *, size_t> near_list = part_obj_->GetMergeListInfo();
  int *inverted_indexes = PAX_NEW_ARRAY<int>(part_counts);
  auto split_tuple_limit = strategy_->SplitTupleNumbers();

  BuildInvertedPartIndex(near_list, inverted_indexes, part_counts);

  auto gen_merge_info = [](size_t left, size_t right) {
    std::vector<size_t> gen;
    for (size_t i = left; i < right; i++) {
      gen.emplace_back(i);
    }
    return gen;
  };

  while (r < part_counts) {
    tuples_counts = 0;
    Assert(r > l);
    // skip the empty writer
    while (!num_tuples_[l] && l < part_counts) {
      l++;
      r = l + 1;
    }

    if (r >= part_counts) {
      break;
    }

    // calculate number of tuple within an [l, r)
    for (int i = l; i < r; i++) {
      tuples_counts += num_tuples_[i];
    }

    // right pointer got empty writer
    // current partition can't merge right partitions
    if (!num_tuples_[r]) {
      if (r - 1 != l) {
        merge_list.emplace_back(gen_merge_info(l, r));
      }
      l = r + 1;
      r = l + 1;
    } else if (tuples_counts <=
               split_tuple_limit) {  // still can merge more partitions
      // no more right partitions
      if (r >= part_counts) {
        break;
      }

      // check current partition is near the last partition
      if (PartIndexIsNear(inverted_indexes, part_counts, r - 1, r)) {
        r++;
      } else if (r - 1 !=
                 l) {  // not nearby last one and not the single partition
        merge_list.emplace_back(gen_merge_info(l, r));
        l = r;
        r = l + 1;
      } else {  // single partition
        l = r;
        r = l + 1;
      }
    } else if (r - l > 1) {  // tuples_counts > split_tuple_limit
      // not the single partition
      merge_list.emplace_back(gen_merge_info(l, r));
      l = r;
      r = l + 1;
    } else {  // single partition, not need add it into list
      l = r;
      r = l + 1;
    }
  }

  Assert(l < r);
  if (l != r - 1) {
    merge_list.emplace_back(gen_merge_info(l, r));
  }

  PAX_DELETE_ARRAY(inverted_indexes);

  return merge_list;
}

void TableParitionWriter::Close() {
  Assert(part_obj_->NumPartitions() + 1 == writer_counts_);
  const auto merge_indexes_list = GetPartitionMergeInfos();

  // When pax uses `TableParitionWriter` to insert data, the file merging
  // operation will be performed in the `Close`.
  //
  // If the current table already build the indexes, Pax can only use
  // delete-update to update the indexes.
  //
  // If the current table is not indexed, pax assumes that the `CTID` in the
  // `TableTupleSlot`(using `SetBlockNumber` + `SetTupleOffset`) has no meaning.
  // which means that we can directly call `writer->MergeTo` to speed up the
  // merge process.
  if (relation_->rd_rel->relhasindex) {
    for (const auto &merge_indexes : merge_indexes_list) {
      Assert(!merge_indexes.empty());

      Snapshot snapshot = nullptr;
      pax::CPaxDeleter del(relation_, snapshot);
      for (size_t i = 0; i < merge_indexes.size(); i++) {
        auto w = writers_[merge_indexes[i]];
        auto block = current_blocknos_[merge_indexes[i]];
        w->Close();
        PAX_DELETE(w);
        writers_[merge_indexes[i]] = nullptr;

        del.MarkDelete(block);
      }
      CommandCounterIncrement();
      del.ExecDelete();
    }
  } else {  // no index
    for (const auto &merge_indexes : merge_indexes_list) {
      auto first_write = writers_[merge_indexes[0]];

      for (size_t i = 1; i < merge_indexes.size(); i++) {
        auto w = writers_[merge_indexes[i]];
        Assert(w);
        first_write->MergeTo(w);
        w->Close();
        PAX_DELETE(w);
        writers_[merge_indexes[i]] = nullptr;

        // Delete the place holder
        // FIXME(jiaqizho): no need open-close per loop. we can open the
        // auxiliary table, delete one tuple per loop, and then close the
        // auxiliary table.
        cbdb::DeleteMicroPartitionEntry(
            RelationGetRelid(relation_), nullptr,
            std::to_string(current_blocknos_[merge_indexes[i]]));
      }
      CommandCounterIncrement();
      first_write->Close();
      PAX_DELETE(first_write);
      writers_[merge_indexes[0]] = nullptr;
    }
  }

  for (int i = 0; i < writer_counts_; i++) {
    if (writers_[i]) {
      writers_[i]->Close();
      PAX_DELETE(writers_[i]);
      writers_[i] = nullptr;
    }
  }
}

}  // namespace pax
