#include "storage/pax_table_partition_writer.h"

#include <cstring>

#include "access/pax_deleter.h"
#include "access/pax_partition.h"
#include "catalog/pax_aux_table.h"
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
    delete writers_[i];
    delete mp_stats_[i];
  }
  delete[] writers_;
  delete[] mp_stats_;
  delete[] num_tuples_;
  delete[] current_blocknos_;
}

void TableParitionWriter::WriteTuple(CTupleSlot *slot) {
  auto part_index = part_obj_->FindPartition(slot->GetTupleTableSlot());
  Assert(part_index < writer_counts_);
  Assert(strategy_);

  if (part_index == -1) {
    part_index = writer_counts_ - 1;
  }

  if (!writers_[part_index]) {
    Assert(!mp_stats_[part_index]);
    mp_stats_[part_index] = new MicroPartitionStats();
    writers_[part_index] = CreateMicroPartitionWriter(mp_stats_[part_index]);
#ifdef ENABLE_LOCAL_INDEX
// insert tuple into the aux table before inserting any tuples.
    current_blocknos_[part_index] = current_blockno_;
    cbdb::InsertMicroPartitionPlaceHolder(RelationGetRelid(relation_), std::to_string(current_blockno_));
#endif
  }

  if (strategy_->ShouldSplit(writers_[part_index]->PhysicalSize(),
                             num_tuples_[part_index])) {
    writers_[part_index]->Close();
    delete writers_[part_index];
    writers_[part_index] = CreateMicroPartitionWriter(mp_stats_[part_index]);
    num_tuples_[part_index] = 0;
#ifdef ENABLE_LOCAL_INDEX
// insert tuple into the aux table before inserting any tuples.
    current_blocknos_[part_index] = current_blockno_;
    cbdb::InsertMicroPartitionPlaceHolder(RelationGetRelid(relation_), std::to_string(current_blockno_));
#endif
  }

  if (TableWriter::mp_stats_) {
    mp_stats_[part_index]->AddRow(slot->GetTupleTableSlot());
  }

  writers_[part_index]->WriteTuple(slot);
  num_tuples_[part_index]++;
#ifdef ENABLE_LOCAL_INDEX
  slot->SetBlockNumber(current_blocknos_[part_index]);
  slot->StoreVirtualTuple();
#endif
}

void TableParitionWriter::Open() {
  // 1 for the default parition
  writer_counts_ = part_obj_->NumPartitions() + 1;
  Assert(writer_counts_ > 1);

  writers_ = new MicroPartitionWriter *[writer_counts_];
  memset(writers_, 0,
         sizeof(MicroPartitionWriter *) * writer_counts_);  // NOLINT

  mp_stats_ = new MicroPartitionStats *[writer_counts_];
  memset(mp_stats_, 0,
         sizeof(MicroPartitionStats *) * writer_counts_);  // NOLINT

  num_tuples_ = new size_t[writer_counts_];
  memset(num_tuples_, 0, sizeof(size_t) * writer_counts_);

  current_blocknos_ = new BlockNumber[writer_counts_];
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
  int *inverted_indexes = new int[part_counts];
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

  delete[] inverted_indexes;

  return merge_list;
}

void TableParitionWriter::Close() {
  Assert(part_obj_->NumPartitions() + 1 == writer_counts_);
  const auto merge_indexes_list = GetPartitionMergeInfos();

  for (const auto &merge_indexes : merge_indexes_list) {
    Assert(!merge_indexes.empty());

#ifdef ENABLE_LOCAL_INDEX
    {
      Snapshot snapshot = nullptr;
      pax::CPaxDeleter del(relation_, snapshot);
      for (size_t i = 0; i < merge_indexes.size(); i++) {
        auto w = writers_[merge_indexes[i]];
        auto block = current_blocknos_[merge_indexes[i]];
        w->Close();
        delete w;
        writers_[merge_indexes[i]] = nullptr;

        del.MarkDelete(block);
      }
      CommandCounterIncrement();
      del.ExecDelete();
    }
#else
    {
      auto first_write = writers_[merge_indexes[0]];

      for (size_t i = 1; i < merge_indexes.size(); i++) {
        auto w = writers_[merge_indexes[i]];
        Assert(w);
        first_write->MergeTo(w);
        w->Close();
        delete w;
        writers_[merge_indexes[i]] = nullptr;
      }
      CommandCounterIncrement();
      first_write->Close();
      delete first_write;
      writers_[merge_indexes[0]] = nullptr;
    }
#endif
  }

  for (int i = 0; i < writer_counts_; i++) {
    if (writers_[i]) {
      writers_[i]->Close();
      delete writers_[i];
      writers_[i] = nullptr;
    }
  }
}

}  // namespace pax
