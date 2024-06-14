#pragma once

#ifdef VEC_BUILD

#include "storage/columns/pax_columns.h"
#include "storage/micro_partition.h"
#include "storage/pax_defined.h"

namespace pax {

class VecAdapter final {
 public:
  struct VecBatchBuffer {
    DataBuffer<char> vec_buffer;
    DataBuffer<char> null_bits_buffer;
    DataBuffer<int32> offset_buffer;
    size_t null_counts;

    VecBatchBuffer();

    void Reset();

    void SetMemoryTakeOver(bool take);
  };

  VecAdapter(TupleDesc tuple_desc, const int max_batch_size,
             bool build_ctid = false);

  ~VecAdapter();

  void SetDataSource(PaxColumns *columns, int group_base_offset);

  bool IsInitialized() const;

  // return -1,0 or batch_cache_lens
  // value -1 means: no remaining tuples in pax_columns
  // value 0 means: all this batch of data have been skipped and FlushVecBuffer
  // is not required, but pax_columns still has unprocessed data.
  // value n > 0 means: this batch of data has N tuples that need to be
  // converted into vec record batch
  int AppendToVecBuffer();

  size_t FlushVecBuffer(TupleTableSlot *slot);

  TupleDesc GetRelationTupleDesc() const;

  bool ShouldBuildCtid() const;

  inline void SetVisibitilyMapInfo(std::shared_ptr<Bitmap8> visibility_bitmap) {
    micro_partition_visibility_bitmap_ = std::move(visibility_bitmap);
  }

  static int GetMaxBatchSizeFromStr(char *max_batch_size_str,
                                    int default_value);

 private:
  void FullWithCTID(TupleTableSlot *slot, VecBatchBuffer *batch_buffer);
  void FillMissColumn(int attr_index);

  std::pair<size_t, size_t> AppendPorcFormat(PaxColumns *columns,
                                             size_t range_begin,
                                             size_t range_lens);
  std::pair<size_t, size_t> AppendPorcVecFormat(PaxColumns *columns);

  inline size_t GetInvisibleNumber(size_t range_begin, size_t range_lens) {
    if (micro_partition_visibility_bitmap_ == nullptr) {
      return 0;
    }

    // The number of bits which set to 1 in visibility map is the number of
    // invisible tuples
    auto begin = group_base_offset_ + range_begin;
    return micro_partition_visibility_bitmap_->CountBits(
        begin, begin + range_lens - 1);
  }

  void BuildCtidOffset(size_t range_begin, size_t range_lengs);

 private:
  TupleDesc rel_tuple_desc_;
  const int max_batch_size_;
  size_t cached_batch_lens_;
  VecBatchBuffer *vec_cache_buffer_;
  int vec_cache_buffer_lens_;

  PaxColumns *process_columns_;
  size_t current_index_;
  bool build_ctid_;

  int group_base_offset_;
  // only referenced
  std::shared_ptr<Bitmap8> micro_partition_visibility_bitmap_ = nullptr;

  // ctid offset in current batch range
  DataBuffer<int32> *ctid_offset_in_current_range_;
};
}  // namespace pax

#endif  // #ifdef VEC_BUILD
