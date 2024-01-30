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

  VecAdapter(TupleDesc tuple_desc, bool build_ctid = false);

  ~VecAdapter();

  void SetDataSource(PaxColumns *columns);

  bool IsInitialized() const;

  bool IsEnd() const;

  bool AppendToVecBuffer();

  size_t FlushVecBuffer(TupleTableSlot *slot);

  const TupleDesc GetRelationTupleDesc() const;

  bool ShouldBuildCtid() const;

 private:
  void FullWithCTID(TupleTableSlot *slot, VecBatchBuffer *batch_buffer);
  bool AppendVecFormat();

 private:
  TupleDesc rel_tuple_desc_;
  size_t cached_batch_lens_;
  VecBatchBuffer *vec_cache_buffer_;
  int vec_cache_buffer_lens_;

  PaxColumns *process_columns_;
  size_t current_cached_pax_columns_index_;
  bool build_ctid_;
};

}  // namespace pax

#endif  // #ifdef VEC_BUILD
