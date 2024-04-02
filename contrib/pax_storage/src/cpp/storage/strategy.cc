#include "storage/strategy.h"

#include "comm/guc.h"
#include "storage/micro_partition.h"

namespace pax {

size_t PaxDefaultSplitStrategy::SplitTupleNumbers() const {
  // The reason why we chose 16384 as a separator value
  // is because in the vectorized version, the number of
  // rows returned by each tuple cannot be greater than 16384
  // and needs to be as close as possible to this value
  return pax_max_tuples_per_file;
}

size_t PaxDefaultSplitStrategy::SplitFileSize() const {
  return pax_max_size_per_file;
}

bool PaxDefaultSplitStrategy::ShouldSplit(size_t phy_size,
                                          size_t num_tuples) const {
  return (num_tuples >= SplitTupleNumbers()) || (phy_size >= SplitFileSize());
}

}  // namespace pax
