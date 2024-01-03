#include "storage/strategy.h"

#include "storage/micro_partition.h"

namespace pax {

size_t PaxDefaultSplitStrategy::SplitTupleNumbers() const {
  // The reason why we chose 16384 as a separator value
  // is because in the vectorized version, the number of
  // rows returned by each tuple cannot be greater than 16384
  // and needs to be as close as possible to this value
  return 16384 * 10;
}

size_t PaxDefaultSplitStrategy::SplitFileSize() const {
  return 64 * 1024 * 1024;
}

bool PaxDefaultSplitStrategy::ShouldSplit(size_t phy_size,
                                          size_t num_tuples) const {
  return (num_tuples >= SplitTupleNumbers()) || (phy_size >= SplitFileSize());
}

}  // namespace pax
