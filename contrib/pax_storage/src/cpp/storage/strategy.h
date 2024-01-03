#pragma once

#include <cstddef>

namespace pax {
class MicroPartitionWriter;
class FileSplitStrategy {
 public:
  virtual ~FileSplitStrategy() = default;

  virtual bool ShouldSplit(size_t phy_size, size_t num_tuples) const = 0;

  virtual size_t SplitTupleNumbers() const = 0;

  virtual size_t SplitFileSize() const = 0;
};

class PaxDefaultSplitStrategy final : public FileSplitStrategy {
 public:
  PaxDefaultSplitStrategy() = default;
  ~PaxDefaultSplitStrategy() override = default;

  size_t SplitTupleNumbers() const override;

  size_t SplitFileSize() const override;

  bool ShouldSplit(size_t phy_size, size_t num_tuples) const override;
};
}  // namespace pax
