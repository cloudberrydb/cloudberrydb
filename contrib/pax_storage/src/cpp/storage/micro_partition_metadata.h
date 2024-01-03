#pragma once
#include "comm/cbdb_api.h"
#include "storage/proto/proto_wrappers.h"

#include <string>
#include <utility>

namespace pax {
// WriteSummary is generated after the current micro partition is flushed and
// closed.
struct WriteSummary {
  std::string file_name;
  std::string block_id;
  size_t file_size;
  size_t num_tuples;
  unsigned int rel_oid;
  pax::stats::MicroPartitionStatisticsInfo mp_stats;
  WriteSummary();
  WriteSummary(const WriteSummary &summary) = default;
};

struct MicroPartitionMetadata {
 public:
  MicroPartitionMetadata() = default;
  MicroPartitionMetadata(const MicroPartitionMetadata &other) = default;

  ~MicroPartitionMetadata() = default;

  MicroPartitionMetadata(MicroPartitionMetadata &&other);

  MicroPartitionMetadata &operator=(const MicroPartitionMetadata &other) = default;

  MicroPartitionMetadata &operator=(MicroPartitionMetadata &&other);

  inline const std::string &GetMicroPartitionId() const { return micro_partition_id_; }

  inline const std::string &GetFileName() const { return file_name_; }

  inline uint32 GetTupleCount() const { return tuple_count_; }

  inline const ::pax::stats::MicroPartitionStatisticsInfo &GetStats() const { return stats_; }

  inline void SetMicroPartitionId(std::string &&id) { micro_partition_id_ = std::move(id); }

  inline void SetFileName(std::string &&name) { file_name_ = std::move(name); }

  inline void SetTupleCount(uint32 tuple_count) { tuple_count_ = tuple_count; }

  inline void SetStats(::pax::stats::MicroPartitionStatisticsInfo &&stats) { stats_ = std::move(stats); }

 private:
  std::string micro_partition_id_;

  std::string file_name_;

  // statistics info
  uint32 tuple_count_ = 0;

  ::pax::stats::MicroPartitionStatisticsInfo stats_;
};  // class MicroPartitionMetadata
}  // namespace pax
