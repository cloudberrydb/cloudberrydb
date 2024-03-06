#include "storage/micro_partition_metadata.h"
namespace pax {

WriteSummary::WriteSummary()
    : file_size(0), num_tuples(0), rel_oid(InvalidOid) {}

MicroPartitionMetadata::MicroPartitionMetadata(MicroPartitionMetadata &&other) {
  micro_partition_id_ = std::move(other.micro_partition_id_);
  file_name_ = std::move(other.file_name_);
  tuple_count_ = other.tuple_count_;
  stats_ = std::move(other.stats_);
  visibility_bitmap_file_ = std::move(other.visibility_bitmap_file_);
}

MicroPartitionMetadata &MicroPartitionMetadata::operator=(
    MicroPartitionMetadata &&other) {
  micro_partition_id_ = std::move(other.micro_partition_id_);
  file_name_ = std::move(other.file_name_);
  tuple_count_ = other.tuple_count_;
  stats_ = std::move(other.stats_);
  visibility_bitmap_file_ = std::move(other.visibility_bitmap_file_);
  return *this;
}

}  // namespace pax
