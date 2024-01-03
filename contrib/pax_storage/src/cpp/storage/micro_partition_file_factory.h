#pragma once

#include <string>

#include "storage/file_system.h"
#include "storage/micro_partition.h"

namespace pax {

#define MICRO_PARTITION_TYPE_PAX "PAX"

class MicroPartitionFileFactory final {
 public:
  // type must be "pax" now!
  static MicroPartitionWriter *CreateMicroPartitionWriter(
      const std::string &type, File *file,
      const MicroPartitionWriter::WriterOptions &options);

  // type must be "pax" now!
  static MicroPartitionReader *CreateMicroPartitionReader(
      const std::string &type, File *file,
      const MicroPartitionReader::ReaderOptions &options);
};

}  // namespace pax
