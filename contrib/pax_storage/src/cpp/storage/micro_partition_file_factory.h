#pragma once

#include <string>

#include "storage/file_system.h"
#include "storage/micro_partition.h"

namespace pax {

#define MICRO_PARTITION_TYPE_PAX "PAX"

// flags is used to control the behavior of the reader
// refer to the definition of ReaderFlags.
typedef enum ReaderFlags {
  FLAGS_DEFAULT = 0,
  FLAGS_VECTOR = 1 << 12,    // use vec_adapter to read and returns the record
                             // batch format required by the vectorized executor
  FLAGS_HAS_CTID = 1 << 13,  // record batch format should build with ctid
} ReaderFlags;

class MicroPartitionFileFactory final {
 public:
  // type must be "pax" now!
  static MicroPartitionWriter *CreateMicroPartitionWriter(
      const std::string &type, File *file,
      const MicroPartitionWriter::WriterOptions &options);

  // type must be "pax" now!
  static MicroPartitionReader *CreateMicroPartitionReader(
      const std::string &type, File *file,
      const MicroPartitionReader::ReaderOptions &options, int flags);
};

}  // namespace pax
