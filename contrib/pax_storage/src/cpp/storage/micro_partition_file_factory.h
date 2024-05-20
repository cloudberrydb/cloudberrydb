#pragma once

#include <string>

#include "storage/file_system.h"
#include "storage/micro_partition.h"

namespace pax {

// flags is used to control the behavior of the reader
// refer to the definition of ReaderFlags.
enum ReaderFlags {
  FLAGS_EMPTY = 0UL,
  FLAGS_VECTOR_PATH =
      1L << 0,  // use vec_adapter to read and returns the record
                // batch format required by the vectorized executor
  FLAGS_SCAN_WITH_CTID = 1L << 1,  // record batch format should build with ctid
};

#define READER_FLAG_SET_VECTOR_PATH(flags) (flags) |= FLAGS_VECTOR_PATH;

#define READER_FLAG_SET_SCAN_WITH_CTID(flags) (flags) |= FLAGS_SCAN_WITH_CTID;

class MicroPartitionFileFactory final {
 public:
  static MicroPartitionWriter *CreateMicroPartitionWriter(
      const MicroPartitionWriter::WriterOptions &options, File *file);

  static MicroPartitionReader *CreateMicroPartitionReader(
      const MicroPartitionReader::ReaderOptions &options, int32 flags,
      File *file);
};

}  // namespace pax
