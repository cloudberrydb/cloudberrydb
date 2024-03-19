#include "storage/micro_partition_file_factory.h"

#include "comm/pax_memory.h"
#include "storage/orc/orc.h"

namespace pax {
MicroPartitionReader *MicroPartitionFileFactory::CreateMicroPartitionReader(
    const std::string &type, File *file,
    const MicroPartitionReader::ReaderOptions &options) {
  if (type == MICRO_PARTITION_TYPE_PAX) {
    MicroPartitionReader *reader = PAX_NEW<OrcReader>(file);

    reader->Open(options);
    return reader;
  }

  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

MicroPartitionWriter *MicroPartitionFileFactory::CreateMicroPartitionWriter(
    const std::string &type, File *file,
    const MicroPartitionWriter::WriterOptions &options) {
  if (type == MICRO_PARTITION_TYPE_PAX) {
    std::vector<pax::orc::proto::Type_Kind> type_kinds;
    MicroPartitionWriter *writer = nullptr;
    type_kinds =
        OrcWriter::BuildSchema(options.desc, options.numeric_vec_storage);
    writer =
        PAX_NEW<OrcWriter>(std::move(options), std::move(type_kinds), file);
    return writer;
  }
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

}  // namespace pax
