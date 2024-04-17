#include "storage/micro_partition_file_factory.h"

#include "comm/pax_memory.h"
#include "storage/micro_partition.h"
#include "storage/orc/porc.h"
#include "storage/vec/pax_vec_adapter.h"
#include "storage/vec/pax_vec_reader.h"

namespace pax {
MicroPartitionReader *MicroPartitionFileFactory::CreateMicroPartitionReader(
    const std::string &type, File *file,
    const MicroPartitionReader::ReaderOptions &options, int flags) {
  CBDB_CHECK(type == MICRO_PARTITION_TYPE_PAX,
             cbdb::CException::ExType::kExTypeLogicError);

  MicroPartitionReader *reader = PAX_NEW<OrcReader>(file);
#ifdef VEC_BUILD
  if (flags & ReaderFlags::FLAGS_VECTOR) {
    auto vec_adapter_ptr = std::make_shared<VecAdapter>(
        options.desc, (flags & ReaderFlags::FLAGS_HAS_CTID) != 0);
    reader = PAX_NEW<PaxVecReader>(reader, vec_adapter_ptr, options.filter);
  }
#endif

  reader->Open(options);
  return reader;
}

MicroPartitionWriter *MicroPartitionFileFactory::CreateMicroPartitionWriter(
    const std::string &type, File *file,
    const MicroPartitionWriter::WriterOptions &options) {
  if (type == MICRO_PARTITION_TYPE_PAX) {
    std::vector<pax::porc::proto::Type_Kind> type_kinds;
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
