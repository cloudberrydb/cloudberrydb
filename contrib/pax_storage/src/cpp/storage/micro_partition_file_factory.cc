#include "storage/micro_partition_file_factory.h"

#include "comm/pax_memory.h"
#include "storage/micro_partition.h"
#include "storage/micro_partition_row_filter_reader.h"
#include "storage/orc/porc.h"
#include "storage/vec/pax_vec_adapter.h"
#include "storage/vec/pax_vec_reader.h"

namespace pax {
MicroPartitionReader *MicroPartitionFileFactory::CreateMicroPartitionReader(
    const MicroPartitionReader::ReaderOptions &options, int32 flags, File *file,
    File *toast_file) {
  MicroPartitionReader *reader = PAX_NEW<OrcReader>(file, toast_file);

#ifdef VEC_BUILD
  if (flags & ReaderFlags::FLAGS_VECTOR_PATH) {
    auto max_batch_size = VecAdapter::GetMaxBatchSizeFromStr(
        cbdb::GetGUCConfigOptionByName(VECTOR_MAX_BATCH_SIZE_GUC_NAME, NULL,
                                       true),
        VEC_BATCH_LENGTH);

    // The max of record batch size must align with 8
    // Because the begin bits of the null bitmap in pax must be aligned 8
    CBDB_CHECK(max_batch_size > 0 && (max_batch_size % MEMORY_ALIGN_SIZE == 0),
               cbdb::CException::kExTypeInvalid);

    auto vec_adapter_ptr = std::make_shared<VecAdapter>(
        options.tuple_desc, max_batch_size,
        (flags & ReaderFlags::FLAGS_SCAN_WITH_CTID) != 0);
    reader = PAX_NEW<PaxVecReader>(reader, vec_adapter_ptr, options.filter);
  } else
#endif
      if (options.filter && options.filter->HasRowScanFilter()) {
    reader = MicroPartitionRowFilterReader::New(reader, options.filter,
                                                options.visibility_bitmap);
  }

  reader->Open(options);
  return reader;
}

MicroPartitionWriter *MicroPartitionFileFactory::CreateMicroPartitionWriter(
    const MicroPartitionWriter::WriterOptions &options, File *file,
    File *toast_file) {
  std::vector<pax::porc::proto::Type_Kind> type_kinds;
  MicroPartitionWriter *writer = nullptr;
  type_kinds = OrcWriter::BuildSchema(
      options.rel_tuple_desc,
      options.storage_format == PaxStorageFormat::kTypeStoragePorcVec);
  writer = PAX_NEW<OrcWriter>(std::move(options), std::move(type_kinds), file,
                              toast_file);
  return writer;
}

}  // namespace pax
