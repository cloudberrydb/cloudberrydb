#include <gtest/gtest.h>

#include "storage/micro_partition_file_factory.h"

#include <cstdio>
#include <random>
#include <string>
#include <utility>

#include "access/tupdesc_details.h"
#include "comm/cbdb_wrappers.h"
#include "comm/gtest_wrappers.h"
#include "comm/singleton.h"
#include "exceptions/CException.h"
#include "storage/local_file_system.h"

namespace pax::tests {
// 3 clomun - string(len 100), string(len 100), int(len 4)
#define COLUMN_NUMS 3
#define COLUMN_SIZE 100
#define INT32_COLUMN_VALUE 0x123
#define INT32_COLUMN_VALUE_DEFAULT 0x001

static void GenFakeBuffer(char *buffer, size_t length) {
  for (size_t i = 0; i < length; i++) {
    buffer[i] = static_cast<char>(i);
  }
}

class MicroPartitionFileFactoryTest : public ::testing::Test {
 public:
  const char *pax_format = MICRO_PARTITION_TYPE_PAX;
  void SetUp() override {
    Singleton<LocalFileSystem>::GetInstance();
    remove(file_name_.c_str());

    MemoryContext micro_partition_test_memory_context = AllocSetContextCreate(
        (MemoryContext)NULL, "OrcTestMemoryContext", 80 * 1024 * 1024,
        80 * 1024 * 1024, 80 * 1024 * 1024);

    MemoryContextSwitchTo(micro_partition_test_memory_context);
    CurrentResourceOwner = ResourceOwnerCreate(NULL, "OrcTestResourceOwner");
  }

  void TearDown() override {
    Singleton<LocalFileSystem>::GetInstance()->Delete(file_name_);
    ResourceOwner tmp_resource_owner = CurrentResourceOwner;
    CurrentResourceOwner = NULL;
    ResourceOwnerRelease(tmp_resource_owner, RESOURCE_RELEASE_BEFORE_LOCKS,
                         false, true);
    ResourceOwnerRelease(tmp_resource_owner, RESOURCE_RELEASE_LOCKS, false,
                         true);
    ResourceOwnerRelease(tmp_resource_owner, RESOURCE_RELEASE_AFTER_LOCKS,
                         false, true);
    ResourceOwnerDelete(tmp_resource_owner);
  }

 protected:
  static CTupleSlot *CreateFakeCTupleSlot(bool with_value = true) {
    TupleTableSlot *tuple_slot = nullptr;

    auto tuple_desc = reinterpret_cast<TupleDescData *>(cbdb::Palloc0(
        sizeof(TupleDescData) + sizeof(FormData_pg_attribute) * COLUMN_NUMS));

    tuple_desc->natts = COLUMN_NUMS;
    tuple_desc->attrs[0] = {
        .atttypid = TEXTOID,
        .attlen = -1,
        .attbyval = false,
        .attalign = TYPALIGN_DOUBLE,
        .attisdropped = false,
    };

    tuple_desc->attrs[1] = {
        .atttypid = TEXTOID,
        .attlen = -1,
        .attbyval = false,
        .attalign = TYPALIGN_DOUBLE,
        .attisdropped = false,
    };

    tuple_desc->attrs[2] = {
        .atttypid = INT4OID,
        .attlen = 4,
        .attbyval = true,
        .attalign = TYPALIGN_INT,
        .attisdropped = false,
    };

    tuple_slot = MakeTupleTableSlot(tuple_desc, &TTSOpsVirtual);

    if (with_value) {
      char column_buff[COLUMN_SIZE * 2];
      GenFakeBuffer(column_buff, COLUMN_SIZE);
      GenFakeBuffer(column_buff + COLUMN_SIZE, COLUMN_SIZE);

      bool *fake_is_null =
          reinterpret_cast<bool *>(cbdb::Palloc0(sizeof(bool) * COLUMN_NUMS));

      fake_is_null[0] = false;
      fake_is_null[1] = false;
      fake_is_null[2] = false;

      tuple_slot->tts_values[0] =
          cbdb::DatumFromCString(column_buff, COLUMN_SIZE);
      tuple_slot->tts_values[1] =
          cbdb::DatumFromCString(column_buff + COLUMN_SIZE, COLUMN_SIZE);
      tuple_slot->tts_values[2] = cbdb::Int32ToDatum(INT32_COLUMN_VALUE);
      tuple_slot->tts_isnull = fake_is_null;
    }

    auto ctuple_slot = new CTupleSlot(tuple_slot);

    return ctuple_slot;
  }

  static CTupleSlot *CreateEmptyCTupleSlot() {
    auto tuple_desc = reinterpret_cast<TupleDescData *>(cbdb::Palloc0(
        sizeof(TupleDescData) + sizeof(FormData_pg_attribute) * COLUMN_NUMS));
    bool *fake_is_null =
        reinterpret_cast<bool *>(cbdb::Palloc0(sizeof(bool) * COLUMN_NUMS));
    auto tuple_slot = reinterpret_cast<TupleTableSlot *>(
        cbdb::Palloc0(sizeof(TupleTableSlot)));
    auto tts_values =
        reinterpret_cast<Datum *>(cbdb::Palloc0(sizeof(Datum) * COLUMN_NUMS));
    tuple_desc->natts = COLUMN_NUMS;
    tuple_desc->attrs[0] = {
        .attlen = -1,
        .attbyval = false,
        .attalign = TYPALIGN_DOUBLE,
        .attisdropped = false,
    };

    tuple_desc->attrs[1] = {
        .attlen = -1,
        .attbyval = false,
        .attalign = TYPALIGN_DOUBLE,
        .attisdropped = false,
    };

    tuple_desc->attrs[2] = {
        .attlen = 4,
        .attbyval = true,
        .attalign = TYPALIGN_INT,
        .attisdropped = false,
    };
    tuple_slot->tts_tupleDescriptor = tuple_desc;
    tuple_slot->tts_values = tts_values;
    tuple_slot->tts_isnull = fake_is_null;
    return new CTupleSlot(tuple_slot);
  }

  static void DeleteCTupleSlot(CTupleSlot *ctuple_slot) {
    auto tuple_table_slot = ctuple_slot->GetTupleTableSlot();
    cbdb::Pfree(tuple_table_slot->tts_tupleDescriptor);
    if (tuple_table_slot->tts_isnull) {
      cbdb::Pfree(tuple_table_slot->tts_isnull);
    }

    cbdb::Pfree(tuple_table_slot);
    delete ctuple_slot;
  }

 protected:
  const std::string file_name_ = "./test.file";
};

TEST_F(MicroPartitionFileFactoryTest, CreateMicroPartitionWriter) {
  CTupleSlot *tuple_slot = CreateFakeCTupleSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<std::tuple<ColumnEncoding_Kind, int>> types_encoding;
  types_encoding.emplace_back(
      std::make_tuple(ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED, 0));
  types_encoding.emplace_back(
      std::make_tuple(ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED, 0));
  types_encoding.emplace_back(
      std::make_tuple(ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED, 0));

  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.desc = tuple_slot->GetTupleDesc();
  writer_options.encoding_opts = types_encoding;

  auto writer = MicroPartitionFileFactory::CreateMicroPartitionWriter(
      pax_format, file_ptr, writer_options);

  writer->WriteTuple(tuple_slot);
  writer->Close();

  DeleteCTupleSlot(tuple_slot);
  delete writer;
}

TEST_F(MicroPartitionFileFactoryTest, CreateMicroPartitionReader) {
  char column_buff[COLUMN_SIZE];

  GenFakeBuffer(column_buff, COLUMN_SIZE);

  CTupleSlot *tuple_slot = CreateFakeCTupleSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<std::tuple<ColumnEncoding_Kind, int>> types_encoding;
  types_encoding.emplace_back(
      std::make_tuple(ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED, 0));
  types_encoding.emplace_back(
      std::make_tuple(ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED, 0));
  types_encoding.emplace_back(
      std::make_tuple(ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED, 0));

  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.desc = tuple_slot->GetTupleDesc();
  writer_options.encoding_opts = types_encoding;

  auto writer = MicroPartitionFileFactory::CreateMicroPartitionWriter(
      pax_format, file_ptr, writer_options);
  CTupleSlot *tuple_slot_empty = CreateEmptyCTupleSlot();

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  MicroPartitionReader::ReaderOptions reader_options;

  auto reader = MicroPartitionFileFactory::CreateMicroPartitionReader(
      pax_format, file_ptr, reader_options);
  tuple_slot_empty->GetTupleDesc()->natts = COLUMN_NUMS;
  reader->ReadTuple(tuple_slot_empty);

  auto vl = (struct varlena *)DatumGetPointer(
      tuple_slot_empty->GetTupleTableSlot()->tts_values[0]);
  auto tunpacked = pg_detoast_datum_packed(vl);
  EXPECT_EQ((Pointer)vl, (Pointer)tunpacked);

  int read_len = VARSIZE(tunpacked);
  char *read_data = VARDATA_ANY(tunpacked);

  EXPECT_EQ(read_len, COLUMN_SIZE + VARHDRSZ);
  EXPECT_EQ(0, memcmp(read_data, column_buff, COLUMN_SIZE));
  reader->Close();

  DeleteCTupleSlot(tuple_slot_empty);
  DeleteCTupleSlot(tuple_slot);
  delete writer;
  delete reader;
}

}  // namespace pax::tests
