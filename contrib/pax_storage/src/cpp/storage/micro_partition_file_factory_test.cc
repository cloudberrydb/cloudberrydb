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
#include "pax_gtest_helper.h"
#include "storage/local_file_system.h"

namespace pax::tests {

class MicroPartitionFileFactoryTest : public ::testing::Test {
 public:
  const char *pax_format = MICRO_PARTITION_TYPE_PAX;
  void SetUp() override {
    Singleton<LocalFileSystem>::GetInstance()->Delete(file_name_);

    CreateMemoryContext();
    CreateTestResourceOwner();
  }

  void TearDown() override {
    Singleton<LocalFileSystem>::GetInstance()->Delete(file_name_);
    ReleaseTestResourceOwner();
  }

 protected:
  const std::string file_name_ = "./test.file";
};

TEST_F(MicroPartitionFileFactoryTest, CreateMicroPartitionWriter) {
  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot();
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
  writer_options.desc = tuple_slot->tts_tupleDescriptor;
  writer_options.encoding_opts = types_encoding;

  auto writer = MicroPartitionFileFactory::CreateMicroPartitionWriter(
      pax_format, file_ptr, writer_options);

  writer->WriteTuple(tuple_slot);
  writer->Close();

  DeleteTestTupleTableSlot(tuple_slot);
  delete writer;
}

TEST_F(MicroPartitionFileFactoryTest, CreateMicroPartitionReader) {
  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot();
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
  writer_options.desc = tuple_slot->tts_tupleDescriptor;
  writer_options.encoding_opts = types_encoding;

  auto writer = MicroPartitionFileFactory::CreateMicroPartitionWriter(
      pax_format, file_ptr, writer_options);
  TupleTableSlot *tuple_slot_empty = CreateTestTupleTableSlot(false);

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  MicroPartitionReader::ReaderOptions reader_options;

  auto reader = MicroPartitionFileFactory::CreateMicroPartitionReader(
      pax_format, file_ptr, reader_options);
  reader->ReadTuple(tuple_slot_empty);
  EXPECT_TRUE(VerifyTestTupleTableSlot(tuple_slot_empty));

  reader->Close();

  DeleteTestTupleTableSlot(tuple_slot_empty);
  DeleteTestTupleTableSlot(tuple_slot);
  delete writer;
  delete reader;
}

}  // namespace pax::tests
