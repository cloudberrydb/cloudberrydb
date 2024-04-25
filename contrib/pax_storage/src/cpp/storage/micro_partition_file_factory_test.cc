#include <gtest/gtest.h>

#include "storage/micro_partition_file_factory.h"

#include <cstdio>
#include <random>
#include <string>
#include <utility>
#ifdef VEC_BUILD
extern "C" {
#include "utils/tuptable_vec.h"  // for vec tuple
}
#endif

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

  int flags = 0;
  flags |= pax::ReaderFlags::FLAGS_DEFAULT;

  auto reader = MicroPartitionFileFactory::CreateMicroPartitionReader(
      pax_format, file_ptr, reader_options, flags);
  reader->ReadTuple(tuple_slot_empty);
  EXPECT_TRUE(VerifyTestTupleTableSlot(tuple_slot_empty));

  reader->Close();

  DeleteTestTupleTableSlot(tuple_slot_empty);
  DeleteTestTupleTableSlot(tuple_slot);
  delete writer;
  delete reader;
}

TEST_F(MicroPartitionFileFactoryTest, OrcReadWithVisibilitymap) {
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

  int tuple_count = 1000;
  for (int i = 0; i < tuple_count; i++) {
    tuple_slot->tts_values[2] = i;
    tuple_slot->tts_isnull[2] = false;
    writer->WriteTuple(tuple_slot);
  }
  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  MicroPartitionReader::ReaderOptions reader_options;
  auto visimap = std::make_shared<Bitmap8>(tuple_count);

  for (int i = 0; i < tuple_count; i += 2) {
    visimap->Set(i);
  }

  reader_options.visibility_bitmap = visimap;

  int flags = 0;
  flags |= pax::ReaderFlags::FLAGS_DEFAULT;

  TupleTableSlot *tuple_slot_empty = CreateTestTupleTableSlot();
  auto reader = MicroPartitionFileFactory::CreateMicroPartitionReader(
      pax_format, file_ptr, reader_options, flags);

  int read_tuple_count = 0;
  while (reader->ReadTuple(tuple_slot_empty)) {
    ASSERT_FALSE(tuple_slot_empty->tts_isnull[2]);
    ASSERT_EQ(tuple_slot_empty->tts_values[2], read_tuple_count * 2 + 1);

    read_tuple_count++;
  }

  ASSERT_EQ(read_tuple_count * 2, tuple_count);

  reader->Close();

  DeleteTestTupleTableSlot(tuple_slot_empty);
  DeleteTestTupleTableSlot(tuple_slot);
  PAX_DELETE(writer);
  PAX_DELETE(reader);
}
#ifdef VEC_BUILD
TEST_F(MicroPartitionFileFactoryTest, VecReadWithVisibilitymap) {
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

  int tuple_count = 1000;
  for (int i = 0; i < tuple_count; i++) {
    tuple_slot->tts_isnull[0] = true;
    if (i % 5 == 0) {
      tuple_slot->tts_isnull[1] = true;
    } else {
      char column_buff[100];
      GenTextBuffer(column_buff, 100);
      tuple_slot->tts_values[1] = cbdb::DatumFromCString(column_buff, 100);
      tuple_slot->tts_isnull[1] = false;
    }
    tuple_slot->tts_values[2] = i;
    tuple_slot->tts_isnull[2] = false;
    writer->WriteTuple(tuple_slot);
  }
  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  MicroPartitionReader::ReaderOptions reader_options;
  auto visimap = std::make_shared<Bitmap8>(tuple_count);

  for (int i = 0; i < tuple_count; i += 2) {
    visimap->Set(i);
  }

  reader_options.visibility_bitmap = visimap;
  reader_options.desc = tuple_slot->tts_tupleDescriptor;

  int flags = 0;
  flags |= pax::ReaderFlags::FLAGS_VECTOR;

  TupleTableSlot *read_tuple_slot =
      CreateVecEmptyTupleSlot(tuple_slot->tts_tupleDescriptor);

  auto reader = MicroPartitionFileFactory::CreateMicroPartitionReader(
      pax_format, file_ptr, reader_options, flags);

  auto ret = reader->ReadTuple(read_tuple_slot);
  ASSERT_TRUE(ret);

  {
    VecTupleTableSlot *vslot = nullptr;
    vslot = (VecTupleTableSlot *)read_tuple_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ArrowArray *arrow_array = &rb->batch;
    ASSERT_EQ(arrow_array->length, tuple_count / 2);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 3);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, nullptr);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(child_array->length, tuple_count / 2);
    ASSERT_EQ(child_array->null_count, tuple_count / 2);
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_NE(child_array->buffers[0], nullptr);  // null bitmap
    ASSERT_EQ(child_array->private_data, child_array);

    child_array = arrow_array->children[1];
    ASSERT_EQ(child_array->length, tuple_count / 2);
    ASSERT_EQ(child_array->null_count, tuple_count / 10);
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_NE(child_array->buffers[0], nullptr);  // null bitmap
    ASSERT_EQ(child_array->private_data, child_array);

    child_array = arrow_array->children[2];
    ASSERT_EQ(child_array->length, tuple_count / 2);
    ASSERT_EQ(child_array->null_count, 0);
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, 2);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_EQ(child_array->buffers[0], nullptr);  // null bitmap
    ASSERT_EQ(child_array->private_data, child_array);

    // if (is_fixed) {
    //   ASSERT_NE(child_array->buffers[1], nullptr);
    //
    //  char *buffer = (char *)child_array->buffers[1];
    //  for (size_t i = 0; i < VEC_BATCH_LENGTH; i++) {
    //    ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))), i);
    //  }
    //} else {
    //  ASSERT_NE(child_array->buffers[1], nullptr);
    //  ASSERT_NE(child_array->buffers[2], nullptr);
    //
    //  char *offset_buffer = (char *)child_array->buffers[1];
    //  char *buffer = (char *)child_array->buffers[2];
    //  for (size_t i = 0; i < VEC_BATCH_LENGTH; i++) {
    //    ASSERT_EQ(*((int32 *)(offset_buffer + i * sizeof(int32))),
    //              i * sizeof(int32));
    //    ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))), i);
    //  }
    //
    //  ASSERT_EQ(*((int32 *)(offset_buffer + VEC_BATCH_LENGTH *
    //  sizeof(int32))),
    //            VEC_BATCH_LENGTH * sizeof(int32));
    //}

    ASSERT_EQ(child_array->dictionary, nullptr);
  }
}
#endif

}  // namespace pax::tests
