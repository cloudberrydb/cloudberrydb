#include "storage/orc/orc.h"  // NOLINT
#include <cstdio>
#include <random>
#include <string>
#include <utility>

#include "access/tupdesc_details.h"
#include "comm/cbdb_wrappers.h"
#include "comm/gtest_wrappers.h"
#include "exceptions/CException.h"
#include "storage/local_file_system.h"
#include "storage/columns/pax_column_traits.h"
#include "pax_gtest_helper.h"
namespace pax::tests {

#ifdef VEC_BUILD

class OrcVecTest : public ::testing::Test {
 public:
  void SetUp() override {
    Singleton<LocalFileSystem>::GetInstance()->Delete(file_name_);

    CreateMemoryContext();
    CreateTestResourceOwner();
  }

  void TearDown() override {
    Singleton<LocalFileSystem>::GetInstance()->Delete(file_name_);
    ReleaseTestResourceOwner();
  }

  static CTupleSlot *CreateFakeCTupleSlot() {
    TupleTableSlot *tuple_slot = nullptr;
    auto tuple_desc = reinterpret_cast<TupleDescData *>(cbdb::Palloc0(
        sizeof(TupleDescData) + sizeof(FormData_pg_attribute) * 2));

    tuple_desc->natts = 2;
    tuple_desc->attrs[0] = {
        .attlen = -1,
        .attbyval = false,
        .attalign = TYPALIGN_DOUBLE,
    };

    tuple_desc->attrs[1] = {
        .attlen = 4,
        .attbyval = true,
        .attalign = TYPALIGN_INT,
    };

    tuple_slot = MakeTupleTableSlot(tuple_desc, &TTSOpsVirtual);
    bool *fake_is_null =
        reinterpret_cast<bool *>(cbdb::Palloc0(sizeof(bool) * 2));
    tuple_slot->tts_isnull = fake_is_null;
    auto ctuple_slot = new CTupleSlot(tuple_slot);
    return ctuple_slot;
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
  const std::string file_name_ = "./test_vec_orc.file";
};

TEST_F(OrcVecTest, WriteReadGroup) {
  CTupleSlot *ctuple_slot = CreateFakeCTupleSlot();

  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  OrcWriter::WriterOptions writer_options;

  writer_options.desc = ctuple_slot->GetTupleDesc();
  writer_options.storage_format = PaxStorageFormat::kTypeStorageOrcVec;

  auto writer = OrcWriter::CreateWriter(writer_options, types, file_ptr);

  for (uint16 i = 0; i < 10000; i++) {
    if (i % 3 == 0) {
      ctuple_slot->GetTupleTableSlot()->tts_isnull[0] = true;
      ctuple_slot->GetTupleTableSlot()->tts_isnull[1] = true;
    } else {
      ctuple_slot->GetTupleTableSlot()->tts_isnull[0] = false;
      ctuple_slot->GetTupleTableSlot()->tts_isnull[1] = false;
    }

    ctuple_slot->GetTupleTableSlot()->tts_values[0] =
        cbdb::DatumFromCString((char *)&i, sizeof(uint16));
    ctuple_slot->GetTupleTableSlot()->tts_values[1] = Int64GetDatum(i);
    writer->WriteTuple(ctuple_slot);
  }

  writer->Flush();

  ctuple_slot->GetTupleTableSlot()->tts_isnull[0] = false;
  ctuple_slot->GetTupleTableSlot()->tts_isnull[1] = false;

  uint16 i = 10000;
  ctuple_slot->GetTupleTableSlot()->tts_values[0] =
      cbdb::DatumFromCString((char *)&i, sizeof(uint16));
  ctuple_slot->GetTupleTableSlot()->tts_values[1] = Int64GetDatum(10000);
  writer->WriteTuple(ctuple_slot);

  writer->Close();

  DeleteCTupleSlot(ctuple_slot);
  delete writer;

  MicroPartitionReader::ReaderOptions reader_options;

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  auto reader = new OrcReader(file_ptr);
  reader->Open(reader_options);

  EXPECT_EQ(2, reader->GetGroupNums());

  // verify group1
  auto group1 = reader->ReadGroup(0);
  auto columns1 = group1->GetAllColumns();

  EXPECT_EQ(2, columns1->GetColumns());

  auto column1 = (PaxVecNonFixedColumn *)(*columns1)[0];
  auto column2 = (PaxVecCommColumn<int32> *)(*columns1)[1];

  auto column1_data = column1->GetDataBuffer();
  auto column2_data = column2->GetDataBuffer();
  auto column1_offsets = column1->GetOffsetBuffer();

  ASSERT_EQ(10000, column1->GetRows());
  ASSERT_EQ(10000, column2->GetRows());

  ASSERT_TRUE(column1->HasNull());
  ASSERT_TRUE(column2->HasNull());

  ASSERT_EQ(6666, column1->GetNonNullRows());
  ASSERT_EQ(6666, column2->GetNonNullRows());

  ASSERT_TRUE(column1_data);
  ASSERT_TRUE(column2_data);
  ASSERT_TRUE(column1_offsets);

  ASSERT_EQ(0, column1_offsets->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_EQ(0, column1_data->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_EQ(0, column2_data->Capacity() % MEMORY_ALIGN_SIZE);

  ASSERT_EQ(10001, column1_offsets->GetSize());
  ASSERT_LE((*column1_offsets)[column1_offsets->GetSize() - 1],
            column1_data->GetSize());

  ASSERT_EQ(10000, column2_data->GetSize());

  for (uint16 i = 0; i < 10000; i++) {
    if (i % 3 == 0) {
      ASSERT_EQ((*column2_data)[i], 0);
    } else {
      ASSERT_EQ((*column2_data)[i], i);
    }
  }

  // verify group 2
  auto group2 = reader->ReadGroup(1);
  auto columns2 = group2->GetAllColumns();

  column1 = (PaxVecNonFixedColumn *)(*columns2)[0];
  column2 = (PaxVecCommColumn<int32> *)(*columns2)[1];

  column1_data = column1->GetDataBuffer();
  column2_data = column2->GetDataBuffer();
  column1_offsets = column1->GetOffsetBuffer();

  ASSERT_FALSE(column1->HasNull());
  ASSERT_FALSE(column2->HasNull());

  ASSERT_EQ(1, column1->GetNonNullRows());
  ASSERT_EQ(1, column2->GetNonNullRows());

  ASSERT_TRUE(column1_data);
  ASSERT_TRUE(column2_data);

  ASSERT_EQ(0, column1_data->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_EQ(0, column2_data->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_TRUE(column1_offsets);

  ASSERT_EQ(0, column1_offsets->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_EQ(0, column1_data->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_EQ(0, column2_data->Capacity() % MEMORY_ALIGN_SIZE);

  ASSERT_EQ(2, column1_offsets->GetSize());
  ASSERT_LE((*column1_offsets)[column1_offsets->GetSize() - 1],
            column1_data->GetSize());
  // still contain memalign size
  ASSERT_EQ(2, column2_data->GetSize());

  ASSERT_EQ((*column2_data)[0], 10000);

  delete group1;
  delete group2;
  delete reader;
}

TEST_F(OrcVecTest, WriteReadGroupWithEncoding) {
  CTupleSlot *ctuple_slot = CreateFakeCTupleSlot();

  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> types_encoding;
  types_encoding.emplace_back(std::make_tuple(
      ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZSTD, 5));
  types_encoding.emplace_back(
      std::make_tuple(ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2, 0));

  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.desc = ctuple_slot->GetTupleDesc();
  writer_options.encoding_opts = types_encoding;
  writer_options.storage_format = PaxStorageFormat::kTypeStorageOrcVec;

  auto writer = new OrcWriter(writer_options, types, file_ptr);

  for (uint16 i = 0; i < 10000; i++) {
    if (i % 3 == 0) {
      ctuple_slot->GetTupleTableSlot()->tts_isnull[0] = true;
      ctuple_slot->GetTupleTableSlot()->tts_isnull[1] = true;
    } else {
      ctuple_slot->GetTupleTableSlot()->tts_isnull[0] = false;
      ctuple_slot->GetTupleTableSlot()->tts_isnull[1] = false;
    }

    ctuple_slot->GetTupleTableSlot()->tts_values[0] =
        cbdb::DatumFromCString((char *)&i, sizeof(uint16));
    ctuple_slot->GetTupleTableSlot()->tts_values[1] = Int64GetDatum(i);
    writer->WriteTuple(ctuple_slot);
  }

  writer->Flush();

  ctuple_slot->GetTupleTableSlot()->tts_isnull[0] = false;
  ctuple_slot->GetTupleTableSlot()->tts_isnull[1] = false;

  uint16 i = 10000;
  ctuple_slot->GetTupleTableSlot()->tts_values[0] =
      cbdb::DatumFromCString((char *)&i, sizeof(uint16));
  ctuple_slot->GetTupleTableSlot()->tts_values[1] = Int64GetDatum(10000);
  writer->WriteTuple(ctuple_slot);

  writer->Close();

  DeleteCTupleSlot(ctuple_slot);
  delete writer;

  MicroPartitionReader::ReaderOptions reader_options;

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  auto reader = new OrcReader(file_ptr);
  reader->Open(reader_options);

  EXPECT_EQ(2, reader->GetGroupNums());

  // verify group1
  auto group1 = reader->ReadGroup(0);
  auto columns1 = group1->GetAllColumns();

  EXPECT_EQ(2, columns1->GetColumns());

  auto column1 = (PaxVecNonFixedColumn *)(*columns1)[0];
  auto column2 = (PaxVecCommColumn<int32> *)(*columns1)[1];

  auto column1_data = column1->GetDataBuffer();
  auto column2_data = column2->GetDataBuffer();
  auto column1_offsets = column1->GetOffsetBuffer();

  ASSERT_EQ(10000, column1->GetRows());
  ASSERT_EQ(10000, column2->GetRows());

  ASSERT_TRUE(column1->HasNull());
  ASSERT_TRUE(column2->HasNull());

  ASSERT_EQ(6666, column1->GetNonNullRows());
  ASSERT_EQ(6666, column2->GetNonNullRows());

  ASSERT_TRUE(column1_data);
  ASSERT_TRUE(column2_data);
  ASSERT_TRUE(column1_offsets);

  ASSERT_EQ(0, column1_offsets->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_EQ(0, column1_data->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_EQ(0, column2_data->Capacity() % MEMORY_ALIGN_SIZE);

  ASSERT_EQ(10001, column1_offsets->GetSize());
  ASSERT_LE((*column1_offsets)[column1_offsets->GetSize() - 1],
            column1_data->GetSize());

  column2->GetBuffer(0);

  for (uint16 i = 0; i < 10000; i++) {
    if (i % 3 == 0) {
      ASSERT_EQ(*(int32 *)column2->GetBuffer(i).first, 0);
    } else {
      ASSERT_EQ(*(int32 *)column2->GetBuffer(i).first, i);
    }
  }

  // verify group 2
  auto group2 = reader->ReadGroup(1);
  auto columns2 = group2->GetAllColumns();

  column1 = (PaxVecNonFixedColumn *)(*columns2)[0];
  column2 = (PaxVecCommColumn<int32> *)(*columns2)[1];

  column1_data = column1->GetDataBuffer();
  column2_data = column2->GetDataBuffer();
  column1_offsets = column1->GetOffsetBuffer();

  ASSERT_FALSE(column1->HasNull());
  ASSERT_FALSE(column2->HasNull());

  ASSERT_EQ(1, column1->GetNonNullRows());
  ASSERT_EQ(1, column2->GetNonNullRows());

  ASSERT_TRUE(column1_data);
  ASSERT_TRUE(column2_data);

  ASSERT_EQ(0, column1_data->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_EQ(0, column2_data->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_TRUE(column1_offsets);

  ASSERT_EQ(0, column1_offsets->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_EQ(0, column1_data->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_EQ(0, column2_data->Capacity() % MEMORY_ALIGN_SIZE);

  ASSERT_EQ(2, column1_offsets->GetSize());
  ASSERT_LE((*column1_offsets)[column1_offsets->GetSize() - 1],
            column1_data->GetSize());

  ASSERT_EQ((*column2_data)[0], 10000);

  delete group1;
  delete group2;
  delete reader;
}

#endif  // VEC_BUILD

}  // namespace pax::tests