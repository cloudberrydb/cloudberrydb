#include <gtest/gtest.h>

#include "storage/pax.h"

#include <string>
#include <utility>
#include <vector>

#include "access/pax_partition.h"
#include "comm/gtest_wrappers.h"
#include "exceptions/CException.h"
#include "pax_gtest_helper.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition.h"
#include "storage/orc/orc.h"
#include "storage/pax_table_partition_writer.h"
#include "stub.h"

#ifdef ENABLE_PLASMA
#include "storage/cache/pax_plasma_cache.h"
#endif

namespace pax::tests {
using ::testing::_;
using ::testing::AtLeast;
using ::testing::Return;
using ::testing::ReturnRoundRobin;

const char *pax_file_name = "12";

class MockReaderInterator : public IteratorBase<MicroPartitionMetadata> {
 public:
  explicit MockReaderInterator(
      const std::vector<MicroPartitionMetadata> &meta_info_list)
      : index_(0) {
    micro_partitions_.insert(micro_partitions_.end(), meta_info_list.begin(),
                             meta_info_list.end());
  }

  bool HasNext() override { return index_ < micro_partitions_.size(); }

  void Rewind() override { index_ = 0; }

  MicroPartitionMetadata Next() override { return micro_partitions_[index_++]; }

 private:
  uint32 index_;
  std::vector<MicroPartitionMetadata> micro_partitions_;
};

class MockWriter : public TableWriter {
 public:
  MockWriter(const Relation relation, WriteSummaryCallback callback)
      : TableWriter(relation) {
    SetWriteSummaryCallback(callback);
    SetFileSplitStrategy(new PaxDefaultSplitStrategy());
  }

  MOCK_METHOD(std::string, GenFilePath, (const std::string &), (override));
  MOCK_METHOD((std::vector<std::tuple<ColumnEncoding_Kind, int>>),
              GetRelEncodingOptions, (), (override));
};

class PaxWriterTest : public ::testing::Test {
 public:
  void SetUp() override {
    Singleton<LocalFileSystem>::GetInstance()->Delete(pax_file_name);
    CreateMemoryContext();
    CreateTestResourceOwner();
  }

  void TearDown() override {
    Singleton<LocalFileSystem>::GetInstance()->Delete(pax_file_name);
    ReleaseTestResourceOwner();
  }
};

TEST_F(PaxWriterTest, WriteReadTuple) {
  CTupleSlot *slot = CreateTestCTupleSlot(true);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;

  auto relation = (Relation)cbdb::Palloc0(sizeof(RelationData));
  relation->rd_att = slot->GetTupleTableSlot()->tts_tupleDescriptor;
  bool callback_called = false;

  TableWriter::WriteSummaryCallback callback =
      [&callback_called](const WriteSummary & /*summary*/) {
        callback_called = true;
      };

  auto writer = new MockWriter(relation, callback);
  EXPECT_CALL(*writer, GenFilePath(_))
      .Times(AtLeast(1))
      .WillRepeatedly(Return(pax_file_name));

  for (size_t i = 0; i < COLUMN_NUMS; i++) {
    encoding_opts.emplace_back(
        std::make_tuple(ColumnEncoding_Kind_NO_ENCODED, 0));
  }
  EXPECT_CALL(*writer, GetRelEncodingOptions())
      .Times(AtLeast(1))
      .WillRepeatedly(Return(encoding_opts));

  writer->Open();

  writer->WriteTuple(slot);
  writer->Close();
  ASSERT_TRUE(callback_called);

  DeleteTestCTupleSlot(slot);
  delete writer;

  std::vector<MicroPartitionMetadata> meta_info_list;
  MicroPartitionMetadata meta_info;

  meta_info.SetFileName(pax_file_name);
  meta_info.SetMicroPartitionId(pax_file_name);

  meta_info_list.push_back(std::move(meta_info));

  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> meta_info_iterator =
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(
          new MockReaderInterator(meta_info_list));

  TableReader *reader;
  TableReader::ReaderOptions reader_options{};
  reader_options.build_bitmap = false;
  reader_options.rel_oid = 0;
  reader = new TableReader(std::move(meta_info_iterator), reader_options);
  reader->Open();

  CTupleSlot *rslot = CreateTestCTupleSlot(false);

  reader->ReadTuple(rslot);
  EXPECT_TRUE(VerifyTestCTupleSlot(rslot));

  DeleteTestCTupleSlot(rslot);
  delete relation;
  delete reader;
}

TEST_F(PaxWriterTest, WriteReadTupleSplitFile) {
  CTupleSlot *slot = CreateTestCTupleSlot(true);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;
  auto relation = (Relation)cbdb::Palloc0(sizeof(RelationData));

  relation->rd_att = slot->GetTupleTableSlot()->tts_tupleDescriptor;
  bool callback_called = false;

  TableWriter::WriteSummaryCallback callback =
      [&callback_called](const WriteSummary & /*summary*/) {
        callback_called = true;
      };

  auto writer = new MockWriter(relation, callback);
  uint32 call_times = 0;
  EXPECT_CALL(*writer, GenFilePath(_))
      .Times(AtLeast(2))
      .WillRepeatedly(testing::Invoke([&call_times]() -> std::string {
        return std::string(pax_file_name) + std::to_string(call_times++);
      }));
  for (size_t i = 0; i < COLUMN_NUMS; i++) {
    encoding_opts.emplace_back(
        std::make_tuple(ColumnEncoding_Kind_NO_ENCODED, 0));
  }
  EXPECT_CALL(*writer, GetRelEncodingOptions())
      .Times(AtLeast(1))
      .WillRepeatedly(Return(encoding_opts));

  writer->Open();

  ASSERT_TRUE(writer->GetFileSplitStrategy()->SplitTupleNumbers());
  auto split_size = writer->GetFileSplitStrategy()->SplitTupleNumbers();

  for (size_t i = 0; i < split_size + 1; i++) {
    writer->WriteTuple(slot);
  }
  writer->Close();
  ASSERT_TRUE(callback_called);

  DeleteTestCTupleSlot(slot);
  delete writer;

  std::vector<MicroPartitionMetadata> meta_info_list;
  MicroPartitionMetadata meta_info1;
  meta_info1.SetMicroPartitionId(std::string(pax_file_name));
  meta_info1.SetFileName(pax_file_name + std::to_string(0));

  MicroPartitionMetadata meta_info2;
  meta_info2.SetMicroPartitionId(std::string(pax_file_name));
  meta_info2.SetFileName(pax_file_name + std::to_string(1));

  meta_info_list.push_back(std::move(meta_info1));
  meta_info_list.push_back(std::move(meta_info2));

  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> meta_info_iterator =
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(
          new MockReaderInterator(meta_info_list));

  TableReader *reader;
  TableReader::ReaderOptions reader_options{.build_bitmap = false,
                                            .rel_oid = 0};
  reader_options.build_bitmap = false;
  reader = new TableReader(std::move(meta_info_iterator), reader_options);
  reader->Open();

  CTupleSlot *rslot = CreateTestCTupleSlot(false);

  for (size_t i = 0; i < split_size + 1; i++) {
    ASSERT_TRUE(reader->ReadTuple(rslot));
    EXPECT_TRUE(VerifyTestCTupleSlot(rslot));
  }
  ASSERT_FALSE(reader->ReadTuple(rslot));
  reader->Close();

  DeleteTestCTupleSlot(rslot);
  delete reader;
  delete relation;

  std::remove((pax_file_name + std::to_string(0)).c_str());
  std::remove((pax_file_name + std::to_string(1)).c_str());
}

#ifdef ENABLE_PLASMA

TEST_F(PaxWriterTest, TestCacheColumns) {
  CTupleSlot *slot = CreateTestCTupleSlot(true);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;
  const char *uuid_file_name = "40fdcd4e-52cc-11ee-a652-52549e1c7e53";

  std::remove(uuid_file_name);

  auto relation = (Relation)cbdb::Palloc0(sizeof(RelationData));
  relation->rd_att = slot->GetTupleTableSlot()->tts_tupleDescriptor;
  bool callback_called = false;

  TableWriter::WriteSummaryCallback callback =
      [&callback_called](const WriteSummary & /*summary*/) {
        callback_called = true;
      };

  auto writer = new MockWriter(relation, callback);
  EXPECT_CALL(*writer, GenFilePath(_))
      .Times(AtLeast(1))
      .WillRepeatedly(Return(uuid_file_name));

  for (size_t i = 0; i < COLUMN_NUMS; i++) {
    encoding_opts.emplace_back(
        std::make_tuple(ColumnEncoding_Kind_NO_ENCODED, 0));
  }
  EXPECT_CALL(*writer, GetRelEncodingOptions())
      .Times(AtLeast(1))
      .WillRepeatedly(Return(encoding_opts));

  writer->Open();
  writer->WriteTuple(slot);
  writer->Close();
  ASSERT_TRUE(callback_called);

  DeleteTestCTupleSlot(slot);
  delete writer;

  std::vector<MicroPartitionMetadata> meta_info_list;
  MicroPartitionMetadata meta_info;

  meta_info.SetFileName(uuid_file_name);
  meta_info.SetMicroPartitionId(uuid_file_name);

  meta_info_list.push_back(std::move(meta_info));

  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> meta_info_iterator =
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(
          new MockReaderInterator(meta_info_list));

  PaxPlasmaCache::CacheOptions cache_options;
  cache_options.domain_socket = "/tmp/plasma";
  cache_options.client_name = "";
  cache_options.memory_quota = 10 * 1024 * 1024;
  cache_options.waitting_ms = 0;

  auto pax_cache = new PaxPlasmaCache(std::move(cache_options));
  pax_cache->Initialize();

  PaxFilter *filter = new PaxFilter();
  auto proj_map = new bool[2];
  memset(proj_map, true, 2);
  filter->SetColumnProjection(proj_map, 2);

  TableReader *reader;
  TableReader::ReaderOptions reader_options{};
  reader_options.build_bitmap = false;
  reader_options.rel_oid = 0;
  reader_options.filter = filter;
  reader_options.pax_cache = pax_cache;

  reader = new TableReader(std::move(meta_info_iterator), reader_options);
  reader->Open();

  CTupleSlot *rslot = CreateTestCTupleSlot(false);

  reader->ReadTuple(rslot);
  reader->Close();

  DeleteTestCTupleSlot(rslot);
  delete reader;

  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> meta_info_iterator2 =
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(
          new MockReaderInterator(meta_info_list));

  reader = new TableReader(std::move(meta_info_iterator2), reader_options);
  reader->Open();
  rslot = CreateTestCTupleSlot(false);

  reader->ReadTuple(rslot);
  EXPECT_TRUE(VerifyTestCTupleSlot(rslot));

  reader->Close();
  DeleteTestCTupleSlot(rslot);
  delete reader;

  std::remove(uuid_file_name);
  pax_cache->Destroy();

  delete relation;
  delete filter;
  delete pax_cache;
}

#endif  // #ifdef ENABLE_PLASMA

class MockParitionWriter : public TableParitionWriter {
 public:
  MockParitionWriter(const Relation relation, PartitionObject *bucket,
                     WriteSummaryCallback callback)
      : TableParitionWriter(relation, bucket) {
    SetWriteSummaryCallback(callback);
    SetFileSplitStrategy(new PaxDefaultSplitStrategy());
  }

  MOCK_METHOD(std::string, GenFilePath, (const std::string &), (override));
  MOCK_METHOD((std::vector<std::tuple<ColumnEncoding_Kind, int>>),
              GetRelEncodingOptions, (), (override));
};

namespace mock_partition_test {
int NumPartitions() { return 10; }
int FindPartition(TupleTableSlot * /*slot*/) {
  static int round = 0;
  auto index = round % 8;
  static std::vector<int> round_indexs = {0, 2, 3, 4, 5, 6, 8, 9};
  ++round;
  return round_indexs[index];
};

std::vector<std::vector<size_t>> GetPartitionMergeInfos() {
  return {{0, 2, 3}, {4, 5}, {6, 8, 9}};
}

void MicroPartitionStatsMerge(MicroPartitionStats *stats, TupleDesc desc) {}
}  // namespace mock_partition_test

TEST_F(PaxWriterTest, ParitionWriteReadTuple) {
  std::vector<const char *> file_names = {
      "pax_parition_0.file", "pax_parition_2.file", "pax_parition_3.file",
      "pax_parition_4.file", "pax_parition_5.file", "pax_parition_6.file",
      "pax_parition_8.file", "pax_parition_9.file",
  };
  CTupleSlot *slot = CreateTestCTupleSlot(true);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;
  auto relation = (Relation)cbdb::Palloc0(sizeof(RelationData));
  relation->rd_rel = (Form_pg_class)cbdb::Palloc0(sizeof(*relation->rd_rel));
  relation->rd_att = slot->GetTupleTableSlot()->tts_tupleDescriptor;
  bool callback_called = false;
  Stub *stub;
  stub = new Stub();

  auto clear_disk_file = [file_names] {
    for (const auto &file_name : file_names) {
      std::remove(file_name);
    }
  };

  clear_disk_file();

  TableWriter::WriteSummaryCallback callback =
      [&callback_called](const WriteSummary & /*summary*/) {
        callback_called = true;
      };

  for (size_t i = 0; i < COLUMN_NUMS; i++) {
    encoding_opts.emplace_back(
        std::make_tuple(ColumnEncoding_Kind_NO_ENCODED, 0));
  }

  stub->set(ADDR(PartitionObject, NumPartitions),
            mock_partition_test::NumPartitions);
  stub->set(ADDR(PartitionObject, FindPartition),
            mock_partition_test::FindPartition);
  stub->set(ADDR(MockParitionWriter, GetPartitionMergeInfos),
            mock_partition_test::GetPartitionMergeInfos);
  stub->set(ADDR(MicroPartitionStats, MergeTo),
            mock_partition_test::MicroPartitionStatsMerge);

  auto part_obj = new PartitionObject();
  auto writer = new MockParitionWriter(relation, part_obj, callback);

  EXPECT_CALL(*writer, GenFilePath(_))
      .Times(8)  // must be 8
      .WillRepeatedly(ReturnRoundRobin(file_names));

  EXPECT_CALL(*writer, GetRelEncodingOptions())
      .Times(AtLeast(1))
      .WillRepeatedly(Return(encoding_opts));

  writer->Open();

  for (size_t i = 0; i < 400; i++) {
    writer->WriteTuple(slot);
  }

  writer->Close();
  ASSERT_TRUE(callback_called);

  DeleteTestCTupleSlot(slot);
  delete part_obj;
  delete writer;

  // will remain pax_parition_0.file, pax_parition_4.file,
  // pax_parition_6.file after merge
  ASSERT_EQ(0, access(file_names[0], 0));
  ASSERT_NE(0, access(file_names[1], 0));
  ASSERT_NE(0, access(file_names[2], 0));
  ASSERT_EQ(0, access(file_names[3], 0));
  ASSERT_NE(0, access(file_names[4], 0));
  ASSERT_EQ(0, access(file_names[5], 0));
  ASSERT_NE(0, access(file_names[6], 0));
  ASSERT_NE(0, access(file_names[7], 0));

  std::vector<MicroPartitionMetadata> meta_info_list;
  MicroPartitionMetadata meta_info;
  meta_info.SetFileName(file_names[0]);
  meta_info.SetMicroPartitionId(file_names[0]);
  meta_info_list.push_back(meta_info);

  meta_info.SetFileName(file_names[3]);
  meta_info.SetMicroPartitionId(file_names[3]);
  meta_info_list.push_back(meta_info);

  meta_info.SetFileName(file_names[5]);
  meta_info.SetMicroPartitionId(file_names[5]);
  meta_info_list.push_back(meta_info);

  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> meta_info_iterator =
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(
          new MockReaderInterator(meta_info_list));

  TableReader *reader;
  TableReader::ReaderOptions reader_options{};
  reader_options.build_bitmap = false;
  reader_options.rel_oid = 0;
  reader = new TableReader(std::move(meta_info_iterator), reader_options);
  reader->Open();

  CTupleSlot *rslot = CreateTestCTupleSlot(false);

  for (int i = 0; i < 400; i++) {
    ASSERT_TRUE(reader->ReadTuple(rslot));
    EXPECT_TRUE(VerifyTestCTupleSlot(rslot));
  }

  DeleteTestCTupleSlot(rslot);
  delete relation;
  delete reader;
  delete stub;

  clear_disk_file();
}

}  // namespace pax::tests
