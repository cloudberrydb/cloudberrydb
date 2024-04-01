#include <gtest/gtest.h>

#include "storage/pax.h"

#include <fstream>
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
  }

  MOCK_METHOD(std::string, GenFilePath, (const std::string &), (override));
  MOCK_METHOD((std::vector<std::tuple<ColumnEncoding_Kind, int>>),
              GetRelEncodingOptions, (), (override));
};

class MockSplitStrategy final : public FileSplitStrategy {
  size_t SplitTupleNumbers() const override {
    // 1600 tuple
    return 1600;
  }

  size_t SplitFileSize() const override { return 0; }

  bool ShouldSplit(size_t phy_size, size_t num_tuples) const override {
    return num_tuples >= SplitTupleNumbers();
  }
};

class MockSplitStrategy2 final : public FileSplitStrategy {
  size_t SplitTupleNumbers() const override {
    // 10000 tuple
    return 10000;
  }

  size_t SplitFileSize() const override {
    // 32kb
    return 32 * 1024;
  }

  bool ShouldSplit(size_t phy_size, size_t num_tuples) const override {
    return num_tuples >= SplitTupleNumbers() || phy_size > SplitFileSize();
  }
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
  TupleTableSlot *slot = CreateTestTupleTableSlot(true);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;

  auto relation = (Relation)cbdb::Palloc0(sizeof(RelationData));
  relation->rd_att = slot->tts_tupleDescriptor;
  bool callback_called = false;

  TableWriter::WriteSummaryCallback callback =
      [&callback_called](const WriteSummary & /*summary*/) {
        callback_called = true;
      };

  auto writer = new MockWriter(relation, callback);
  writer->SetFileSplitStrategy(new PaxDefaultSplitStrategy());
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

  DeleteTestTupleTableSlot(slot);
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

  TupleTableSlot *rslot = CreateTestTupleTableSlot(false);

  reader->ReadTuple(rslot);
  EXPECT_TRUE(VerifyTestTupleTableSlot(rslot));

  DeleteTestTupleTableSlot(rslot);
  delete relation;
  delete reader;
}

TEST_F(PaxWriterTest, TestOper) {
  TupleTableSlot *slot = CreateTestTupleTableSlot(true);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;
  Relation relation;
  std::vector<size_t> mins;
  std::vector<size_t> maxs;
  int origin_pax_max_tuples_per_group = pax_max_tuples_per_group;

  std::remove((pax_file_name + std::to_string(0)).c_str());
  std::remove((pax_file_name + std::to_string(1)).c_str());
  std::remove((pax_file_name + std::to_string(2)).c_str());

  relation = (Relation)cbdb::Palloc0(sizeof(RelationData));
  relation->rd_att = slot->tts_tupleDescriptor;

  TableWriter::WriteSummaryCallback callback =
      [&mins, &maxs](const WriteSummary &summary) {
        auto min = *reinterpret_cast<const int32 *>(
            summary.mp_stats.columnstats(2).datastats().minimal().data());
        auto max = *reinterpret_cast<const int32 *>(
            summary.mp_stats.columnstats(2).datastats().maximum().data());

        mins.emplace_back(min);
        maxs.emplace_back(max);
      };

  auto strategy = new MockSplitStrategy();
  auto split_size = strategy->SplitTupleNumbers();

  // 8 groups in a file
  pax_max_tuples_per_group = split_size / 8;

  auto writer = new MockWriter(relation, callback);
  writer->SetFileSplitStrategy(strategy);
  writer->SetStatsCollector(new MicroPartitionStats());
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

  // 3 files
  for (size_t i = 0; i < split_size * 3; i++) {
    slot->tts_values[2] = i;
    writer->WriteTuple(slot);
  }
  writer->Close();

  // verify file min/max
  ASSERT_EQ(mins.size(), 3);
  ASSERT_EQ(maxs.size(), 3);

  for (size_t i = 0; i < 3; i++) {
    ASSERT_EQ(mins[i], split_size * i);
    ASSERT_EQ(maxs[i], split_size * (i + 1) - 1);
  }

  DeleteTestTupleTableSlot(slot);
  delete writer;

  // verify stripe min/max
  auto verify_single_file = [](size_t file_index, size_t file_tuples) {
    LocalFileSystem *local_fs;
    MicroPartitionReader::ReaderOptions reader_options;
    TupleTableSlot *rslot = CreateTestTupleTableSlot(false);
    size_t file_min_max_offset = file_index * file_tuples;

    local_fs = Singleton<LocalFileSystem>::GetInstance();
    auto reader = new OrcReader(local_fs->Open(
        pax_file_name + std::to_string(file_index), fs::kReadMode));
    reader->Open(reader_options);

    ASSERT_EQ(reader->GetGroupNums(), 8);
    for (size_t i = 0; i < 8; i++) {
      auto stats = reader->GetGroupStatsInfo(i);
      auto min = *reinterpret_cast<const int32 *>(
          stats->DataStats(2).minimal().data());
      auto max = *reinterpret_cast<const int32 *>(
          stats->DataStats(2).maximum().data());

      EXPECT_EQ(pax_max_tuples_per_group * i + file_min_max_offset, min);
      EXPECT_EQ(pax_max_tuples_per_group * (i + 1) + file_min_max_offset - 1,
                max);
    }

    delete reader;
    DeleteTestTupleTableSlot(rslot);
  };

  verify_single_file(0, split_size);
  verify_single_file(1, split_size);
  verify_single_file(2, split_size);

  std::remove((pax_file_name + std::to_string(0)).c_str());
  std::remove((pax_file_name + std::to_string(1)).c_str());
  std::remove((pax_file_name + std::to_string(2)).c_str());

  pax_max_tuples_per_group = origin_pax_max_tuples_per_group;
}

TEST_F(PaxWriterTest, WriteReadTupleSplitFile) {
  TupleTableSlot *slot = CreateTestTupleTableSlot(true);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;
  auto relation = (Relation)cbdb::Palloc0(sizeof(RelationData));

  relation->rd_att = slot->tts_tupleDescriptor;
  bool callback_called = false;

  TableWriter::WriteSummaryCallback callback =
      [&callback_called](const WriteSummary & /*summary*/) {
        callback_called = true;
      };

  auto writer = new MockWriter(relation, callback);
  writer->SetFileSplitStrategy(new MockSplitStrategy());
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

  DeleteTestTupleTableSlot(slot);
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

  TupleTableSlot *rslot = CreateTestTupleTableSlot(false);

  for (size_t i = 0; i < split_size + 1; i++) {
    ASSERT_TRUE(reader->ReadTuple(rslot));
    EXPECT_TRUE(VerifyTestTupleTableSlot(rslot));
  }
  ASSERT_FALSE(reader->ReadTuple(rslot));
  reader->Close();

  DeleteTestTupleTableSlot(rslot);
  delete reader;
  delete relation;

  std::remove((pax_file_name + std::to_string(0)).c_str());
  std::remove((pax_file_name + std::to_string(1)).c_str());
}

TEST_F(PaxWriterTest, WriteReadTupleSplitFile2) {
  TupleTableSlot *slot = CreateTestTupleTableSlot(true);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;
  auto relation = (Relation)cbdb::Palloc0(sizeof(RelationData));
  int origin_pax_max_tuples_per_group = pax_max_tuples_per_group;
  pax_max_tuples_per_group = 100;

  relation->rd_att = slot->tts_tupleDescriptor;
  bool callback_called = false;

  TableWriter::WriteSummaryCallback callback =
      [&callback_called](const WriteSummary & /*summary*/) {
        callback_called = true;
      };

  auto writer = new MockWriter(relation, callback);

  writer->SetFileSplitStrategy(new MockSplitStrategy2());
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

  // The length of each tuple is 212 bytes
  // The file size limit is 32kb
  // Should write 2 files here
  for (size_t i = 0; i < 200; i++) {
    writer->WriteTuple(slot);
  }

  writer->Close();
  ASSERT_TRUE(callback_called);

  DeleteTestTupleTableSlot(slot);
  delete writer;

  auto file_size = [](std::string file_name) {
    std::ifstream in(file_name, std::ifstream::ate | std::ifstream::binary);
    return in.tellg();
  };
  auto file1_size = file_size(std::string(pax_file_name) + std::to_string(0));
  EXPECT_GT(file1_size, 0);
  EXPECT_TRUE(file1_size < (32 * 1024 * 1.15) &&
              file1_size > (32 * 1024 * 0.85));

  std::remove((std::string(pax_file_name) + std::to_string(0)).c_str());
  std::remove((std::string(pax_file_name) + std::to_string(1)).c_str());
  // set back to pax_max_tuples_per_group
  pax_max_tuples_per_group = origin_pax_max_tuples_per_group;
}

#ifdef ENABLE_PLASMA

TEST_F(PaxWriterTest, TestCacheColumns) {
  TupleTableSlot *slot = CreateTestTupleTableSlot(true);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;
  const char *uuid_file_name = "40fdcd4e-52cc-11ee-a652-52549e1c7e53";

  std::remove(uuid_file_name);

  auto relation = (Relation)cbdb::Palloc0(sizeof(RelationData));
  relation->rd_att = slot->tts_tupleDescriptor;
  bool callback_called = false;

  TableWriter::WriteSummaryCallback callback =
      [&callback_called](const WriteSummary & /*summary*/) {
        callback_called = true;
      };

  auto writer = new MockWriter(relation, callback);
  writer->SetFileSplitStrategy(new PaxDefaultSplitStrategy());
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

  DeleteTestTupleTableSlot(slot);
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

  TupleTableSlot *rslot = CreateTestTupleTableSlot(false);

  reader->ReadTuple(rslot);
  reader->Close();

  DeleteTestTupleTableSlot(rslot);
  delete reader;

  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> meta_info_iterator2 =
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(
          new MockReaderInterator(meta_info_list));

  reader = new TableReader(std::move(meta_info_iterator2), reader_options);
  reader->Open();
  rslot = CreateTestTupleTableSlot(false);

  reader->ReadTuple(rslot);
  EXPECT_TRUE(VerifyTestTupleTableSlot(rslot));

  reader->Close();
  DeleteTestTupleTableSlot(rslot);
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
      "80000", "80002", "80003", "80004", "80005", "80006", "80008", "80009",
  };
  TupleTableSlot *slot = CreateTestTupleTableSlot(true);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;
  auto relation = (Relation)cbdb::Palloc0(sizeof(RelationData));
  relation->rd_rel = (Form_pg_class)cbdb::Palloc0(sizeof(*relation->rd_rel));
  relation->rd_att = slot->tts_tupleDescriptor;
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

  DeleteTestTupleTableSlot(slot);
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

  TupleTableSlot *rslot = CreateTestTupleTableSlot(false);

  for (int i = 0; i < 400; i++) {
    ASSERT_TRUE(reader->ReadTuple(rslot));
    EXPECT_TRUE(VerifyTestTupleTableSlot(rslot));
  }

  DeleteTestTupleTableSlot(rslot);
  delete relation;
  delete reader;
  delete stub;

  clear_disk_file();
}

}  // namespace pax::tests
