#pragma once

#include <string>
#include <utility>
#include <vector>

#include "comm/cbdb_wrappers.h"
#include "comm/pax_memory.h"
#include "exceptions/CException.h"
#include "storage/columns/pax_column.h"
#include "storage/columns/pax_columns.h"
#include "storage/file_system.h"
#include "storage/micro_partition.h"
#include "storage/micro_partition_stats.h"
#include "storage/orc/orc_format_reader.h"
#include "storage/proto/protobuf_stream.h"

namespace pax {
class MicroPartitionStats;
class OrcFormatReader;
class OrcGroup;

class OrcColumnStatsData : public MicroPartitionStatsData {
 public:
  OrcColumnStatsData() = default;
  OrcColumnStatsData *Initialize(int natts);
  void CopyFrom(MicroPartitionStatsData *stats) override;
  ::pax::stats::ColumnBasicInfo *GetColumnBasicInfo(int column_index) override;
  ::pax::stats::ColumnDataStats *GetColumnDataStats(int column_index) override;
  int ColumnSize() const override;
  void SetAllNull(int column_index, bool allnull) override;
  void SetHasNull(int column_index, bool hasnull) override;
  bool GetAllNull(int column_index) override;
  bool GetHasNull(int column_index) override;
  void Reset();

 private:
  void CheckVectorSize() const;

  std::vector<::pax::stats::ColumnDataStats> col_data_stats_;
  std::vector<::pax::stats::ColumnBasicInfo> col_basic_info_;
  std::vector<bool> has_nulls_;
  std::vector<bool> all_nulls_;
};

class OrcWriter : public MicroPartitionWriter {
 public:
  OrcWriter(const MicroPartitionWriter::WriterOptions &orc_writer_options,
            const std::vector<pax::orc::proto::Type_Kind> &column_types,
            File *file);

  ~OrcWriter() override;

  void Flush() override;

  void WriteTuple(TupleTableSlot *slot) override;

  void MergeTo(MicroPartitionWriter *writer) override;

  void Close() override;

  MicroPartitionWriter *SetStatsCollector(
      MicroPartitionStats *mpstats) override;

  size_t PhysicalSize() const override;

  static std::vector<pax::orc::proto::Type_Kind> BuildSchema(
      TupleDesc desc, bool enable_numeric_vec_storage);

#ifndef RUN_GTEST
 protected:  // NOLINT
#endif

  // only for test
  static MicroPartitionWriter *CreateWriter(
      MicroPartitionWriter::WriterOptions options,
      const std::vector<pax::orc::proto::Type_Kind> column_types, File *file) {
    std::vector<std::tuple<ColumnEncoding_Kind, int>> all_no_encoding_types;
    for (auto _ : column_types) {
      (void)_;
      all_no_encoding_types.emplace_back(std::make_tuple(
          ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED, 0));
    }

    options.encoding_opts = all_no_encoding_types;

    return PAX_NEW<OrcWriter>(options, column_types, file);
  }

  void BuildFooterType();
  bool WriteStripe(BufferedOutputStream *buffer_mem_stream,
                   PaxColumns *pax_columns, MicroPartitionStats *stripe_stats,
                   MicroPartitionStats *file_stats);
  bool WriteStripe(BufferedOutputStream *buffer_mem_stream);
  void WriteFileFooter(BufferedOutputStream *buffer_mem_stream);
  void WritePostscript(BufferedOutputStream *buffer_mem_stream);

  void MergePaxColumns(OrcWriter *writer);
  void MergeGroups(OrcWriter *orc_writer);
  void MergeGroup(OrcWriter *orc_writer, int group_index,
                  DataBuffer<char> *merge_buffer);
  void DeleteUnstateFile();

 protected:
  bool is_closed_;
  PaxColumns *pax_columns_;
  Datum *toast_holder_;
  const std::vector<pax::orc::proto::Type_Kind> column_types_;
  File *file_;
  int32 current_written_phy_size_;
  WriteSummary summary_;

  int32 row_index_;
  uint64 total_rows_;
  uint64 current_offset_;

  ::pax::orc::proto::Footer file_footer_;
  ::pax::orc::proto::PostScript post_script_;
  ::pax::MicroPartitionStats stats_collector_;
};

#ifdef ENABLE_PLASMA
class PaxColumnCache;
#endif  // ENABLE_PLASMA

class OrcReader : public MicroPartitionReader {
 public:
  explicit OrcReader(File *file);

  ~OrcReader() override = default;

  void Open(const ReaderOptions &options) override;

  void Close() override;

  bool ReadTuple(TupleTableSlot *cslot) override;

  bool GetTuple(TupleTableSlot *slot, size_t row_index) override;

  size_t GetGroupNums() override;

  MicroPartitionReader::Group *ReadGroup(size_t group_index) override;

  std::unique_ptr<ColumnStatsProvider> GetGroupStatsInfo(
      size_t group_index) override;

  void SetProjColumnIndex(const std::vector<int> &proj_column_index) {
    proj_column_index_ = &proj_column_index;
  }

#ifndef RUN_GTEST
 protected:  // NOLINT
#endif

  // Clean up reading status
  void ResetCurrentReading();

 protected:
  MicroPartitionReader::Group *working_group_;

  // used to cache the group in `GetTuple`
  MicroPartitionReader::Group *cached_group_;
  size_t current_group_index_;

  // nullptr means read all columns
  bool *proj_map_;
  size_t proj_len_;

  // nullptr means read all columns
  // only a reference, owner by pax_filter
  const std::vector<int> *proj_column_index_;

  OrcFormatReader format_reader_;
  bool is_closed_;

#ifdef ENABLE_PLASMA
  PaxColumnCache *pax_column_cache_ = nullptr;
  std::vector<std::string> release_key_;
#endif  // ENABLE_PLASMA
  // orc reader owner visibility_bitmap_
  const Bitmap8 *visibility_bitmap_ = nullptr;
};

};  // namespace pax
