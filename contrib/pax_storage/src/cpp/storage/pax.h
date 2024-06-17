#pragma once

#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "comm/bitmap.h"
#include "comm/iterator.h"
#include "storage/file_system.h"
#include "storage/micro_partition.h"
#include "storage/micro_partition_metadata.h"
#include "storage/orc/porc.h"
#include "storage/pax_filter.h"
#include "storage/pax_itemptr.h"
#include "storage/strategy.h"

#ifdef VEC_BUILD
#include "storage/vec/pax_vec_adapter.h"
#endif

namespace pax {

class TableWriter {
 public:
  using WriteSummaryCallback = MicroPartitionWriter::WriteSummaryCallback;

  explicit TableWriter(Relation relation);

  virtual ~TableWriter();

  PaxStorageFormat GetStorageFormat();

  virtual const FileSplitStrategy *GetFileSplitStrategy() const;

  virtual void WriteTuple(TupleTableSlot *slot);

  virtual void Open();

  virtual void Close();

  TableWriter *SetWriteSummaryCallback(WriteSummaryCallback callback);

  TableWriter *SetFileSplitStrategy(const FileSplitStrategy *strategy);

  BlockNumber GetBlockNumber() const { return current_blockno_; }

 protected:
  virtual std::string GenFilePath(const std::string &block_id);

  virtual std::string GenToastFilePath(const std::string &file_path);

  virtual std::vector<std::tuple<ColumnEncoding_Kind, int>>
  GetRelEncodingOptions();

  std::vector<int> GetMinMaxColumnIndexes();

  MicroPartitionWriter *CreateMicroPartitionWriter(
      MicroPartitionStats *mp_stats);

 protected:
  std::string rel_path_;
  const Relation relation_ = nullptr;
  MicroPartitionWriter *writer_ = nullptr;
  const FileSplitStrategy *strategy_ = nullptr;
  MicroPartitionStats *mp_stats_ = nullptr;
  WriteSummaryCallback summary_callback_;
  FileSystem *file_system_ = nullptr;
  FileSystemOptions *file_system_options_ = nullptr;

  size_t num_tuples_ = 0;
  BlockNumber current_blockno_ = 0;
  bool already_get_format_ = false;
  PaxStorageFormat storage_format_ = PaxStorageFormat::kTypeStoragePorcNonVec;
  bool already_get_min_max_col_idx_ = false;
  std::vector<int> min_max_col_idx_;
  bool is_dfs_table_space_;
};

class TableReader final {
 public:
  struct ReaderOptions {
    Oid table_space_id = 0;

    DataBuffer<char> *reused_buffer = nullptr;

    // Will not used in TableReader
    // But pass into micro partition reader
    PaxFilter *filter = nullptr;

#ifdef VEC_BUILD
    bool is_vec = false;
    TupleDesc tuple_desc = nullptr;
    bool vec_build_ctid = false;
#endif
  };

  TableReader(std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator,
              ReaderOptions options);
  virtual ~TableReader();

  void Open();

  void ReOpen();

  void Close();

  bool ReadTuple(TupleTableSlot *slot);

  bool GetTuple(TupleTableSlot *slot, ScanDirection direction,
                const size_t offset);

  // deprecate:
  // DON'T USE, this function will be removed
  const std::string &GetCurrentMicroPartitionId() const {
    return micro_partition_id_;
  }

 private:
  void OpenFile();

 private:
  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> iterator_;
  MicroPartitionReader *reader_ = nullptr;
  bool is_empty_ = false;
  const ReaderOptions reader_options_;
  uint32 current_block_number_ = 0;

  std::string micro_partition_id_;

  // only for analyze scan
  MicroPartitionMetadata current_block_metadata_;
  // only for analyze scan
  size_t current_block_row_index_ = 0;
  FileSystem *file_system_ = nullptr;
  FileSystemOptions *file_system_options_ = nullptr;
  bool is_dfs_table_space_;
};

class TableDeleter final {
 public:
  TableDeleter(Relation rel,
               std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator,
               std::map<std::string, std::shared_ptr<Bitmap8>> delete_bitmap,
               Snapshot snapshot);

  ~TableDeleter();

  void Delete();

  // delete and mark in visibility map
  void DeleteWithVisibilityMap(TransactionId delete_xid);

 private:
  void OpenWriter();
  void OpenReader();

 private:
  Relation rel_;
  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> iterator_;
  std::map<std::string, std::shared_ptr<Bitmap8>> delete_bitmap_;
  Snapshot snapshot_;
  TableReader *reader_;
  TableWriter *writer_;
  FileSystem *file_system_ = nullptr;
  FileSystemOptions *file_system_options_ = nullptr;
};

}  // namespace pax
