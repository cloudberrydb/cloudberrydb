#include "storage/pax.h"

#include <map>
#include <utility>

#include "access/paxc_rel_options.h"
#include "catalog/pax_aux_table.h"
#include "comm/cbdb_wrappers.h"
#include "storage/columns/pax_encoding.h"
#include "storage/micro_partition_file_factory.h"
#include "storage/micro_partition_metadata.h"
#include "storage/micro_partition_row_filter_reader.h"
#include "storage/micro_partition_stats.h"

#ifdef VEC_BUILD
#include "storage/vec/pax_vec_reader.h"
#endif

namespace paxc {
class IndexUpdaterInternal {
 public:
  void Begin(Relation rel) {
    Assert(rel);

    rel_ = rel;
    slot_ = MakeTupleTableSlot(rel->rd_att, &TTSOpsVirtual);

    if (HasIndex()) {
      estate_ = CreateExecutorState();

      relinfo_ = makeNode(ResultRelInfo);
      relinfo_->ri_RelationDesc = rel;
      ExecOpenIndices(relinfo_, false);
    }
  }

  void UpdateIndex(TupleTableSlot *slot) {
    Assert(slot == slot_);
    Assert(HasIndex());
    auto recheck_index =
        ExecInsertIndexTuples(relinfo_, slot_, estate_, true, false, NULL, NIL);
    list_free(recheck_index);
  }

  void End() {
    if (HasIndex()) {
      Assert(relinfo_ && estate_);

      ExecCloseIndices(relinfo_);
      pfree(relinfo_);
      relinfo_ = nullptr;

      FreeExecutorState(estate_);
      estate_ = nullptr;
    }
    Assert(relinfo_ == nullptr && estate_ == nullptr);

    ExecDropSingleTupleTableSlot(slot_);
    slot_ = nullptr;

    rel_ = nullptr;
  }

  inline TupleTableSlot *GetSlot() { return slot_; }
  inline bool HasIndex() const { return rel_->rd_rel->relhasindex; }

 private:
  Relation rel_ = nullptr;
  TupleTableSlot *slot_ = nullptr;
  EState *estate_ = nullptr;
  ResultRelInfo *relinfo_ = nullptr;
};
}  // namespace paxc

namespace pax {
class IndexUpdater final {
 public:
  void Begin(Relation rel) {
    CBDB_WRAP_START;
    { stub_.Begin(rel); }
    CBDB_WRAP_END;
  }
  void UpdateIndex(TupleTableSlot *slot) {
    CBDB_WRAP_START;
    { stub_.UpdateIndex(slot); }
    CBDB_WRAP_END;
  }
  void End() {
    CBDB_WRAP_START;
    { stub_.End(); }
    CBDB_WRAP_END;
  }
  inline TupleTableSlot *GetSlot() { return stub_.GetSlot(); }
  inline bool HasIndex() const { return stub_.HasIndex(); }

 private:
  paxc::IndexUpdaterInternal stub_;
};
}  // namespace pax

namespace pax {

TableWriter::TableWriter(Relation relation)
    : relation_(relation), summary_callback_(nullptr) {
  Assert(relation);
}

PaxStorageFormat TableWriter::GetStorageFormat() {
  if (!already_get_format_) {
    Assert(relation_);
    storage_format_ = StorageFormatKeyToPaxStorageFormat(RelationGetOptions(
        relation_, storage_format, STORAGE_FORMAT_TYPE_DEFAULT));
    already_get_format_ = true;
  }

  return storage_format_;
}

TableWriter *TableWriter::SetWriteSummaryCallback(
    WriteSummaryCallback callback) {
  Assert(!summary_callback_);
  summary_callback_ = callback;
  return this;
}

TableWriter *TableWriter::SetFileSplitStrategy(
    const FileSplitStrategy *strategy) {
  Assert(!strategy_);
  strategy_ = strategy;
  return this;
}

TableWriter *TableWriter::SetStatsCollector(MicroPartitionStats *mp_stats) {
  Assert(!mp_stats_);
  Assert(!writer_);  // must be set before the writer is created.

  mp_stats_ = mp_stats;
  return this;
}

TableWriter::~TableWriter() {
  // must call close before delete table writer
  Assert(writer_ == nullptr);

  delete strategy_;
  delete mp_stats_;
}

const FileSplitStrategy *TableWriter::GetFileSplitStrategy() const {
  return strategy_;
}

std::string TableWriter::GenFilePath(const std::string &block_id) {
  return cbdb::BuildPaxFilePath(relation_, block_id);
}

std::vector<std::tuple<ColumnEncoding_Kind, int>>
TableWriter::GetRelEncodingOptions() {
  size_t nattrs = 0;
  paxc::PaxOptions **pax_options = nullptr;
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;

  CBDB_WRAP_START;
  { pax_options = paxc::paxc_relation_get_attribute_options(relation_); }
  CBDB_WRAP_END;
  Assert(pax_options);

  nattrs = relation_->rd_att->natts;

  for (size_t index = 0; index < nattrs; index++) {
    if (pax_options[index]) {
      encoding_opts.emplace_back(std::make_tuple(
          CompressKeyToColumnEncodingKind(pax_options[index]->compress_type),
          pax_options[index]->compress_level));
    } else {
      // TODO(jiaqizho): In pax, we will fill a `DEF_ENCODED` if user not set
      // the encoding clause. Need a GUC to decide whether we should use
      // `NO_ENCODE` or keep use `DEF_ENCODED` also may allow user define
      // different default encoding type for the different pg_type?
      encoding_opts.emplace_back(
          std::make_tuple(ColumnEncoding_Kind_DEF_ENCODED, 0));
    }
  }
  cbdb::Pfree(pax_options);
  return encoding_opts;
}

MicroPartitionWriter *TableWriter::CreateMicroPartitionWriter(
    MicroPartitionStats *mp_stats) {
  MicroPartitionWriter::WriterOptions options;
  std::string file_path;
  std::string block_id;

  Assert(relation_);
  Assert(strategy_);
  Assert(summary_callback_);

  block_id = GenerateBlockID(relation_);
  file_path = GenFilePath(block_id);
#ifdef ENABLE_LOCAL_INDEX
  current_blockno_ = std::stol(block_id);
#endif

  options.rel_oid = relation_->rd_id;
  options.desc = relation_->rd_att;
  options.block_id = std::move(block_id);
  options.file_name = std::move(file_path);
  options.encoding_opts = std::move(GetRelEncodingOptions());
  options.storage_format = GetStorageFormat();

  File *file = Singleton<LocalFileSystem>::GetInstance()->Open(
      options.file_name, fs::kReadWriteMode);

  auto mp_writer = MicroPartitionFileFactory::CreateMicroPartitionWriter(
      MICRO_PARTITION_TYPE_PAX, file, std::move(options));

  mp_writer->SetWriteSummaryCallback(summary_callback_);
  mp_writer->SetStatsCollector(mp_stats);
  return mp_writer;
}

void TableWriter::Open() {
  writer_ = CreateMicroPartitionWriter(mp_stats_);
  num_tuples_ = 0;
#ifdef ENABLE_LOCAL_INDEX
  // insert tuple into the aux table before inserting any tuples.
  cbdb::InsertMicroPartitionPlaceHolder(RelationGetRelid(relation_),
                                        std::to_string(current_blockno_));
#endif
}

void TableWriter::WriteTuple(CTupleSlot *slot) {
  Assert(writer_);
  Assert(strategy_);
  // should check split strategy before write tuple
  // otherwise, may got a empty file in the disk
  if (strategy_->ShouldSplit(writer_->PhysicalSize(), num_tuples_)) {
    writer_->Close();
    delete writer_;
    Open();
  }
  if (mp_stats_) mp_stats_->AddRow(slot->GetTupleTableSlot());

  writer_->WriteTuple(slot);
#ifdef ENABLE_LOCAL_INDEX
  slot->SetBlockNumber(current_blockno_);
  slot->StoreVirtualTuple();
#endif
  ++num_tuples_;
}

void TableWriter::Close() {
  writer_->Close();
  delete writer_;
  writer_ = nullptr;
  num_tuples_ = 0;
}

TableReader::TableReader(
    std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator,
    ReaderOptions options)
    : iterator_(std::move(iterator)),
      reader_(nullptr),
      is_empty_(true),
      reader_options_(options) {}

TableReader::~TableReader() {
  if (reader_) {
    reader_->Close();
    delete reader_;
    reader_ = nullptr;
  }
}

void TableReader::Open() {
  if (!iterator_->HasNext()) {
    is_empty_ = true;
    return;
  }
#ifndef ENABLE_LOCAL_INDEX
  if (reader_options_.build_bitmap) {
    // first open, now alloc a table no in pax shmem for scan
    block_number_manager_.InitOpen(reader_options_.rel_oid);
  }
#endif
  OpenFile();
  is_empty_ = false;
}

void TableReader::ReOpen() {
  Close();
  iterator_->Rewind();
  Open();
}

void TableReader::Close() {
  if (is_empty_) {
    return;
  }

  if (reader_) {
    reader_->Close();
    reader_ = nullptr;
  }
}

bool TableReader::ReadTuple(CTupleSlot *slot) {
  if (is_empty_) {
    return false;
  }

  ExecClearTuple(slot->GetTupleTableSlot());
#ifndef ENABLE_LOCAL_INDEX
  slot->SetTableNo(block_number_manager_.GetTableNo());
#endif
  while (!reader_->ReadTuple(slot)) {
    reader_->Close();
    if (!iterator_->HasNext()) {
      is_empty_ = true;
      return false;
    }
    OpenFile();
  }
  slot->SetBlockNumber(current_block_number_);
  slot->StoreVirtualTuple();
  return true;
}

void TableReader::OpenFile() {
  Assert(iterator_->HasNext());
  auto it = iterator_->Next();
  MicroPartitionReader::ReaderOptions options;
  micro_partition_id_ = options.block_id = it.GetMicroPartitionId();
#ifdef ENABLE_LOCAL_INDEX
  current_block_number_ = std::stol(options.block_id);
#else
  if (reader_options_.build_bitmap) {
    int block_number = 0;
    block_number = block_number_manager_.GetBlockNumber(reader_options_.rel_oid,
                                                        options.block_id);

    Assert(block_number >= 0);
    current_block_number_ = block_number;
  }
#endif
  options.file_name = it.GetFileName();
  options.filter = reader_options_.filter;
  options.reused_buffer = reader_options_.reused_buffer;
#ifdef ENABLE_PLASMA
  options.pax_cache = reader_options_.pax_cache;
#endif

  if (reader_) {
    delete reader_;
  }

  reader_ = new OrcReader(Singleton<LocalFileSystem>::GetInstance()->Open(
      options.file_name, fs::kReadMode));

#ifdef VEC_BUILD
  if (reader_options_.is_vec) {
    Assert(reader_options_.adapter);
    reader_ = new PaxVecReader(reader_, reader_options_.adapter,
                               reader_options_.filter);
  } else
#endif  // VEC_BUILD
    if (reader_options_.filter && reader_options_.filter->HasRowScanFilter()) {
      reader_ =
          MicroPartitionRowFilterReader::New(reader_, reader_options_.filter);
    }

  reader_->Open(options);
}

TableDeleter::TableDeleter(
    Relation rel,
    std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator,
    std::map<std::string, std::unique_ptr<Bitmap64>> &&delete_bitmap,
    Snapshot snapshot)
    : rel_(rel),
      iterator_(std::move(iterator)),
      delete_bitmap_(std::move(delete_bitmap)),
      snapshot_(snapshot),
      reader_(nullptr),
      writer_(nullptr) {}

TableDeleter::~TableDeleter() {
  if (reader_) {
    reader_->Close();
    delete reader_;
    reader_ = nullptr;
  }

  if (writer_) {
    writer_->Close();
    delete writer_;
    writer_ = nullptr;
  }
}

void TableDeleter::Delete() {
  if (!iterator_->HasNext()) {
    return;
  }
  OpenReader();
  OpenWriter();
  pax::IndexUpdater index_updater;

  index_updater.Begin(rel_);
  Assert(rel_->rd_rel->relhasindex == index_updater.HasIndex());
  auto slot = index_updater.GetSlot();

  CTupleSlot cslot(slot);
  slot->tts_tableOid = RelationGetRelid(rel_);
  // TODO(gongxun): because bulk insert as AO/HEAP does with tuples iteration
  // not implemented. we should implement bulk insert firstly. and then we can
  // use ReadTupleN and WriteTupleN to delete tuples in batch.
  while (reader_->ReadTuple(&cslot)) {
    auto block_id = reader_->GetCurrentMicroPartitionId();
    auto it = delete_bitmap_.find(block_id);
    Assert(it != delete_bitmap_.end());

    auto bitmap = it->second.get();
    if (bitmap->Test(pax::GetTupleOffset(cslot.GetCtid()))) continue;

    writer_->WriteTuple(&cslot);
#ifdef ENABLE_LOCAL_INDEX
    if (index_updater.HasIndex()) {
      // TableWriter has stored ctid in cslot, we need to store it in
      // slot.tts_tid and set the valid number of columns.
      cslot.StoreVirtualTuple();
      index_updater.UpdateIndex(slot);
    }
#endif
  }
  index_updater.End();
}

void TableDeleter::OpenWriter() {
  writer_ = new TableWriter(rel_);
  writer_->SetWriteSummaryCallback(&cbdb::InsertOrUpdateMicroPartitionEntry)
      ->SetFileSplitStrategy(new PaxDefaultSplitStrategy())
      ->SetStatsCollector(new MicroPartitionStats())
      ->Open();
}

void TableDeleter::OpenReader() {
  TableReader::ReaderOptions reader_options{};
  reader_options.build_bitmap = false;
  reader_options.rel_oid = rel_->rd_id;
  reader_ = new TableReader(std::move(iterator_), reader_options);
  reader_->Open();
}

}  // namespace pax