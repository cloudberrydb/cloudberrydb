#include "comm/cbdb_api.h"

#include "comm/cbdb_wrappers.h"
#include "comm/guc.h"
#include "comm/log.h"
#include "comm/pax_memory.h"
#include "storage/columns/pax_column_traits.h"
#include "storage/micro_partition_stats.h"
#include "storage/orc/orc.h"
#include "storage/orc/orc_defined.h"
#include "storage/orc/orc_group.h"
#include "storage/pax_itemptr.h"

namespace pax {

std::vector<pax::orc::proto::Type_Kind> OrcWriter::BuildSchema(TupleDesc desc) {
  std::vector<pax::orc::proto::Type_Kind> type_kinds;
  for (int i = 0; i < desc->natts; i++) {
    auto attr = &desc->attrs[i];
    if (attr->attbyval) {
      switch (attr->attlen) {
        case 1:
          type_kinds.emplace_back(pax::orc::proto::Type_Kind::Type_Kind_BYTE);
          break;
        case 2:
          type_kinds.emplace_back(pax::orc::proto::Type_Kind::Type_Kind_SHORT);
          break;
        case 4:
          type_kinds.emplace_back(pax::orc::proto::Type_Kind::Type_Kind_INT);
          break;
        case 8:
          type_kinds.emplace_back(pax::orc::proto::Type_Kind::Type_Kind_LONG);
          break;
        default:
          Assert(!"should not be here! pg_type which attbyval=true only have typlen of "
                  "1, 2, 4, or 8");
      }
    } else {
      Assert(attr->attlen > 0 || attr->attlen == -1);
      type_kinds.emplace_back(pax::orc::proto::Type_Kind::Type_Kind_STRING);
    }
  }

  return type_kinds;
}

template <typename N>
static PaxColumn *CreateCommColumn(bool is_vec,
                                   const PaxEncoder::EncodingOption &opts) {
  return is_vec
             ? (PaxColumn *)traits::ColumnOptCreateTraits<
                   PaxVecEncodingColumn, N>::create_encoding(DEFAULT_CAPACITY,
                                                             opts)
             : (PaxColumn *)traits::ColumnOptCreateTraits<
                   PaxEncodingColumn, N>::create_encoding(DEFAULT_CAPACITY,
                                                          opts);
}

static PaxColumns *BuildColumns(
    const std::vector<pax::orc::proto::Type_Kind> &types,
    const std::vector<std::tuple<ColumnEncoding_Kind, int>>
        &column_encoding_types,
    const PaxStorageFormat &storage_format) {
  PaxColumns *columns;
  bool is_vec;

  columns = PAX_NEW<PaxColumns>();
  is_vec = (storage_format == PaxStorageFormat::kTypeStorageOrcVec);
  columns->SetStorageFormat(storage_format);

  for (size_t i = 0; i < types.size(); i++) {
    auto type = types[i];

    PaxEncoder::EncodingOption encoding_option;
    encoding_option.column_encode_type = std::get<0>(column_encoding_types[i]);
    encoding_option.is_sign = true;
    encoding_option.compress_level = std::get<1>(column_encoding_types[i]);

    switch (type) {
      case (pax::orc::proto::Type_Kind::Type_Kind_STRING): {
        encoding_option.is_sign = false;
        columns->Append(is_vec
                            ? (PaxColumn *)traits::ColumnOptCreateTraits2<
                                  PaxVecNonFixedEncodingColumn>::
                                  create_encoding(DEFAULT_CAPACITY,
                                                  std::move(encoding_option))
                            : (PaxColumn *)traits::ColumnOptCreateTraits2<
                                  PaxNonFixedEncodingColumn>::
                                  create_encoding(DEFAULT_CAPACITY,
                                                  std::move(encoding_option)));
        break;
      }
      case (pax::orc::proto::Type_Kind::Type_Kind_BOOLEAN):
      case (pax::orc::proto::Type_Kind::Type_Kind_BYTE):  // len 1 integer
        columns->Append(
            CreateCommColumn<int8>(is_vec, std::move(encoding_option)));
        break;
      case (pax::orc::proto::Type_Kind::Type_Kind_SHORT):  // len 2 integer
        columns->Append(
            CreateCommColumn<int16>(is_vec, std::move(encoding_option)));
        break;
      case (pax::orc::proto::Type_Kind::Type_Kind_INT):  // len 4 integer
        columns->Append(
            CreateCommColumn<int32>(is_vec, std::move(encoding_option)));
        break;
      case (pax::orc::proto::Type_Kind::Type_Kind_LONG):  // len 8 integer
        columns->Append(
            CreateCommColumn<int64>(is_vec, std::move(encoding_option)));
        break;
      default:
        Assert(!"non-implemented column type");
        break;
    }
  }

  return columns;
}

OrcWriter::OrcWriter(const MicroPartitionWriter::WriterOptions &writer_options,
                     const std::vector<pax::orc::proto::Type_Kind> &column_types,
                     File *file)
    : MicroPartitionWriter(writer_options),
      is_closed_(false),
      column_types_(column_types),
      file_(file),
      row_index_(0),
      total_rows_(0),
      current_offset_(0) {
  pax_columns_ = BuildColumns(column_types_, writer_options.encoding_opts,
                              writer_options.storage_format);

  TupleDesc desc = writer_options.desc;
  for (int i = 0; i < desc->natts; i++) {
    auto attr = &desc->attrs[i];
    Assert((size_t)i < pax_columns_->GetColumns());
    auto column = (*pax_columns_)[i];

    Assert(column);
    size_t align_size;
    switch (attr->attalign) {
      case TYPALIGN_SHORT:
        align_size = ALIGNOF_SHORT;
        break;
      case TYPALIGN_INT:
        align_size = ALIGNOF_INT;
        break;
      case TYPALIGN_DOUBLE:
        align_size = ALIGNOF_DOUBLE;
        break;
      case TYPALIGN_CHAR:
        align_size = PAX_DATA_NO_ALIGN;
        break;
      default:
        CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
    }

    column->SetAlignSize(align_size);
  }

  summary_.rel_oid = writer_options.rel_oid;
  summary_.block_id = writer_options.block_id;
  summary_.file_name = writer_options.file_name;

  file_footer_.set_contentlength(0);
  file_footer_.set_numberofrows(0);
  BuildFooterType();

  post_script_.set_footerlength(0);
  post_script_.set_majorversion(PAX_MAJOR_VERSION);
  post_script_.set_minorversion(PAX_MINOR_VERSION);
  post_script_.set_writer(ORC_WRITER_ID);
  post_script_.set_magic(ORC_MAGIC_ID);

  auto natts = static_cast<int>(column_types.size());
  auto stats_data = PAX_NEW<OrcColumnStatsData>();
  stats_collector_.SetStatsMessage(stats_data->Initialize(natts), natts);
  toast_holder_ = PAX_NEW_ARRAY<Datum>(natts);
  memset(toast_holder_, 0, natts * sizeof(Datum));
}

OrcWriter::~OrcWriter() {
  PAX_DELETE(pax_columns_);
  PAX_DELETE(file_);
  PAX_DELETE_ARRAY(toast_holder_);
}

MicroPartitionWriter *OrcWriter::SetStatsCollector(
    MicroPartitionStats *mpstats) {
  if (mpstats) {
    auto stats_data = PAX_NEW<MicroPartittionFileStatsData>(
        &summary_.mp_stats, static_cast<int>(column_types_.size()));
    mpstats->SetStatsMessage(stats_data, column_types_.size());
    return MicroPartitionWriter::SetStatsCollector(mpstats);
  }
  return MicroPartitionWriter::SetStatsCollector(mpstats);
}

void OrcWriter::Flush() {
  BufferedOutputStream buffer_mem_stream(2048);
  if (WriteStripe(&buffer_mem_stream)) {
    Assert(current_offset_ >= buffer_mem_stream.GetDataBuffer()->Used());
    summary_.file_size += buffer_mem_stream.GetDataBuffer()->Used();
    file_->PWriteN(buffer_mem_stream.GetDataBuffer()->GetBuffer(),
                   buffer_mem_stream.GetDataBuffer()->Used(),
                   current_offset_ - buffer_mem_stream.GetDataBuffer()->Used());
    PAX_DELETE(pax_columns_);
    pax_columns_ =
        PAX_NEW<PaxColumns>(column_types_, writer_options_.encoding_opts,
                            writer_options_.storage_format);
  }
}

void OrcWriter::WriteTuple(TupleTableSlot *table_slot) {
  int n;
  TupleDesc table_desc;
  int16 type_len;
  bool type_by_val;
  bool is_null, is_dropped;
  Datum tts_value;
  bool has_detoast = false;
  struct varlena *tts_value_vl = nullptr, *detoast_vl = nullptr;

  summary_.num_tuples++;

  table_desc = table_slot->tts_tupleDescriptor;
  SetTupleOffset(&table_slot->tts_tid, row_index_++);
  n = table_desc->natts;

  CBDB_CHECK(pax_columns_->GetColumns() == static_cast<size_t>(n),
             cbdb::CException::ExType::kExTypeSchemaNotMatch);

  for (int i = 0; i < n; i++) {
    type_len = table_desc->attrs[i].attlen;
    type_by_val = table_desc->attrs[i].attbyval;
    is_dropped = table_desc->attrs[i].attisdropped;
    is_null = table_slot->tts_isnull[i];
    tts_value = table_slot->tts_values[i];

    AssertImply(is_dropped, is_null);

    if (is_null) {
      (*pax_columns_)[i]->AppendNull();
      continue;
    }

    if (type_by_val) {
      switch (type_len) {
        case 1: {
          auto value = cbdb::DatumToInt8(tts_value);
          (*pax_columns_)[i]->Append(reinterpret_cast<char *>(&value),
                                     type_len);
          break;
        }
        case 2: {
          auto value = cbdb::DatumToInt16(tts_value);
          (*pax_columns_)[i]->Append(reinterpret_cast<char *>(&value),
                                     type_len);
          break;
        }
        case 4: {
          auto value = cbdb::DatumToInt32(tts_value);
          (*pax_columns_)[i]->Append(reinterpret_cast<char *>(&value),
                                     type_len);
          break;
        }
        case 8: {
          auto value = cbdb::DatumToInt64(tts_value);
          (*pax_columns_)[i]->Append(reinterpret_cast<char *>(&value),
                                     type_len);
          break;
        }
        default:
          Assert(!"should not be here! pg_type which attbyval=true only have typlen of "
                  "1, 2, 4, or 8 ");
      }
    } else {
      switch (type_len) {
        case -1: {
          tts_value_vl = (struct varlena *)DatumGetPointer(tts_value);
          detoast_vl = cbdb::PgDeToastDatum(tts_value_vl);
          Assert(detoast_vl != nullptr);

          if (tts_value_vl != detoast_vl) {
            has_detoast = true;
            toast_holder_[i] = tts_value;
            table_slot->tts_values[i] = PointerGetDatum(detoast_vl);
          }

          if (COLUMN_STORAGE_FORMAT_IS_VEC(pax_columns_)) {
            (*pax_columns_)[i]->Append(VARDATA_ANY(detoast_vl),
                                       VARSIZE_ANY_EXHDR(detoast_vl));
          } else {
            (*pax_columns_)[i]->Append(reinterpret_cast<char *>(detoast_vl),
                                       VARSIZE_ANY(detoast_vl));
          }
          break;
        }
        default:
          Assert(type_len > 0);
          (*pax_columns_)[i]->Append(
              static_cast<char *>(cbdb::DatumToPointer(tts_value)), type_len);
          break;
      }
    }
  }

  pax_columns_->AddRows(1);
  stats_collector_.AddRow(table_slot);

  if (has_detoast) {
    for (int i = 0; i < n; i++) {
      if (PointerIsValid(toast_holder_[i])) {
        cbdb::Pfree(DatumGetPointer(table_slot->tts_values[i]));
        table_slot->tts_values[i] = toast_holder_[i];
        toast_holder_[i] = 0;
      }
    }
  }

  if (pax_columns_->GetRows() >= writer_options_.group_limit) {
    Flush();
  }
}

void OrcWriter::MergeTo(MicroPartitionWriter *writer) {
  auto orc_writer = dynamic_cast<OrcWriter *>(writer);
  Assert(orc_writer);
  Assert(!is_closed_ && !(orc_writer->is_closed_));
  Assert(this != writer);
  Assert(writer_options_.rel_oid == orc_writer->writer_options_.rel_oid);

  // merge the groups which in disk
  MergeGroups(orc_writer);

  // clear the unstate file in disk.
  orc_writer->DeleteUnstateFile();

  // merge the memory
  MergePaxColumns(orc_writer);

  // Update summary
  summary_.num_tuples += orc_writer->summary_.num_tuples;
  if (mp_stats_) {
    mp_stats_->MergeTo(orc_writer->mp_stats_, writer_options_.desc);
  }
}

void OrcWriter::MergePaxColumns(OrcWriter *writer) {
  PaxColumns *columns = writer->pax_columns_;
  Assert(columns->GetColumns() == pax_columns_->GetColumns());
  Assert(columns->GetRows() < writer_options_.group_limit);
  if (columns->GetRows() == 0) {
    return;
  }

  BufferedOutputStream buffer_mem_stream(2048);
  auto ok = WriteStripe(&buffer_mem_stream, columns,
                        &(writer->stats_collector_), writer->mp_stats_);

  // must be ok
  Assert(ok);

  file_->PWriteN(buffer_mem_stream.GetDataBuffer()->GetBuffer(),
                 buffer_mem_stream.GetDataBuffer()->Used(),
                 current_offset_ - buffer_mem_stream.GetDataBuffer()->Used());

  // Not do memory merge
}

void OrcWriter::MergeGroups(OrcWriter *orc_writer) {
  DataBuffer<char> merge_buffer(0);

  for (int index = 0; index < orc_writer->file_footer_.stripes_size();
       index++) {
    MergeGroup(orc_writer, index, &merge_buffer);
  }
}

void OrcWriter::MergeGroup(OrcWriter *orc_writer, int group_index,
                           DataBuffer<char> *merge_buffer) {
  const auto &stripe_info = orc_writer->file_footer_.stripes(group_index);
  auto total_len = stripe_info.footerlength();
  auto stripe_data_len = stripe_info.datalength();
  auto number_of_rows = stripe_info.numberofrows();

  // will not flush empty group in disk
  Assert(stripe_data_len);

  if (!merge_buffer->GetBuffer()) {
    merge_buffer->Set((char *)cbdb::Palloc(total_len), total_len);
    merge_buffer->SetMemTakeOver(true);
  } else if (merge_buffer->Capacity() < total_len) {
    merge_buffer->Clear();
    merge_buffer->Set((char *)cbdb::Palloc(total_len), total_len);
  }
  orc_writer->file_->Flush();
  orc_writer->file_->PReadN(merge_buffer->GetBuffer(), total_len,
                            stripe_info.offset());

  summary_.file_size += total_len;
  file_->PWriteN(merge_buffer->GetBuffer(), total_len, current_offset_);

  auto stripe_info_write = file_footer_.add_stripes();

  stripe_info_write->set_offset(current_offset_);
  stripe_info_write->set_datalength(stripe_data_len);
  stripe_info_write->set_footerlength(total_len);
  stripe_info_write->set_numberofrows(number_of_rows);

  current_offset_ += total_len;
  total_rows_ += number_of_rows;

  Assert((size_t)stripe_info.colstats_size() == pax_columns_->GetColumns());

  for (int stats_index = 0; stats_index < stripe_info.colstats_size();
       stats_index++) {
    auto col_stats = stripe_info.colstats(stats_index);
    auto col_stats_write = stripe_info_write->add_colstats();
    col_stats_write->CopyFrom(col_stats);
  }
}

void OrcWriter::DeleteUnstateFile() {
  file_->Delete();
  file_->Close();
  is_closed_ = true;
}

bool OrcWriter::WriteStripe(BufferedOutputStream *buffer_mem_stream) {
  return WriteStripe(buffer_mem_stream, pax_columns_, &stats_collector_,
                     mp_stats_);
}

bool OrcWriter::WriteStripe(BufferedOutputStream *buffer_mem_stream,
                            PaxColumns *pax_columns,
                            MicroPartitionStats *stripe_stats,
                            MicroPartitionStats *file_stats) {
  std::vector<pax::orc::proto::Stream> streams;
  std::vector<ColumnEncoding> encoding_kinds;
  pax::orc::proto::StripeFooter stripe_footer;
  pax::orc::proto::StripeInformation *stripe_info;

  size_t data_len = 0;
  size_t number_of_row = pax_columns->GetRows();

  // No need add stripe if nothing in memeory
  if (number_of_row == 0) {
    return false;
  }

  PaxColumns::ColumnStreamsFunc column_streams_func =
      [&streams](const pax::orc::proto::Stream_Kind &kind, size_t column,
                 size_t length) {
        pax::orc::proto::Stream stream;
        stream.set_kind(kind);
        stream.set_column(static_cast<uint32>(column));
        stream.set_length(length);
        streams.push_back(std::move(stream));
      };

  PaxColumns::ColumnEncodingFunc column_encoding_func =
      [&encoding_kinds](const ColumnEncoding_Kind &encoding_kind,
                        int64 origin_len) {
        ColumnEncoding column_encoding;
        Assert(encoding_kind !=
               ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);
        if (encoding_kind != ColumnEncoding_Kind_NO_ENCODED &&
            origin_len == NO_ENCODE_ORIGIN_LEN) {
          CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
        }
        column_encoding.set_kind(encoding_kind);
        column_encoding.set_length(origin_len);

        encoding_kinds.push_back(std::move(column_encoding));
      };

  DataBuffer<char> *data_buffer =
      pax_columns->GetDataBuffer(column_streams_func, column_encoding_func);

  Assert(data_buffer->Used() == data_buffer->Capacity());

  for (const auto &stream : streams) {
    *stripe_footer.add_streams() = stream;
    data_len += stream.length();
  }

  stripe_info = file_footer_.add_stripes();
  auto stats_data =
      dynamic_cast<OrcColumnStatsData *>(stripe_stats->GetStatsData());
  Assert(stats_data);
  for (size_t i = 0; i < pax_columns->GetColumns(); i++) {
    auto pb_stats = stripe_info->add_colstats();
    PaxColumn *pax_column = (*pax_columns)[i];

    *stripe_footer.add_pax_col_encodings() = encoding_kinds[i];

    pb_stats->set_hasnull(pax_column->HasNull());
    pb_stats->set_allnull(pax_column->AllNull());
    *pb_stats->mutable_coldatastats() =
        *stats_data->GetColumnDataStats(static_cast<int>(i));
    PAX_LOG_IF(pax_enable_debug,
               "write group[%lu](allnull=%s, hasnull=%s, nrows=%lu)", i,
               pax_column->AllNull() ? "true" : "false",
               pax_column->HasNull() ? "true" : "false", pax_column->GetRows());
  }
  if (file_stats) {
    file_stats->MergeTo(stripe_stats, writer_options_.desc);
  }

  stats_data->Reset();
  stripe_stats->LightReset();

  buffer_mem_stream->Set(data_buffer);

  // check memory io with protobuf
  CBDB_CHECK(stripe_footer.SerializeToZeroCopyStream(buffer_mem_stream),
             cbdb::CException::ExType::kExTypeIOError);

  stripe_info->set_offset(current_offset_);
  stripe_info->set_datalength(data_len);
  stripe_info->set_footerlength(buffer_mem_stream->GetSize());
  stripe_info->set_numberofrows(number_of_row);

  current_offset_ += buffer_mem_stream->GetSize();
  total_rows_ += number_of_row;

  return true;
}

void OrcWriter::Close() {
  if (is_closed_) {
    return;
  }
  BufferedOutputStream buffer_mem_stream(2048);
  size_t file_offset = current_offset_;
  bool empty_stripe = false;
  DataBuffer<char> *data_buffer;

  empty_stripe = !WriteStripe(&buffer_mem_stream);
  if (empty_stripe) {
    data_buffer = PAX_NEW<DataBuffer<char>>(2048);
    buffer_mem_stream.Set(data_buffer);
  }

  WriteFileFooter(&buffer_mem_stream);
  WritePostscript(&buffer_mem_stream);
  if (summary_callback_) {
    summary_.file_size += buffer_mem_stream.GetDataBuffer()->Used();
    summary_callback_(summary_);
  }

  file_->PWriteN(buffer_mem_stream.GetDataBuffer()->GetBuffer(),
                 buffer_mem_stream.GetDataBuffer()->Used(), file_offset);
  file_->Flush();
  file_->Close();
  if (empty_stripe) {
    PAX_DELETE(data_buffer);
  }
  is_closed_ = true;
}

size_t OrcWriter::PhysicalSize() const { return pax_columns_->PhysicalSize(); }

void OrcWriter::BuildFooterType() {
  auto proto_type = file_footer_.add_types();
  proto_type->set_kind(::pax::orc::proto::Type_Kind_STRUCT);

  for (size_t i = 0; i < column_types_.size(); ++i) {
    auto orc_type = column_types_[i];

    auto sub_proto_type = file_footer_.add_types();
    sub_proto_type->set_kind(orc_type);
    file_footer_.mutable_types(0)->add_subtypes(i);
  }
}

void OrcWriter::WriteFileFooter(BufferedOutputStream *buffer_mem_stream) {
  Assert(writer_options_.storage_format == kTypeStorageOrcNonVec ||
         writer_options_.storage_format == kTypeStorageOrcVec);
  file_footer_.set_contentlength(current_offset_);
  file_footer_.set_numberofrows(total_rows_);
  file_footer_.set_storageformat(writer_options_.storage_format);

  auto stats_data =
      dynamic_cast<OrcColumnStatsData *>(stats_collector_.GetStatsData());
  Assert(file_footer_.colinfo_size() == 0);
  for (size_t i = 0; i < pax_columns_->GetColumns(); i++) {
    auto pb_colinfo = file_footer_.add_colinfo();
    *pb_colinfo = *stats_data->GetColumnBasicInfo(static_cast<int>(i));
  }

  buffer_mem_stream->StartBufferOutRecord();
  CBDB_CHECK(file_footer_.SerializeToZeroCopyStream(buffer_mem_stream),
             cbdb::CException::ExType::kExTypeIOError);

  post_script_.set_footerlength(buffer_mem_stream->EndBufferOutRecord());
}

void OrcWriter::WritePostscript(BufferedOutputStream *buffer_mem_stream) {
  buffer_mem_stream->StartBufferOutRecord();
  CBDB_CHECK(post_script_.SerializeToZeroCopyStream(buffer_mem_stream),
             cbdb::CException::ExType::kExTypeIOError);

  auto ps_len = (uint64)buffer_mem_stream->EndBufferOutRecord();
  Assert(ps_len > 0);
  static_assert(sizeof(ps_len) == ORC_POST_SCRIPT_SIZE,
                "post script type len not match.");
  buffer_mem_stream->DirectWrite((char *)&ps_len, ORC_POST_SCRIPT_SIZE);
}

OrcColumnStatsData *OrcColumnStatsData::Initialize(int natts) {
  Assert(natts >= 0);
  col_data_stats_.resize(natts);
  col_basic_info_.resize(natts);
  has_nulls_.resize(natts);
  all_nulls_.resize(natts);

  Reset();
  return this;
}

void OrcColumnStatsData::CopyFrom(MicroPartitionStatsData * /*stats*/) {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeUnImplements);
}

void OrcColumnStatsData::CheckVectorSize() const {
  Assert(col_data_stats_.size() == col_basic_info_.size());
}

void OrcColumnStatsData::Reset() {
  auto n = col_basic_info_.size();
  for (size_t i = 0; i < n; i++) {
    col_data_stats_[i].Clear();
    has_nulls_[i] = false;
    all_nulls_[i] = true;
  }
}

::pax::stats::ColumnBasicInfo *OrcColumnStatsData::GetColumnBasicInfo(
    int column_index) {
  Assert(column_index >= 0 && column_index < ColumnSize());
  return &col_basic_info_[column_index];
}

::pax::stats::ColumnDataStats *OrcColumnStatsData::GetColumnDataStats(
    int column_index) {
  Assert(column_index >= 0 && column_index < ColumnSize());
  return &col_data_stats_[column_index];
}

int OrcColumnStatsData::ColumnSize() const {
  Assert(col_data_stats_.size() == col_basic_info_.size());
  return static_cast<int>(col_basic_info_.size());
}

void OrcColumnStatsData::SetAllNull(int column_index, bool allnull) {
  all_nulls_[column_index] = allnull;
}

void OrcColumnStatsData::SetHasNull(int column_index, bool hasnull) {
  has_nulls_[column_index] = hasnull;
}

bool OrcColumnStatsData::GetAllNull(int column_index) {
  return all_nulls_[column_index];
}

bool OrcColumnStatsData::GetHasNull(int column_index) {
  return has_nulls_[column_index];
}
}  // namespace pax
