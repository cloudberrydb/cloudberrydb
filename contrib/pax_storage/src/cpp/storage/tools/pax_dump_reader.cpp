#include "storage/tools/pax_dump_reader.h"

#include <tabulate/table.hpp>

#include "comm/singleton.h"
#include "exceptions/CException.h"
#include "storage/columns/pax_column_traits.h"
#include "storage/file_system.h"
#include "storage/local_file_system.h"
#include "storage/orc/orc_defined.h"
#include "storage/orc/orc_format_reader.h"
#include "storage/orc/orc_group.h"

namespace pax::tools {

static inline std::string GetWriterDesc(uint32 writer_id) {
  switch (writer_id) {
    case 1:
      return "CPP writer";
    default:
      return "Unknown";
  }
}

static inline std::string GetStorageFormat(uint32 storage_format) {
  switch (storage_format) {
    case PaxStorageFormat::kTypeStorageOrcNonVec:
      return "Origin format";
    case PaxStorageFormat::kTypeStorageOrcVec:
      return "VEC format";
    default:
      return "Unknown";
  }
}

static inline std::string BoolCast(const bool b) {
  std::ostringstream ss;
  ss << std::boolalpha << b;
  return ss.str();
}

static inline std::tuple<int64, int64, bool> ParseRange(
    const int64 start, const int64 len, const int64 total_size) {
  int64 range_start = 0;
  int64 range_end = total_size;

  if (start != NO_SPEC_ID) {
    if (len == 0 || start + len > total_size) {
      return {range_start, range_end, false};
    }

    range_start = start;
    range_end = start + len;
  }

  return {range_start, range_end, true};
}

PaxDumpReader::PaxDumpReader(DumpConfig *config)
    : config_(config), format_reader_(nullptr) {}

bool PaxDumpReader::Initialize() {
  assert(config_);
  assert(config_->file_name);

  try {
    FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();
    auto open_file = fs->Open(config_->file_name, fs::kReadMode);
    if (open_file->FileLength() == 0) {
      return false;
    }

    format_reader_ = new OrcFormatReader(open_file);
    format_reader_->Open();
  } catch (cbdb::CException &e) {
    std::cout << "error happend\n" << e.Stack() << std::endl;
    Release();
    return false;
  }

  return true;
}

void PaxDumpReader::Release() {
  if (format_reader_) {
    format_reader_->Close();
    delete format_reader_;
    format_reader_ = nullptr;
  }
}

void PaxDumpReader::Dump() {
  bool no_print = true;
  if (config_->print_all) {
    DumpAllInfo();
    no_print = false;
  }

  if (config_->print_all_desc) {
    DumpAllDesc();
    no_print = false;
  }

  if (config_->print_post_script) {
    DumpPostScript();
    no_print = false;
  }

  if (config_->print_footer) {
    DumpFooter();
    no_print = false;
  }

  if (config_->print_schema) {
    DumpSchema();
    no_print = false;
  }

  if (config_->print_group_info) {
    DumpGroupInfo();
    no_print = false;
  }

  if (config_->print_group_footer) {
    DumpGroupFooter();
    no_print = false;
  }

  if (config_->print_all_data) {
    DumpAllData();
    no_print = false;
  }

  // defualt dump all of desc
  if (no_print) {
    DumpAllDesc();
  }
}

void PaxDumpReader::DumpAllInfo() {
  DumpAllDesc();
  DumpAllData();
}

void PaxDumpReader::DumpAllDesc() {
  DumpPostScript();
  DumpFooter();
  DumpSchema();
  DumpGroupInfo();
  DumpGroupFooter();
}

void PaxDumpReader::DumpPostScript() {
  tabulate::Table post_srcipt_table;
  tabulate::Table desc_table;
  auto postsrcipt = &(format_reader_->post_script_);

  desc_table.add_row(
      {"Major version", std::to_string(postsrcipt->majorversion())});
  desc_table.add_row(
      {"Minor version", std::to_string(postsrcipt->minorversion())});
  desc_table.add_row({"Writer desc", GetWriterDesc(postsrcipt->writer())});
  desc_table.add_row(
      {"Footer length", std::to_string(postsrcipt->footerlength())});
  desc_table.add_row({"Magic", postsrcipt->magic()});

  post_srcipt_table.add_row(tabulate::Table::Row_t{"Post Script description"});
  post_srcipt_table[0].format().font_align(tabulate::FontAlign::center);
  post_srcipt_table[0].format().font_color(tabulate::Color::red);

  post_srcipt_table.add_row(tabulate::Table::Row_t{desc_table});
  post_srcipt_table[1].format().hide_border_top();

  std::cout << post_srcipt_table << std::endl;
}

void PaxDumpReader::DumpSchema() {
  tabulate::Table schema_table;
  tabulate::Table desc_table;

  auto footer = &(format_reader_->file_footer_);
  auto col_infos = &(footer->colinfo());

  // verify schema exist
  auto max_id = footer->types_size();
  CBDB_CHECK(max_id > 0, cbdb::CException::ExType::kExTypeInvalidORCFormat);

  // verify schema defined
  auto struct_types = &(footer->types(0));
  CBDB_CHECK(struct_types->kind() == pax::orc::proto::Type_Kind_STRUCT,
             cbdb::CException::ExType::kExTypeInvalidORCFormat);
  CBDB_CHECK(struct_types->subtypes_size() == col_infos->size(),
             cbdb::CException::ExType::kExTypeInvalidORCFormat);

  // create desc header, types and basic info
  tabulate::Table::Row_t desc_table_header{""};
  tabulate::Table::Row_t desc_table_types{"Type"};
  tabulate::Table::Row_t desc_table_typeids{"Type id"};
  tabulate::Table::Row_t desc_table_collation{"Collation"};
  tabulate::Table::Row_t desc_table_opfamilys{"Operator family"};

  int64 column_start, column_end;
  bool succ;
  std::tie(column_start, column_end, succ) =
      ParseRange(config_->column_id_start, config_->column_id_len,
                 struct_types->subtypes_size());
  if (!succ) {
    std::cout << "Fail to parse the column range. \n"
              << "file: " << config_->file_name << "\n"
              << "column range should in [0, " << struct_types->subtypes_size()
              << ")" << std::endl;
    CBDB_RAISE(cbdb::CException::ExType::kExTypeInvalid);
  }

  for (int j = column_start; j < column_end; ++j) {
    int sub_type_id = static_cast<int>(struct_types->subtypes(j)) + 1;
    auto sub_type = &(footer->types(sub_type_id));

    desc_table_header.emplace_back(std::string("column" + std::to_string(j)));
    desc_table_types.emplace_back(sub_type->DebugString());
    desc_table_typeids.emplace_back(std::to_string((*col_infos)[j].typid()));
    desc_table_collation.emplace_back(
        std::to_string((*col_infos)[j].collation()));
    desc_table_opfamilys.emplace_back(
        std::to_string((*col_infos)[j].opfamily()));
  }

  desc_table.add_row(desc_table_header);
  desc_table.add_row(desc_table_types);
  desc_table.add_row(desc_table_typeids);
  desc_table.add_row(desc_table_collation);
  desc_table.add_row(desc_table_opfamilys);

  schema_table.add_row(tabulate::Table::Row_t{"Schema description"});
  schema_table[0].format().font_align(tabulate::FontAlign::center);
  schema_table[0].format().font_color(tabulate::Color::red);

  schema_table.add_row(tabulate::Table::Row_t{desc_table});
  schema_table[1].format().hide_border_top();

  std::cout << schema_table << std::endl;
}

void PaxDumpReader::DumpFooter() {
  tabulate::Table footer_table;
  tabulate::Table desc_table;

  auto footer = &(format_reader_->file_footer_);

  desc_table.add_row(
      {"Length of Content", std::to_string(footer->contentlength())});
  desc_table.add_row(
      {"Number Of Groups", std::to_string(footer->stripes_size())});
  desc_table.add_row(
      {"Number Of Columns", std::to_string(footer->colinfo_size())});
  desc_table.add_row(
      {"Number Of Rows", std::to_string(footer->numberofrows())});
  desc_table.add_row(
      {"Storage Format", GetStorageFormat(footer->storageformat())});

  footer_table.add_row(tabulate::Table::Row_t{"Footer description"});
  footer_table[0].format().font_align(tabulate::FontAlign::center);
  footer_table[0].format().font_color(tabulate::Color::red);

  footer_table.add_row(tabulate::Table::Row_t{desc_table});
  footer_table[1].format().hide_border_top();

  std::cout << footer_table << std::endl;
}

void PaxDumpReader::DumpGroupInfo() {
  auto footer = &(format_reader_->file_footer_);
  auto stripes = &(footer->stripes());
  auto number_of_column = footer->colinfo_size();

  bool succ;
  int64 group_start, group_end;
  int64 column_start, column_end;

  std::tie(group_start, group_end, succ) = ParseRange(
      config_->group_id_start, config_->group_id_len, stripes->size());
  if (!succ) {
    std::cout << "Fail to parse the group range. \n"
              << "file: " << config_->file_name << "\n"
              << "group range should in [0, " << stripes->size() << ")"
              << std::endl;
    CBDB_RAISE(cbdb::CException::ExType::kExTypeInvalid);
  }

  std::tie(column_start, column_end, succ) = ParseRange(
      config_->column_id_start, config_->column_id_len, number_of_column);
  if (!succ) {
    std::cout << "Fail to parse the column range. \n"
              << "file: " << config_->file_name << "\n"
              << "column range should in [0, " << number_of_column << ")"
              << std::endl;
    CBDB_RAISE(cbdb::CException::ExType::kExTypeInvalid);
  }

  for (int i = group_start; i < group_end; i++) {
    tabulate::Table group_table;
    tabulate::Table group_desc_table;

    tabulate::Table group_col_desc_table;
    tabulate::Table::Row_t col_desc_table_header{""};
    tabulate::Table::Row_t col_desc_table_allnulls{"All null"};
    tabulate::Table::Row_t col_desc_table_hasnulls{"Has null"};
    tabulate::Table::Row_t col_desc_table_mins{"Minimal"};
    tabulate::Table::Row_t col_desc_table_maxs{"Maximum"};

    auto stripe = (*stripes)[i];

    CBDB_CHECK(stripe.colstats_size() == number_of_column,
               cbdb::CException::ExType::kExTypeInvalidORCFormat);

    // full group desc
    group_desc_table.add_row({"Group no", std::to_string(i)});
    group_desc_table.add_row({"Offset", std::to_string(stripe.offset())});
    group_desc_table.add_row(
        {"Data length", std::to_string(stripe.datalength())});
    group_desc_table.add_row(
        {"Stripe footer length", std::to_string(stripe.footerlength())});
    group_desc_table.add_row(
        {"Number of rows", std::to_string(stripe.numberofrows())});

    // full group col statistics desc
    for (int j = column_start; j < column_end; j++) {
      const auto &col_stats = stripe.colstats(j);
      const auto &col_data_stats = col_stats.coldatastats();
      col_desc_table_header.emplace_back(
          std::string("column" + std::to_string(j)));
      col_desc_table_allnulls.emplace_back(BoolCast(col_stats.allnull()));
      col_desc_table_hasnulls.emplace_back(BoolCast(col_stats.hasnull()));

      int64 minimal_val;
      int64 maximum_val;
      bool support = true;

      switch (col_data_stats.maximum().size()) {
        case 1: {
          minimal_val = *reinterpret_cast<const int8 *>(  // NOLINT
              col_data_stats.minimal().data());
          maximum_val = *reinterpret_cast<const int8 *>(  // NOLINT
              col_data_stats.maximum().data());
          break;
        }
        case 2: {
          minimal_val =
              *reinterpret_cast<const int16 *>(col_data_stats.minimal().data());
          maximum_val =
              *reinterpret_cast<const int16 *>(col_data_stats.maximum().data());
          break;
        }
        case 4: {
          minimal_val =
              *reinterpret_cast<const int32 *>(col_data_stats.minimal().data());
          maximum_val =
              *reinterpret_cast<const int32 *>(col_data_stats.maximum().data());
          break;
        }
        case 8: {
          minimal_val =
              *reinterpret_cast<const int64 *>(col_data_stats.minimal().data());
          maximum_val =
              *reinterpret_cast<const int64 *>(col_data_stats.maximum().data());
          break;
        }
        default: {
          support = false;
        }
      }

      if (support) {
        col_desc_table_mins.emplace_back(std::to_string(minimal_val));
        col_desc_table_maxs.emplace_back(std::to_string(maximum_val));
      } else {
        col_desc_table_mins.emplace_back(col_data_stats.minimal());
        col_desc_table_maxs.emplace_back(col_data_stats.maximum());
      }
    }

    // build group col desc table
    group_col_desc_table.add_row(col_desc_table_header);
    group_col_desc_table.add_row(col_desc_table_allnulls);
    group_col_desc_table.add_row(col_desc_table_hasnulls);
    group_col_desc_table.add_row(col_desc_table_mins);
    group_col_desc_table.add_row(col_desc_table_maxs);

    // build group table
    group_table.add_row(tabulate::Table::Row_t{
        std::string("Group description " + std::to_string(i))});
    group_table[0].format().font_align(tabulate::FontAlign::center);
    group_table[0].format().font_color(tabulate::Color::red);
    group_table.add_row(tabulate::Table::Row_t{group_desc_table});
    group_table[1].format().hide_border_top();

    group_table.add_row(tabulate::Table::Row_t{group_col_desc_table});
    group_table[2].format().hide_border_top();

    std::cout << group_table << std::endl;
  }
}

void PaxDumpReader::DumpGroupFooter() {
  DataBuffer<char> *data_buffer = nullptr;
  auto footer = &(format_reader_->file_footer_);
  auto stripes = &(footer->stripes());
  auto number_of_column = footer->colinfo_size();

  bool succ;
  int64 group_start, group_end;
  int64 column_start, column_end;

  std::tie(group_start, group_end, succ) = ParseRange(
      config_->group_id_start, config_->group_id_len, stripes->size());
  if (!succ) {
    std::cout << "Fail to parse the group range. \n"
              << "file: " << config_->file_name << "\n"
              << "group range should in [0, " << stripes->size() << ")"
              << std::endl;
    CBDB_RAISE(cbdb::CException::ExType::kExTypeInvalid);
  }

  std::tie(column_start, column_end, succ) = ParseRange(
      config_->column_id_start, config_->column_id_len, number_of_column);
  if (!succ) {
    std::cout << "Fail to parse the column range. \n"
              << "file: " << config_->file_name << "\n"
              << "column range should in [0, " << number_of_column << ")"
              << std::endl;
    CBDB_RAISE(cbdb::CException::ExType::kExTypeInvalid);
  }

  data_buffer = new DataBuffer<char>(8192);

  size_t streams_index;
  for (int i = group_start; i < group_end; i++) {
    auto stripe_footer = format_reader_->ReadStripeFooter(data_buffer, i);
    const pax::orc::proto::Stream *n_stream = nullptr;
    const pax::ColumnEncoding *column_encoding = nullptr;

    tabulate::Table group_footer_table;
    tabulate::Table group_footer_desc_tables;
    tabulate::Table::Row_t group_footer_descs;
    tabulate::Table::Row_t group_footer_desc_cids;
    tabulate::Table stream_desc_table;

    streams_index = 0;

    for (int j = 0; j < column_start;) {
      n_stream = &stripe_footer.streams(streams_index++);
      if (n_stream->kind() == ::pax::orc::proto::Stream_Kind::Stream_Kind_DATA) {
        j++;
      }
    }

    for (int j = column_start; j < column_end;) {
      n_stream = &stripe_footer.streams(streams_index++);
      stream_desc_table.add_row(
          {"Stream type", std::to_string(n_stream->kind())});
      stream_desc_table.add_row({"Column", std::to_string(n_stream->column())});
      stream_desc_table.add_row({"Length", std::to_string(n_stream->length())});

      if (n_stream->kind() == ::pax::orc::proto::Stream_Kind::Stream_Kind_DATA) {
        column_encoding = &stripe_footer.pax_col_encodings(j);

        tabulate::Table group_footer_desc_table;

        group_footer_desc_table.add_row(
            {"Compress type", std::to_string(column_encoding->kind())});
        group_footer_desc_table.add_row(
            {"Compress level",
             std::to_string(column_encoding->compress_lvl())});
        group_footer_desc_table.add_row(
            {"Origin length", std::to_string(column_encoding->length())});
        group_footer_desc_table.add_row({"Streams", stream_desc_table});
        stream_desc_table = tabulate::Table();

        group_footer_desc_cids.emplace_back(
            std::string("Column" + std::to_string(j)));
        group_footer_descs.emplace_back(group_footer_desc_table);
        j++;
      }
    }

    group_footer_desc_tables.add_row(group_footer_desc_cids);
    group_footer_desc_tables.add_row(group_footer_descs);

    group_footer_table.add_row(tabulate::Table::Row_t{
        std::string("Group footer description " + std::to_string(i))});
    group_footer_table[0].format().font_align(tabulate::FontAlign::center);
    group_footer_table[0].format().font_color(tabulate::Color::red);
    group_footer_table.add_row(
        tabulate::Table::Row_t{group_footer_desc_tables});
    group_footer_table[1].format().hide_border_top();

    std::cout << group_footer_table << std::endl;
  }

  delete data_buffer;
}

void PaxDumpReader::DumpAllData() {
  auto footer = &(format_reader_->file_footer_);
  auto stripes = &(footer->stripes());
  auto number_of_columns = footer->colinfo_size();

  bool succ;
  int64 group_start, group_end;
  int64 column_start, column_end;
  int64 row_start, row_end;
  bool is_vec = footer->storageformat() == kTypeStorageOrcVec;

  std::tie(group_start, group_end, succ) = ParseRange(
      config_->group_id_start, config_->group_id_len, stripes->size());
  if (!succ || config_->group_id_len != 1) {
    std::cout << "Invalid group range, with option -d/--print-data the range "
                 "length should be 1 \n"
              << "file: " << config_->file_name << "\n"
              << "group range shoule in [0, " << stripes->size() << ")"
              << std::endl;
    CBDB_RAISE(cbdb::CException::ExType::kExTypeInvalid);
  }

  std::tie(column_start, column_end, succ) = ParseRange(
      config_->column_id_start, config_->column_id_len, number_of_columns);
  if (!succ) {
    std::cout << "Fail to parse the column range. \n"
              << "file: " << config_->file_name << "\n"
              << "column range should in [0, " << number_of_columns << ")"
              << std::endl;
    CBDB_RAISE(cbdb::CException::ExType::kExTypeInvalid);
  }

  DataBuffer<char> *data_buffer = nullptr;
  auto stripe_info = (*stripes)[group_start];
  size_t number_of_rows = 0;
  pax::orc::proto::StripeFooter stripe_footer;
  bool proj_map[number_of_columns];
  PaxColumns *columns = nullptr;
  OrcGroup *group = nullptr;

  data_buffer = new DataBuffer<char>(8192);
  number_of_rows = stripe_info.numberofrows();
  stripe_footer = format_reader_->ReadStripeFooter(data_buffer, group_start);

  std::tie(row_start, row_end, succ) =
      ParseRange(config_->row_id_start, config_->row_id_len, number_of_rows);
  if (!succ) {
    std::cout << "Fail to parse the row range. \n"
              << "file: " << config_->file_name << "\n"
              << "row range should in [0, " << number_of_rows << ")"
              << std::endl;
    CBDB_RAISE(cbdb::CException::ExType::kExTypeInvalid);
  }

  memset(proj_map, false, number_of_columns);
  for (int column_index = column_start; column_index < column_end;
       column_index++) {
    proj_map[column_index] = true;
  }

  columns =
      format_reader_->ReadStripe(group_start, proj_map, number_of_columns);

  if (!is_vec)
    group = new OrcGroup(columns, 0);
  else
    group = new OrcVecGroup(columns, 0);

  tabulate::Table data_table;
  tabulate::Table data_datum_table;
  tabulate::Table::Row_t data_table_header;
  for (int column_index = column_start; column_index < column_end;
       column_index++) {
    data_table_header.emplace_back(
        std::string("Column" + std::to_string(column_index)));
  }
  data_datum_table.add_row(data_table_header);

  for (int row_index = row_start; row_index < row_end; row_index++) {
    tabulate::Table::Row_t current_row;

    for (int column_index = column_start; column_index < column_end;
         column_index++) {
      Datum d;
      bool null;
      PaxColumn *column = (*columns)[column_index];

      std::tie(d, null) = group->GetColumnValue(column_index, row_index);
      if (null) {
        current_row.emplace_back("");
      } else {
        switch (column->GetPaxColumnTypeInMem()) {
          case kTypeNonFixed:
            current_row.emplace_back(std::string(DatumGetPointer(d)));
            break;
          case kTypeFixed: {
            switch (column->GetTypeLength()) {
              case 1:
                current_row.emplace_back(std::to_string(cbdb::Int8ToDatum(d)));
                break;
              case 2:
                current_row.emplace_back(std::to_string(cbdb::Int16ToDatum(d)));
                break;
              case 4:
                current_row.emplace_back(std::to_string(cbdb::Int32ToDatum(d)));
                break;
              case 8:
                current_row.emplace_back(std::to_string(cbdb::Int64ToDatum(d)));
                break;
              default:
                Assert(
                    !"should't be here, fixed type len should be 1, 2, 4, 8");
            }
            break;
          }
          case kTypeDecimal: {
            CBDB_WRAP_START;
            {
              auto numeric_str = numeric_normalize(DatumGetNumeric(d));
              current_row.emplace_back(std::string(numeric_str));
              pfree(numeric_str);
            }
            CBDB_WRAP_END;
            break;
          }
          default:
            Assert(!"should't be here, non-implemented column type in memory");
            break;
        }
      }
    }

    data_datum_table.add_row(current_row);
  }

  data_table.add_row(tabulate::Table::Row_t{"Table data"});
  data_table[0].format().font_align(tabulate::FontAlign::center);
  data_table[0].format().font_color(tabulate::Color::red);

  data_table.add_row(tabulate::Table::Row_t{data_datum_table});
  data_table[1].format().hide_border_top();

  std::cout << data_table << std::endl;
  delete data_buffer;
  delete group;
}

}  // namespace pax::tools
