#pragma once
#include <cstdlib>

namespace pax {
class OrcFormatReader;
namespace tools {

#define NO_SPEC_ID -1
#define NO_SPEC_LEN 0

struct DumpConfig {
  ~DumpConfig() {
    if (file_name) {
      free(file_name); // NOLINT
    }
  }

  char *file_name = nullptr;
  bool print_all = false;
  bool print_all_desc = false;
  bool print_post_script = false;
  bool print_footer = false;
  bool print_schema = false;
  bool print_group_info = false;
  bool print_group_footer = false;
  bool print_all_data = false;

  int64_t group_id_start = NO_SPEC_ID;
  int64_t group_id_len = NO_SPEC_LEN;

  int64_t column_id_start = NO_SPEC_ID;
  int64_t column_id_len = NO_SPEC_LEN;

  int64_t row_id_start = NO_SPEC_ID;
  int64_t row_id_len = NO_SPEC_LEN;
};

class PaxDumpReader final {
 public:
  explicit PaxDumpReader(DumpConfig *config);

  bool Initialize();
  void Release();

  void Dump();

 private:
  void DumpAllInfo();
  void DumpAllDesc();
  void DumpPostScript();
  void DumpFooter();
  void DumpSchema();
  void DumpGroupInfo();
  void DumpGroupFooter();
  void DumpAllData();

 private:
  DumpConfig *config_;
  OrcFormatReader *format_reader_;
};

}  // namespace tools
}  // namespace pax
