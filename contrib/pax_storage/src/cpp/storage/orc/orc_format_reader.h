#pragma once

#include "storage/columns/pax_columns.h"
#include "storage/file_system.h"
#include "storage/proto/proto_wrappers.h"
#include "storage/proto/protobuf_stream.h"

namespace pax {
namespace tools { class PaxDumpReader; }
class OrcFormatReader final {
 public:
  explicit OrcFormatReader(File *file);

  ~OrcFormatReader();

  void SetReusedBuffer(DataBuffer<char> *data_buffer);

  void Open();

  void Close();

  size_t GetStripeNums() const;

  size_t GetStripeNumberOfRows(size_t stripe_index);

  size_t GetStripeOffset(size_t stripe_index);

  PaxColumns *ReadStripe(size_t group_index, bool *proj_map = nullptr,
                         size_t proj_len = 0);

 private:
  pax::orc::proto::StripeFooter ReadStripeWithProjection(
      DataBuffer<char> *data_buffer,
      const ::pax::orc::proto::StripeInformation &stripe_info, const bool *proj_map,
      size_t proj_len);

  pax::orc::proto::StripeFooter ReadStripeFooter(DataBuffer<char> *data_buffer,
                                            size_t sf_length, size_t sf_offset,
                                            size_t sf_data_len);

  pax::orc::proto::StripeFooter ReadStripeFooter(DataBuffer<char> *data_buffer,
                                            size_t stripe_index);

  void BuildProtoTypes();

 private:
  friend class tools::PaxDumpReader;
  friend class OrcGroupStatsProvider;
  std::vector<pax::orc::proto::Type_Kind> column_types_;
  File *file_;
  DataBuffer<char> *reused_buffer_;
  size_t num_of_stripes_;
  bool is_vec_;

  std::vector<size_t> stripe_row_offsets_;

  pax::orc::proto::PostScript post_script_;
  pax::orc::proto::Footer file_footer_;
};

}  // namespace pax
