#pragma once
#include "comm/cbdb_api.h"

#include "comm/cbdb_wrappers.h"
#include "storage/micro_partition.h"

namespace pax {

namespace tools {
class PaxDumpReader;
}

class OrcGroup : public MicroPartitionReader::Group {
 public:
  OrcGroup(PaxColumns *pax_column, size_t row_offset,
           const std::vector<int> *proj_col_index,
           Bitmap8 *micro_partition_visibility_bitmap = nullptr);

  ~OrcGroup() override;

  size_t GetRows() const override;

  size_t GetRowOffset() const override;

  PaxColumns *GetAllColumns() const override;

  std::pair<bool, size_t> ReadTuple(TupleTableSlot *slot) override;

  bool GetTuple(TupleTableSlot *slot, size_t row_index) override;

  std::pair<Datum, bool> GetColumnValue(TupleDesc desc, size_t column_index,
                                        size_t row_index) override;
  void SetVisibilityMap(std::shared_ptr<Bitmap8>visibility_bitmap) {
    micro_partition_visibility_bitmap_ = visibility_bitmap;
  }

 protected:
  // Used to direct get datum from columns
  virtual std::pair<Datum, bool> GetColumnValue(size_t column_index,
                                                size_t row_index);

  virtual std::pair<Datum, bool> GetColumnValue(PaxColumn *column,
                                                size_t row_index);

  // Used in `ReadTuple`
  // Different from the other `GetColumnValue` function, in this function, if a
  // null row is encountered, then we will perform an accumulation operation on
  // `null_counts`. If no null row is encountered, the offset of the row data
  // will be calculated through `null_counts`. The other `GetColumnValue`
  // function are less efficient in `foreach` because they have to calculate the
  // offset of the row data from scratch every time.
  virtual std::pair<Datum, bool> GetColumnValue(PaxColumn *column,
                                                size_t row_index,
                                                uint32 *null_counts);

 protected:
  PaxColumns *pax_columns_;
  // only referenced
  std::shared_ptr<Bitmap8>micro_partition_visibility_bitmap_;
  size_t row_offset_;
  size_t current_row_index_;

 private:
  friend class tools::PaxDumpReader;
  uint32 *current_nulls_ = nullptr;
  // only a reference, owner by pax_filter
  const std::vector<int> *proj_col_index_;
};

class OrcVecGroup final : public OrcGroup {
 public:
  OrcVecGroup(PaxColumns *pax_column, size_t row_offset,
              const std::vector<int> *proj_col_index);

  ~OrcVecGroup() override;

 private:
  std::pair<Datum, bool> GetColumnValue(size_t column_index,
                                        size_t row_index) override;

  std::pair<Datum, bool> GetColumnValue(PaxColumn *column,
                                        size_t row_index) override;

  std::pair<Datum, bool> GetColumnValue(PaxColumn *column, size_t row_index,
                                        uint32 *null_counts) override;

 private:
  std::vector<struct varlena *> buffer_holder_;
};

}  // namespace pax
