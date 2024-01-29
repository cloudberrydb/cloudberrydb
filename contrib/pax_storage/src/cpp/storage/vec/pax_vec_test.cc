#include "comm/gtest_wrappers.h"
#include "pax_gtest_helper.h"
#include "storage/pax.h"
#include "storage/vec/pax_vec_adapter.h"
#ifdef VEC_BUILD
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-but-set-variable"

extern "C" {
#include "utils/tuptable_vec.h"  // for vec tuple
}

#pragma GCC diagnostic pop
#include "storage/vec/arrow_wrapper.h"
#endif  // VEC_BUILD

namespace pax::tests {

#ifdef VEC_BUILD
using ::testing::_;
using ::testing::AtLeast;
using ::testing::Return;

static void GenFakeBuffer(char *buffer, size_t length) {
  for (size_t i = 0; i < length; i++) {
    buffer[i] = static_cast<char>(i);
  }
}

class PaxVecTest : public ::testing::TestWithParam<bool> {
 public:
  void SetUp() override {
    Singleton<LocalFileSystem>::GetInstance()->Delete(file_name_);
    CreateMemoryContext();
    CreateTestResourceOwner();
  }

  static CTupleSlot *CreateCtuple(bool is_fixed, bool with_value = false) {
    TupleTableSlot *tuple_slot;
    TupleDescData *tuple_desc;
    CTupleSlot *ctuple_slot;

    tuple_desc = reinterpret_cast<TupleDescData *>(cbdb::Palloc0(
        sizeof(TupleDescData) + sizeof(FormData_pg_attribute) * 1));

    tuple_desc->natts = 1;
    if (is_fixed) {
      tuple_desc->attrs[0] = {.atttypid = INT4OID,
                              .attlen = 4,
                              .attbyval = true,
                              .attalign = TYPALIGN_INT,
                              .attcollation = InvalidOid};
    } else {
      tuple_desc->attrs[0] = {.atttypid = TEXTOID,
                              .attlen = -1,
                              .attbyval = false,
                              .attalign = TYPALIGN_DOUBLE,
                              .attcollation = DEFAULT_COLLATION_OID};
    }

    tuple_slot = (TupleTableSlot *)cbdb::RePalloc(
        MakeTupleTableSlot(tuple_desc, &TTSOpsVirtual),
        MAXALIGN(TTSOpsVirtual.base_slot_size) +
            MAXALIGN(tuple_desc->natts * sizeof(Datum)) +
            MAXALIGN(tuple_desc->natts * sizeof(bool)) +
            MAXALIGN(sizeof(VecTupleTableSlot)));

    if (with_value) {
      if (is_fixed) {
        tuple_slot->tts_values[0] = cbdb::Int32ToDatum(0x123);
      } else {
        char column_buff[100];
        GenFakeBuffer(column_buff, 100);
        tuple_slot->tts_values[0] = cbdb::DatumFromCString(column_buff, 100);
      }

      bool *fake_is_null =
          reinterpret_cast<bool *>(cbdb::Palloc0(sizeof(bool)));
      fake_is_null[0] = false;
      tuple_slot->tts_isnull = fake_is_null;
    }

    tuple_slot->tts_tupleDescriptor = tuple_desc;
    ctuple_slot = new CTupleSlot(tuple_slot);

    return ctuple_slot;
  }

  static void DeleteCTupleSlot(CTupleSlot *ctuple_slot) {
    auto tuple_table_slot = ctuple_slot->GetTupleTableSlot();
    cbdb::Pfree(tuple_table_slot->tts_tupleDescriptor);
    cbdb::Pfree(tuple_table_slot);
    delete ctuple_slot;
  }

  void TearDown() override {
    Singleton<LocalFileSystem>::GetInstance()->Delete(file_name_);
    ReleaseTestResourceOwner();
  }

 protected:
  const char *file_name_ = "./test.file";
};

TEST_P(PaxVecTest, PaxColumnToVec) {
  VecAdapter *adapter;
  PaxColumns *columns;
  PaxColumn *column;

  auto is_fixed = GetParam();
  auto ctuple_slot = CreateCtuple(is_fixed);

  adapter = new VecAdapter(ctuple_slot->GetTupleDesc());
  columns = new PaxColumns();
  if (is_fixed) {
    column = new PaxCommColumn<int32>(VEC_BATCH_LENGTH + 1000);
  } else {
    column = new PaxNonFixedColumn(VEC_BATCH_LENGTH + 1000);
  }

  for (size_t i = 0; i < VEC_BATCH_LENGTH + 1000; i++) {
    if (is_fixed) {
      column->Append((char *)&i, sizeof(int32));
    } else {
      auto data = cbdb::DatumFromCString((char *)&i, sizeof(int32));
      int len = -1;
      auto vl = cbdb::PointerAndLenFromDatum(data, &len);

      column->Append(reinterpret_cast<char *>(vl), len);
    }
  }

  columns->AddRows(column->GetRows());
  columns->Append(column);
  adapter->SetDataSource(columns);
  auto append_rc = adapter->AppendToVecBuffer();
  ASSERT_TRUE(append_rc);

  // already full
  append_rc = adapter->AppendToVecBuffer();
  ASSERT_FALSE(append_rc);

  size_t flush_counts = adapter->FlushVecBuffer(ctuple_slot);
  ASSERT_EQ(VEC_BATCH_LENGTH, flush_counts);

  // verify ctuple_slot 1
  {
    VecTupleTableSlot *vslot = nullptr;
    TupleTableSlot *tuple_table_slot = ctuple_slot->GetTupleTableSlot();
    vslot = (VecTupleTableSlot *)tuple_table_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ArrowArray *arrow_array = &rb->batch;
    ASSERT_EQ(arrow_array->length, VEC_BATCH_LENGTH);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, nullptr);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(child_array->length, VEC_BATCH_LENGTH);
    ASSERT_EQ(child_array->null_count, 0);
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_EQ(child_array->buffers[0], nullptr);  // null bitmap
    ASSERT_EQ(child_array->private_data, child_array);

    if (is_fixed) {
      ASSERT_NE(child_array->buffers[1], nullptr);

      char *buffer = (char *)child_array->buffers[1];
      for (size_t i = 0; i < VEC_BATCH_LENGTH; i++) {
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))), i);
      }
    } else {
      ASSERT_NE(child_array->buffers[1], nullptr);
      ASSERT_NE(child_array->buffers[2], nullptr);

      char *offset_buffer = (char *)child_array->buffers[1];
      char *buffer = (char *)child_array->buffers[2];
      for (size_t i = 0; i < VEC_BATCH_LENGTH; i++) {
        ASSERT_EQ(*((int32 *)(offset_buffer + i * sizeof(int32))),
                  i * sizeof(int32));
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))), i);
      }

      ASSERT_EQ(*((int32 *)(offset_buffer + VEC_BATCH_LENGTH * sizeof(int32))),
                VEC_BATCH_LENGTH * sizeof(int32));
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
  }

  append_rc = adapter->AppendToVecBuffer();
  ASSERT_TRUE(append_rc);

  flush_counts = adapter->FlushVecBuffer(ctuple_slot);
  ASSERT_EQ(1000, flush_counts);

  // verify ctuple_slot 2
  {
    VecTupleTableSlot *vslot = nullptr;
    TupleTableSlot *tuple_table_slot = ctuple_slot->GetTupleTableSlot();
    vslot = (VecTupleTableSlot *)tuple_table_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ASSERT_NE(rb, nullptr);
    ArrowArray *arrow_array = &rb->batch;
    ASSERT_EQ(arrow_array->length, 1000);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, nullptr);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(child_array->length, 1000);
    ASSERT_EQ(child_array->null_count, 0);
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_EQ(child_array->buffers[0], nullptr);  // null bitmap
    ASSERT_EQ(child_array->private_data, child_array);

    if (is_fixed) {
      ASSERT_NE(child_array->buffers[1], nullptr);

      char *buffer = (char *)child_array->buffers[1];
      for (size_t i = 0; i < 1000; i++) {
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))),
                  i + VEC_BATCH_LENGTH);
      }
    } else {
      ASSERT_NE(child_array->buffers[1], nullptr);
      ASSERT_NE(child_array->buffers[2], nullptr);

      char *offset_buffer = (char *)child_array->buffers[1];
      char *buffer = (char *)child_array->buffers[2];
      for (size_t i = 0; i < 1000; i++) {
        ASSERT_EQ(*((int32 *)(offset_buffer + i * sizeof(int32))),
                  i * sizeof(int32));
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))),
                  i + VEC_BATCH_LENGTH);
      }

      ASSERT_EQ(*((int32 *)(offset_buffer + 1000 * sizeof(int32))),
                1000 * sizeof(int32));
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
  }

  DeleteCTupleSlot(ctuple_slot);

  delete columns;
  delete adapter;
}

TEST_P(PaxVecTest, PaxColumnWithNullToVec) {
  VecAdapter *adapter;
  PaxColumns *columns;
  PaxColumn *column;
  CTupleSlot *ctuple_slot;
  size_t null_counts = 0;
  auto is_fixed = GetParam();

  ctuple_slot = CreateCtuple(is_fixed);

  adapter = new VecAdapter(ctuple_slot->GetTupleDesc());
  columns = new PaxColumns();
  if (is_fixed) {
    column = new PaxCommColumn<int32>(VEC_BATCH_LENGTH + 1000);
  } else {
    column = new PaxNonFixedColumn(VEC_BATCH_LENGTH + 1000);
  }

  for (size_t i = 0; i < VEC_BATCH_LENGTH + 1000; i++) {
    if (i % 5 == 0) {
      null_counts++;
      column->AppendNull();
    }

    if (is_fixed) {
      column->Append((char *)&i, sizeof(int32));
    } else {
      auto data = cbdb::DatumFromCString((char *)&i, sizeof(int32));
      int len = -1;
      auto vl = cbdb::PointerAndLenFromDatum(data, &len);

      column->Append(reinterpret_cast<char *>(vl), len);
    }
  }

  columns->AddRows(column->GetRows());
  columns->Append(column);
  adapter->SetDataSource(columns);

  auto append_rc = adapter->AppendToVecBuffer();
  ASSERT_TRUE(append_rc);

  append_rc = adapter->AppendToVecBuffer();
  ASSERT_FALSE(append_rc);

  size_t flush_counts = adapter->FlushVecBuffer(ctuple_slot);
  ASSERT_EQ(VEC_BATCH_LENGTH, flush_counts);

  {
    VecTupleTableSlot *vslot = nullptr;
    TupleTableSlot *tuple_table_slot = ctuple_slot->GetTupleTableSlot();
    vslot = (VecTupleTableSlot *)tuple_table_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ASSERT_NE(rb, nullptr);
    ArrowArray *arrow_array = &rb->batch;
    ASSERT_EQ(arrow_array->length, VEC_BATCH_LENGTH);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, nullptr);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(child_array->length, VEC_BATCH_LENGTH);
    ASSERT_EQ(
        child_array->null_count,
        VEC_BATCH_LENGTH - column->GetRangeNonNullRows(0, VEC_BATCH_LENGTH));
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_EQ(child_array->private_data, child_array);

    if (is_fixed) {
      ASSERT_NE(child_array->buffers[0], nullptr);
      ASSERT_NE(child_array->buffers[1], nullptr);

      auto null_bits_array = (uint8 *)child_array->buffers[0];

      // verify null bitmap
      for (size_t i = 0; i < VEC_BATCH_LENGTH; i++) {
        // N 0 1 2 3 4 N 5 6 7 8 9 N 10 11 ...
        // should % 6 rather then 5
        if (i % 6 == 0) {
          ASSERT_FALSE(arrow::bit_util::GetBit(null_bits_array, i));
        } else {
          ASSERT_TRUE(arrow::bit_util::GetBit(null_bits_array, i));
        }
      }

      // verify data
      char *buffer = (char *)child_array->buffers[1];
      size_t verify_null_counts = 0;
      for (size_t i = 0; i < VEC_BATCH_LENGTH; i++) {
        if (i % 6 == 0) {
          verify_null_counts++;
          continue;
        }
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))),
                  i - verify_null_counts);
      }

      ASSERT_EQ(verify_null_counts, child_array->null_count);
    } else {
      ASSERT_NE(child_array->buffers[0], nullptr);
      ASSERT_NE(child_array->buffers[1], nullptr);
      ASSERT_NE(child_array->buffers[2], nullptr);

      auto null_bits_array = (uint8 *)child_array->buffers[0];

      // verify null bitmap
      for (size_t i = 0; i < VEC_BATCH_LENGTH; i++) {
        if (i % 6 == 0) {
          ASSERT_FALSE(arrow::bit_util::GetBit(null_bits_array, i));
        } else {
          ASSERT_TRUE(arrow::bit_util::GetBit(null_bits_array, i));
        }
      }

      // verify offset data
      char *offset_buffer = (char *)child_array->buffers[1];
      size_t last_offset = 0;
      size_t verify_null_counts = 0;
      for (size_t i = 0; i < VEC_BATCH_LENGTH; i++) {
        if (i % 6 == 0) {
          verify_null_counts++;
          ASSERT_EQ(*((int32 *)(offset_buffer + i * sizeof(int32))),
                    last_offset == 0 ? 0 : last_offset + sizeof(int32));
          continue;
        }
        ASSERT_EQ(*((int32 *)(offset_buffer + i * sizeof(int32))),
                  (i - verify_null_counts) * sizeof(int32));
        last_offset = *((int32 *)(offset_buffer + i * sizeof(int32)));
      }

      ASSERT_EQ(*((int32 *)(offset_buffer + VEC_BATCH_LENGTH * sizeof(int32))),
                last_offset + sizeof(int32));
      ASSERT_EQ(verify_null_counts, child_array->null_count);

      // verify data
      char *buffer = (char *)child_array->buffers[2];
      for (size_t i = 0; i < VEC_BATCH_LENGTH - verify_null_counts; i++) {
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))), i);
      }

      ASSERT_EQ(verify_null_counts, child_array->null_count);
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
  }

  append_rc = adapter->AppendToVecBuffer();
  ASSERT_TRUE(append_rc);

  flush_counts = adapter->FlushVecBuffer(ctuple_slot);
  ASSERT_EQ(null_counts + 1000, flush_counts);

  {
    VecTupleTableSlot *vslot = nullptr;
    TupleTableSlot *tuple_table_slot = ctuple_slot->GetTupleTableSlot();
    vslot = (VecTupleTableSlot *)tuple_table_slot;

    size_t range_size = null_counts + 1000;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ASSERT_NE(rb, nullptr);
    ArrowArray *arrow_array = &rb->batch;
    ASSERT_EQ(arrow_array->length, range_size);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, nullptr);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(child_array->length, range_size);
    ASSERT_EQ(
        child_array->null_count,
        range_size - column->GetRangeNonNullRows(VEC_BATCH_LENGTH, range_size));
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_EQ(child_array->private_data, child_array);

    if (is_fixed) {
      ASSERT_NE(child_array->buffers[0], nullptr);
      ASSERT_NE(child_array->buffers[1], nullptr);

      auto null_bits_array = (uint8 *)child_array->buffers[0];
      char *buffer = (char *)child_array->buffers[1];

      size_t verify_null_counts = 0;
      size_t start = column->GetRangeNonNullRows(0, VEC_BATCH_LENGTH);
      for (size_t i = 0; i < range_size; i++) {
        if (arrow::bit_util::GetBit(null_bits_array, i)) {
          ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))), start++);
        } else {
          verify_null_counts++;
        }
      }

      ASSERT_EQ(verify_null_counts, child_array->null_count);
    } else {
      ASSERT_NE(child_array->buffers[0], nullptr);
      ASSERT_NE(child_array->buffers[1], nullptr);
      ASSERT_NE(child_array->buffers[2], nullptr);

      // verify null bitmap
      auto null_bits_array = (uint8 *)child_array->buffers[0];

      size_t verify_null_counts = 0;
      for (size_t i = 0; i < range_size; i++) {
        if (!arrow::bit_util::GetBit(null_bits_array, i)) {
          verify_null_counts++;
        }
      }

      ASSERT_EQ(verify_null_counts, child_array->null_count);

      // verify data
      char *buffer = (char *)child_array->buffers[2];
      size_t start = column->GetRangeNonNullRows(0, VEC_BATCH_LENGTH);
      for (size_t i = 0; i < (range_size - child_array->null_count); i++) {
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))), start++);
      }

      // verify offset with data
      char *offset_buffer = (char *)child_array->buffers[1];
      start = column->GetRangeNonNullRows(0, VEC_BATCH_LENGTH);

      verify_null_counts = 0;
      for (size_t i = 0; i < range_size; i++) {
        auto current_offset = *((int32 *)(offset_buffer + i * sizeof(int32)));
        auto next_offset =
            *((int32 *)(offset_buffer + (i + 1) * sizeof(int32)));
        if (current_offset != next_offset) {
          ASSERT_EQ(
              *((int32 *)(buffer + (i - verify_null_counts) * sizeof(int32))),
              start++);
        } else {
          verify_null_counts++;
        }
      }
      ASSERT_EQ(verify_null_counts, child_array->null_count);
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
  }

  DeleteCTupleSlot(ctuple_slot);

  delete columns;
  delete adapter;
}

TEST_P(PaxVecTest, PaxColumnToVecNoFull) {
  VecAdapter *adapter;
  PaxColumns *columns;
  PaxColumn *column;

  auto is_fixed = GetParam();
  auto ctuple_slot = CreateCtuple(is_fixed);

  adapter = new VecAdapter(ctuple_slot->GetTupleDesc());
  columns = new PaxColumns();
  if (is_fixed) {
    column = new PaxCommColumn<int32>(VEC_BATCH_LENGTH + 1000);
  } else {
    column = new PaxNonFixedColumn(VEC_BATCH_LENGTH + 1000);
  }

  for (size_t i = 0; i < 1000; i++) {
    if (is_fixed) {
      column->Append((char *)&i, sizeof(int32));
    } else {
      auto data = cbdb::DatumFromCString((char *)&i, sizeof(int32));
      int len = -1;
      auto vl = cbdb::PointerAndLenFromDatum(data, &len);

      column->Append(reinterpret_cast<char *>(vl), len);
    }
  }

  columns->AddRows(column->GetRows());
  columns->Append(column);
  adapter->SetDataSource(columns);
  auto append_rc = adapter->AppendToVecBuffer();
  ASSERT_TRUE(append_rc);

  // append finish
  append_rc = adapter->AppendToVecBuffer();
  ASSERT_FALSE(append_rc);

  size_t flush_counts = adapter->FlushVecBuffer(ctuple_slot);
  ASSERT_EQ(1000, flush_counts);

  // verify ctuple_slot
  {
    VecTupleTableSlot *vslot = nullptr;
    TupleTableSlot *tuple_table_slot = ctuple_slot->GetTupleTableSlot();
    vslot = (VecTupleTableSlot *)tuple_table_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ASSERT_NE(rb, nullptr);
    ArrowArray *arrow_array = &rb->batch;
    ASSERT_EQ(arrow_array->length, 1000);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, nullptr);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(child_array->length, 1000);
    ASSERT_EQ(child_array->null_count, 0);
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_EQ(child_array->buffers[0], nullptr);  // null bitmap
    ASSERT_EQ(child_array->private_data, child_array);

    if (is_fixed) {
      ASSERT_NE(child_array->buffers[1], nullptr);

      char *buffer = (char *)child_array->buffers[1];
      for (size_t i = 0; i < 1000; i++) {
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))), i);
      }
    } else {
      ASSERT_NE(child_array->buffers[1], nullptr);
      ASSERT_NE(child_array->buffers[2], nullptr);

      char *offset_buffer = (char *)child_array->buffers[1];
      char *buffer = (char *)child_array->buffers[2];
      for (size_t i = 0; i < 1000; i++) {
        ASSERT_EQ(*((int32 *)(offset_buffer + i * sizeof(int32))),
                  i * sizeof(int32));
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))), i);
      }

      ASSERT_EQ(*((int32 *)(offset_buffer + 1000 * sizeof(int32))),
                1000 * sizeof(int32));
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
  }

  DeleteCTupleSlot(ctuple_slot);

  delete columns;
  delete adapter;
}

TEST_P(PaxVecTest, PaxColumnWithNullToVecNoFull) {
  VecAdapter *adapter;
  PaxColumns *columns;
  PaxColumn *column;
  size_t null_counts = 0;

  auto is_fixed = GetParam();
  auto ctuple_slot = CreateCtuple(is_fixed);

  adapter = new VecAdapter(ctuple_slot->GetTupleDesc());
  columns = new PaxColumns();
  if (is_fixed) {
    column = new PaxCommColumn<int32>(VEC_BATCH_LENGTH + 1000);
  } else {
    column = new PaxNonFixedColumn(VEC_BATCH_LENGTH + 1000);
  }

  for (size_t i = 0; i < 1000; i++) {
    if (i % 5 == 0) {
      null_counts++;
      column->AppendNull();
    }

    if (is_fixed) {
      column->Append((char *)&i, sizeof(int32));
    } else {
      auto data = cbdb::DatumFromCString((char *)&i, sizeof(int32));
      int len = -1;
      auto vl = cbdb::PointerAndLenFromDatum(data, &len);

      column->Append(reinterpret_cast<char *>(vl), len);
    }
  }
  ASSERT_EQ(column->GetRows() - column->GetNonNullRows(), null_counts);
  ASSERT_EQ(column->GetNonNullRows(), 1000);

  columns->AddRows(column->GetRows());
  columns->Append(column);
  adapter->SetDataSource(columns);
  auto append_rc = adapter->AppendToVecBuffer();
  ASSERT_TRUE(append_rc);

  // already full
  append_rc = adapter->AppendToVecBuffer();
  ASSERT_FALSE(append_rc);

  size_t flush_counts = adapter->FlushVecBuffer(ctuple_slot);
  ASSERT_EQ(1000 + null_counts, flush_counts);

  // verify ctuple_slot 2
  {
    VecTupleTableSlot *vslot = nullptr;
    TupleTableSlot *tuple_table_slot = ctuple_slot->GetTupleTableSlot();
    vslot = (VecTupleTableSlot *)tuple_table_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ASSERT_NE(rb, nullptr);
    ArrowArray *arrow_array = &rb->batch;
    ASSERT_EQ(arrow_array->length, 1000 + null_counts);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, nullptr);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(child_array->length, 1000 + null_counts);
    ASSERT_EQ(child_array->null_count, null_counts);
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_EQ(child_array->private_data, child_array);

    if (is_fixed) {
      ASSERT_NE(child_array->buffers[0], nullptr);
      ASSERT_NE(child_array->buffers[1], nullptr);

      auto null_bits_array = (uint8 *)child_array->buffers[0];
      char *buffer = (char *)child_array->buffers[1];

      size_t verify_null_counts = 0;
      size_t start = 0;
      for (int64 i = 0; i < child_array->length; i++) {
        if (arrow::bit_util::GetBit(null_bits_array, i)) {
          ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))), start++);
        } else {
          verify_null_counts++;
        }
      }

      ASSERT_EQ(start, 1000);
      ASSERT_EQ(verify_null_counts, child_array->null_count);
    } else {
      ASSERT_NE(child_array->buffers[0], nullptr);
      ASSERT_NE(child_array->buffers[1], nullptr);
      ASSERT_NE(child_array->buffers[2], nullptr);

      // verify null bitmap
      auto null_bits_array = (uint8 *)child_array->buffers[0];

      size_t verify_null_counts = 0;
      for (int64 i = 0; i < child_array->length; i++) {
        if (!arrow::bit_util::GetBit(null_bits_array, i)) {
          verify_null_counts++;
        }
      }

      ASSERT_EQ(verify_null_counts, child_array->null_count);

      // verify data
      char *buffer = (char *)child_array->buffers[2];
      size_t start = 0;
      for (int64 i = 0; i < (child_array->length - child_array->null_count);
           i++) {
        ASSERT_EQ(*((int32 *)(buffer + i * sizeof(int32))), start++);
      }
      ASSERT_EQ(start, 1000);

      // verify offset with data
      char *offset_buffer = (char *)child_array->buffers[1];
      start = 0;

      verify_null_counts = 0;
      for (int64 i = 0; i < child_array->length; i++) {
        auto current_offset = *((int32 *)(offset_buffer + i * sizeof(int32)));
        auto next_offset =
            *((int32 *)(offset_buffer + (i + 1) * sizeof(int32)));
        if (current_offset != next_offset) {
          ASSERT_EQ(
              *((int32 *)(buffer + (i - verify_null_counts) * sizeof(int32))),
              start++);
        } else {
          verify_null_counts++;
        }
      }
      ASSERT_EQ(start, 1000);
      ASSERT_EQ(verify_null_counts, child_array->null_count);
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
  }

  DeleteCTupleSlot(ctuple_slot);

  delete columns;
  delete adapter;
}

TEST_P(PaxVecTest, PaxColumnAllNullToVec) {
  VecAdapter *adapter;
  PaxColumns *columns;
  PaxColumn *column;

  auto is_fixed = GetParam();
  auto ctuple_slot = CreateCtuple(is_fixed);

  adapter = new VecAdapter(ctuple_slot->GetTupleDesc());
  columns = new PaxColumns();
  if (is_fixed) {
    column = new PaxCommColumn<int32>(1000);
  } else {
    column = new PaxNonFixedColumn(1000);
  }

  for (size_t i = 0; i < 1000; i++) {
    column->AppendNull();
  }

  columns->AddRows(column->GetRows());
  columns->Append(column);
  adapter->SetDataSource(columns);
  auto append_rc = adapter->AppendToVecBuffer();
  ASSERT_TRUE(append_rc);

  // already full
  append_rc = adapter->AppendToVecBuffer();
  ASSERT_FALSE(append_rc);

  size_t flush_counts = adapter->FlushVecBuffer(ctuple_slot);
  ASSERT_EQ(1000, flush_counts);

  {
    VecTupleTableSlot *vslot = nullptr;
    TupleTableSlot *tuple_table_slot = ctuple_slot->GetTupleTableSlot();
    vslot = (VecTupleTableSlot *)tuple_table_slot;

    auto rb = (ArrowRecordBatch *)vslot->tts_recordbatch;
    ASSERT_NE(rb, nullptr);
    ArrowArray *arrow_array = &rb->batch;
    ASSERT_EQ(arrow_array->length, 1000);
    ASSERT_EQ(arrow_array->null_count, 0);
    ASSERT_EQ(arrow_array->offset, 0);
    ASSERT_EQ(arrow_array->n_buffers, 1);
    ASSERT_EQ(arrow_array->n_children, 1);
    ASSERT_NE(arrow_array->children, nullptr);
    ASSERT_EQ(arrow_array->buffers[0], nullptr);
    ASSERT_EQ(arrow_array->dictionary, nullptr);
    ASSERT_EQ(arrow_array->private_data, nullptr);

    ArrowArray *child_array = arrow_array->children[0];
    ASSERT_EQ(child_array->length, 1000);
    ASSERT_EQ(child_array->null_count, 1000);
    ASSERT_EQ(child_array->offset, 0);
    ASSERT_EQ(child_array->n_buffers, is_fixed ? 2 : 3);
    ASSERT_EQ(child_array->n_children, 0);
    ASSERT_EQ(child_array->children, nullptr);
    ASSERT_EQ(child_array->private_data, child_array);

    if (is_fixed) {
      ASSERT_NE(child_array->buffers[0], nullptr);
      ASSERT_NE(child_array->buffers[1], nullptr);

      auto null_bits_array = (uint8 *)child_array->buffers[0];

      // verify null bitmap
      for (size_t i = 0; i < 1000; i++) {
        ASSERT_FALSE(arrow::bit_util::GetBit(null_bits_array, i));
      }

    } else {
      ASSERT_NE(child_array->buffers[0], nullptr);
      ASSERT_NE(child_array->buffers[1], nullptr);
      ASSERT_NE(child_array->buffers[2], nullptr);

      auto null_bits_array = (uint8 *)child_array->buffers[0];

      // verify null bitmap
      for (size_t i = 0; i < 1000; i++) {
        ASSERT_FALSE(arrow::bit_util::GetBit(null_bits_array, i));
      }

      char *offset_buffer = (char *)child_array->buffers[1];
      for (size_t i = 0; i <= 1000; i++) {
        // all of offset is 0
        // no data in data part
        ASSERT_EQ(*((int32 *)(offset_buffer + i * sizeof(int32))), 0);
      }
    }

    ASSERT_EQ(child_array->dictionary, nullptr);
  }

  DeleteCTupleSlot(ctuple_slot);

  delete columns;
  delete adapter;
}

class MockTableWriter : public TableWriter {
 public:
  MockTableWriter(const Relation relation, WriteSummaryCallback callback)
      : TableWriter(relation) {
    SetWriteSummaryCallback(callback);
    SetFileSplitStrategy(new PaxDefaultSplitStrategy());
  }

  MOCK_METHOD(std::string, GenFilePath, (const std::string &), (override));
  MOCK_METHOD((std::vector<std::tuple<ColumnEncoding_Kind, int>>),
              GetRelEncodingOptions, (), (override));
};

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

TEST_P(PaxVecTest, PaxVecReaderTest) {
  auto is_fixed = GetParam();
  CTupleSlot *ctuple_slot = CreateCtuple(is_fixed, true);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;

  auto relation = (Relation)cbdb::Palloc0(sizeof(RelationData));
  relation->rd_att = ctuple_slot->GetTupleTableSlot()->tts_tupleDescriptor;
  bool callback_called = false;

  TableWriter::WriteSummaryCallback callback =
      [&callback_called](const WriteSummary & /*summary*/) {
        callback_called = true;
      };

  auto writer = new MockTableWriter(relation, callback);
  EXPECT_CALL(*writer, GenFilePath(_))
      .Times(AtLeast(1))
      .WillRepeatedly(Return(file_name_));
  encoding_opts.emplace_back(
      std::make_tuple(ColumnEncoding_Kind_NO_ENCODED, 0));
  EXPECT_CALL(*writer, GetRelEncodingOptions())
      .Times(AtLeast(1))
      .WillRepeatedly(Return(encoding_opts));

  writer->Open();

  for (size_t i = 0; i < VEC_BATCH_LENGTH + 1000; i++) {
    writer->WriteTuple(ctuple_slot);
  }

  writer->Close();
  ASSERT_TRUE(callback_called);

  DeleteCTupleSlot(ctuple_slot);
  delete writer;

  ctuple_slot = CreateCtuple(is_fixed);
  auto adapter = new VecAdapter(ctuple_slot->GetTupleDesc());

  std::vector<MicroPartitionMetadata> meta_info_list;
  MicroPartitionMetadata meta_info;

  meta_info.SetFileName(file_name_);
  meta_info.SetMicroPartitionId(file_name_);
  meta_info_list.push_back(std::move(meta_info));

  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> meta_info_iterator =
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>>(
          new MockReaderInterator(meta_info_list));

  TableReader *reader;
  TableReader::ReaderOptions reader_options{};
  reader_options.build_bitmap = false;
  reader_options.rel_oid = 0;
  reader_options.is_vec = true;
  reader_options.adapter = adapter;

  reader = new TableReader(std::move(meta_info_iterator), reader_options);
  reader->Open();

  bool ok = reader->ReadTuple(ctuple_slot);
  ASSERT_TRUE(ok);

  ok = reader->ReadTuple(ctuple_slot);
  ASSERT_TRUE(ok);
  ok = reader->ReadTuple(ctuple_slot);
  ASSERT_FALSE(ok);

  reader->Close();
  DeleteCTupleSlot(ctuple_slot);
  delete adapter;
  delete relation;
  delete reader;
}

INSTANTIATE_TEST_CASE_P(PaxVecTestCombine, PaxVecTest,
                        testing::Values(true, false));

#endif  // VEC_BUILD

}  // namespace pax::tests
