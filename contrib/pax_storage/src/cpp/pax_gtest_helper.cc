#include "pax_gtest_helper.h"

#include "storage/micro_partition.h"

namespace pax::tests {

void GenTextBuffer(char *buffer, size_t length) {
  for (size_t i = 0; i < length; i++) {
    buffer[i] = static_cast<char>(i);
  }
}

void CreateMemoryContext() {
  MemoryContext test_memory_context = AllocSetContextCreate(
      (MemoryContext)NULL, "TestMemoryContext", 80 * 1024 * 1024,
      80 * 1024 * 1024, 80 * 1024 * 1024);
  MemoryContextSwitchTo(test_memory_context);
}

void CreateTestResourceOwner() {
  CurrentResourceOwner = ResourceOwnerCreate(NULL, "TestResourceOwner");
}

void ReleaseTestResourceOwner() {
  ResourceOwner tmp_resource_owner = CurrentResourceOwner;
  CurrentResourceOwner = NULL;
  ResourceOwnerRelease(tmp_resource_owner, RESOURCE_RELEASE_BEFORE_LOCKS, false,
                       true);
  ResourceOwnerRelease(tmp_resource_owner, RESOURCE_RELEASE_LOCKS, false, true);
  ResourceOwnerRelease(tmp_resource_owner, RESOURCE_RELEASE_AFTER_LOCKS, false,
                       true);
  ResourceOwnerDelete(tmp_resource_owner);
}

static TupleDesc CreateTestTupleDesc() {
  auto tuple_desc = reinterpret_cast<TupleDescData *>(cbdb::Palloc0(
      sizeof(TupleDescData) + sizeof(FormData_pg_attribute) * COLUMN_NUMS));

  tuple_desc->natts = COLUMN_NUMS;
  tuple_desc->attrs[0] = {.atttypid = TEXTOID,
                          .attlen = -1,
                          .attbyval = false,
                          .attalign = TYPALIGN_DOUBLE,
                          .attisdropped = false,
                          .attcollation = DEFAULT_COLLATION_OID};

  tuple_desc->attrs[1] = {.atttypid = TEXTOID,
                          .attlen = -1,
                          .attbyval = false,
                          .attalign = TYPALIGN_DOUBLE,
                          .attisdropped = false,
                          .attcollation = DEFAULT_COLLATION_OID};

  tuple_desc->attrs[2] = {.atttypid = INT4OID,
                          .attlen = 4,
                          .attbyval = true,
                          .attalign = TYPALIGN_INT,
                          .attisdropped = false,
                          .attcollation = InvalidOid};
  return tuple_desc;
}

CTupleSlot *CreateTestCTupleSlot(bool with_value) {
  TupleTableSlot *tuple_slot = nullptr;
  TupleDesc tuple_desc = nullptr;

  tuple_desc = CreateTestTupleDesc();

  tuple_slot = MakeTupleTableSlot(tuple_desc, &TTSOpsVirtual);

  if (with_value) {
    char column_buff[COLUMN_SIZE * 2];
    GenTextBuffer(column_buff, COLUMN_SIZE);
    GenTextBuffer(column_buff + COLUMN_SIZE, COLUMN_SIZE);

    tuple_slot->tts_values[0] =
        cbdb::DatumFromCString(column_buff, COLUMN_SIZE);
    tuple_slot->tts_values[1] =
        cbdb::DatumFromCString(column_buff + COLUMN_SIZE, COLUMN_SIZE);
    tuple_slot->tts_values[2] = cbdb::Int32ToDatum(INT32_COLUMN_VALUE);
    tuple_slot->tts_isnull[0] = false;
    tuple_slot->tts_isnull[1] = false;
    tuple_slot->tts_isnull[2] = false;
  }

  return new CTupleSlot(tuple_slot);
}

static bool VerifyTestNonFixed(Datum datum, bool is_null) {
  struct varlena *vl, *tunpacked;
  int read_len;
  char *read_data;
  char column_buff[COLUMN_SIZE];

  GenTextBuffer(column_buff, COLUMN_SIZE);

  if (is_null) {
    return false;
  }

  vl = (struct varlena *)DatumGetPointer(datum);
  tunpacked = pg_detoast_datum_packed(vl);
  if ((Pointer)vl != (Pointer)tunpacked) {
    return false;
  }

  read_len = VARSIZE(tunpacked);
  read_data = VARDATA_ANY(tunpacked);

  if (read_len != COLUMN_SIZE + VARHDRSZ) {
    return false;
  }

  if (std::memcmp(read_data, column_buff, COLUMN_SIZE) != 0) {
    return false;
  }
  return true;
}

static bool VerifyTestFixed(Datum datum, bool is_null) {
  return !is_null && cbdb::DatumToInt32(datum) == INT32_COLUMN_VALUE;
}

bool VerifyTestCTupleSlot(CTupleSlot *ctuple_slot) {
  TupleTableSlot *tuple_slot = nullptr;
  bool ok = true;

  if (!ctuple_slot || !ctuple_slot->GetTupleTableSlot()) {
    return false;
  }

  tuple_slot = ctuple_slot->GetTupleTableSlot();

  ok &=
      VerifyTestNonFixed(tuple_slot->tts_values[0], tuple_slot->tts_isnull[0]);
  ok &=
      VerifyTestNonFixed(tuple_slot->tts_values[1], tuple_slot->tts_isnull[1]);
  ok &= VerifyTestFixed(tuple_slot->tts_values[2], tuple_slot->tts_isnull[2]);
  return ok;
}

bool VerifyTestCTupleSlot(CTupleSlot *ctuple_slot, int attrno) {
  TupleTableSlot *tuple_slot = nullptr;

  Assert(attrno <= 3 && attrno > 0);

  if (!ctuple_slot || !ctuple_slot->GetTupleTableSlot()) {
    return false;
  }

  tuple_slot = ctuple_slot->GetTupleTableSlot();
  if (attrno <= 2) {
    return VerifyTestNonFixed(tuple_slot->tts_values[attrno - 1],
                              tuple_slot->tts_isnull[attrno - 1]);
  } else {
    return VerifyTestFixed(tuple_slot->tts_values[attrno - 1],
                           tuple_slot->tts_isnull[attrno - 1]);
  }
}

void DeleteTestCTupleSlot(CTupleSlot *ctuple_slot) {
  auto tuple_table_slot = ctuple_slot->GetTupleTableSlot();
  cbdb::Pfree(tuple_table_slot->tts_tupleDescriptor);
  cbdb::Pfree(tuple_table_slot);
  delete ctuple_slot;
}

std::vector<orc::proto::Type_Kind> CreateTestSchemaTypes() {
  std::vector<orc::proto::Type_Kind> types;
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(orc::proto::Type_Kind::Type_Kind_INT);
  return types;
}

}  // namespace pax::tests