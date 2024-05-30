#pragma once
#include "comm/cbdb_api.h"

#include <vector>

#include "storage/proto/proto_wrappers.h"

namespace pax::tests {

// 3 clomun - string(len 100), string(len 100), int(len 4)
#define COLUMN_NUMS 3
#define COLUMN_SIZE 100
#define INT32_COLUMN_VALUE 0x123
#define INT32_COLUMN_VALUE_DEFAULT 0x001

#define TOAST_COLUMN_NUMS 4
#define NO_TOAST_COLUMN_SIZE 14
#define COMPRESS_TOAST_COLUMN_SIZE 516
#define EXTERNAL_TOAST_COLUMN_SIZE 772
#define EXTERNAL_COMPRESS_TOAST_COLUMN_SIZE 1028

extern void CreateMemoryContext();
extern void CreateTestResourceOwner();
extern void ReleaseTestResourceOwner();
extern TupleTableSlot *CreateTestTupleTableSlot(bool with_value = true);
extern TupleTableSlot *CreateTestToastTupleTableSlot();
#ifdef VEC_BUILD
extern TupleTableSlot *CreateVecEmptyTupleSlot(TupleDesc tuple_desc);
#endif
extern bool VerifyTestTupleTableSlot(TupleTableSlot *tuple_slot);
extern bool VerifyTestTupleTableSlot(TupleTableSlot *tuple_slot, int attrno);
extern void DeleteTestTupleTableSlot(TupleTableSlot *tuple_slot);

extern void GenTextBuffer(char *buffer, size_t length);
extern std::vector<pax::porc::proto::Type_Kind> CreateTestSchemaTypes();
}  // namespace pax::tests
