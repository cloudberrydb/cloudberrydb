#pragma once
#include <vector>

#include "storage/proto/proto_wrappers.h"

namespace pax {
class CTupleSlot;
namespace tests {

// 3 clomun - string(len 100), string(len 100), int(len 4)
#define COLUMN_NUMS 3
#define COLUMN_SIZE 100
#define INT32_COLUMN_VALUE 0x123
#define INT32_COLUMN_VALUE_DEFAULT 0x001

extern void CreateMemoryContext();
extern void CreateTestResourceOwner();
extern void ReleaseTestResourceOwner();
extern CTupleSlot *CreateTestCTupleSlot(bool with_value = true);
extern bool VerifyTestCTupleSlot(CTupleSlot *ctuple_slot);
extern bool VerifyTestCTupleSlot(CTupleSlot *ctuple_slot, int attrno);
extern void DeleteTestCTupleSlot(CTupleSlot *ctuple_slot);

extern void GenTextBuffer(char *buffer, size_t length);
extern std::vector<orc::proto::Type_Kind> CreateTestSchemaTypes();
}  // namespace tests
}  // namespace pax