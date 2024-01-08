
#include "comm/cbdb_wrappers.h"
#include "comm/gtest_wrappers.h"

namespace pax::tests {
class CommTest : public ::testing::Test {
 public:
  void SetUp() override {
    MemoryContext comm_test_memory_context = AllocSetContextCreate(
        (MemoryContext)NULL, "CommTestMemoryContext", 1 * 1024 * 1024,
        1 * 1024 * 1024, 1 * 1024 * 1024);
    MemoryContextSwitchTo(comm_test_memory_context);
  }
};

TEST_F(CommTest, TestDeleteOperator) {
  // In standard C++, we allow the delete null operation
  // In Pax, we overloaded the delete operator which used
  // `pfree` to free the memory. `pfree` not allow pass NULL.
  // But we still need to conform the overloaded delete operation
  // to the semantics of c++ which achieve a complete semantic replacement.
  auto obj = new int32();
  delete obj;
  obj = nullptr;
  delete obj;

  auto array_obj = new int32[10];
  delete[] array_obj;
  array_obj = nullptr;
  delete[] array_obj;
}


TEST_F(CommTest, TestNewOperator) {
  auto obj = new bool[0];
  ASSERT_NE(obj, nullptr);
  delete[] obj;

  auto obj2 = cbdb::Palloc(0);
  ASSERT_NE(obj2, nullptr);
  cbdb::Pfree(obj2);

  auto obj3 = cbdb::Palloc0(0);
  ASSERT_NE(obj3, nullptr);
  cbdb::Pfree(obj3);
}

}  // namespace pax::tests