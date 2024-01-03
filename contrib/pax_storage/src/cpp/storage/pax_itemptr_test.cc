#include <gtest/gtest.h>

#include "storage/pax_itemptr.h"

#include "comm/cbdb_api.h"

namespace pax::tests {
class PaxItemPtrTest : public ::testing::Test {
 public:
  void SetUp() override {}

  void TearDown() override {}
};

#define MAX_BIT_NUMBER(nbits) ((1ULL << (nbits)) - 1)

#ifdef ENABLE_LOCAL_INDEX

TEST_F(PaxItemPtrTest, ItemPointerLocalIndexBlockNumber) {
  uint32 block;
  uint32 tuple_offsets[] = {0, 1, MAX_BIT_NUMBER(PAX_TUPLE_BIT_SIZE)};
  for (auto tuple_offset : tuple_offsets) {
    for (block = 0; block <= 0xFFFF; block++) {
      auto ctid = pax::MakeCTID(block, tuple_offset);
      EXPECT_EQ(tuple_offset, pax::GetTupleOffset(ctid));
      EXPECT_EQ(block, pax::GetBlockNumber(ctid));

      SetBlockNumber(&ctid, block);
      EXPECT_EQ(block, pax::GetBlockNumber(ctid));
    }
    for (block = MAX_BIT_NUMBER(PAX_BLOCK_BIT_SIZE) - 0xFFFF;
         block <= MAX_BIT_NUMBER(PAX_BLOCK_BIT_SIZE); block++) {
      auto ctid = pax::MakeCTID(block, tuple_offset);
      EXPECT_EQ(tuple_offset, pax::GetTupleOffset(ctid));
      EXPECT_EQ(block, pax::GetBlockNumber(ctid));

      SetBlockNumber(&ctid, block);
      EXPECT_EQ(block, pax::GetBlockNumber(ctid));
    }
  }
}

TEST_F(PaxItemPtrTest, ItemPointerLocalIndexTupleNumber) {
  uint32 blocks[] = {0, 1, 0xff, 0xfff, MAX_BIT_NUMBER(PAX_BLOCK_BIT_SIZE)};
  uint32 tuple_offset;
  for (auto block : blocks) {
    for (tuple_offset = 0; tuple_offset <= 0xFFFF; tuple_offset++) {
      auto ctid = pax::MakeCTID(block, tuple_offset);
      EXPECT_EQ(block, pax::GetBlockNumber(ctid));
      EXPECT_EQ(tuple_offset, pax::GetTupleOffset(ctid));

      SetTupleOffset(&ctid, tuple_offset);
      EXPECT_EQ(tuple_offset, pax::GetTupleOffset(ctid));
    }
    for (tuple_offset = MAX_BIT_NUMBER(PAX_TUPLE_BIT_SIZE) - 0xFFFF;
         tuple_offset <= MAX_BIT_NUMBER(PAX_TUPLE_BIT_SIZE); tuple_offset++) {
      auto ctid = pax::MakeCTID(block, tuple_offset);
      EXPECT_EQ(block, pax::GetBlockNumber(ctid));
      EXPECT_EQ(tuple_offset, pax::GetTupleOffset(ctid));

      SetTupleOffset(&ctid, tuple_offset);
      EXPECT_EQ(tuple_offset, pax::GetTupleOffset(ctid));
    }
  }
}

#else

TEST_F(PaxItemPtrTest, ItemPointerGenericTableNo) {
  uint8 table_no;
  uint32 blocks[] = {0, 1, 0xff, 0xfff, MAX_BIT_NUMBER(PAX_BLOCK_BIT_SIZE)};
  uint32 tuple_offset = 0xFF;
  for (auto block : blocks) {
    for (table_no = 0; table_no <= (uint8)MAX_TABLE_NUM_IN_CTID; table_no++) {
      auto ctid = pax::MakeCTID(table_no, block, tuple_offset);
      EXPECT_EQ(table_no, pax::GetTableNo(ctid));
      EXPECT_EQ(block, pax::GetBlockNumber(ctid));
    }
  }
}

TEST_F(PaxItemPtrTest, ItemPointerGenericBlockNumber) {
  uint8 table_nos[] = {0, 1, 0xf, MAX_TABLE_NUM_IN_CTID};
  uint32 block;
  uint32 tuple_offsets[] = {0, 1, MAX_BIT_NUMBER(PAX_TUPLE_BIT_SIZE)};
  for (auto table_no : table_nos) {
    for (auto tuple_offset : tuple_offsets) {
      for (block = 0; block <= 0xFFFF; block++) {
        auto ctid = pax::MakeCTID(table_no, block, tuple_offset);
        EXPECT_EQ(table_no, pax::GetTableNo(ctid));
        EXPECT_EQ(block, pax::GetBlockNumber(ctid));

        SetBlockNumber(&ctid, block);
        EXPECT_EQ(block, pax::GetBlockNumber(ctid));
      }
      for (block = MAX_BIT_NUMBER(PAX_BLOCK_BIT_SIZE) - 0xFFFF;
           block <= MAX_BIT_NUMBER(PAX_BLOCK_BIT_SIZE); block++) {
        auto ctid = pax::MakeCTID(table_no, block, tuple_offset);
        EXPECT_EQ(table_no, pax::GetTableNo(ctid));
        EXPECT_EQ(block, pax::GetBlockNumber(ctid));

        SetBlockNumber(&ctid, block);
        EXPECT_EQ(block, pax::GetBlockNumber(ctid));
      }
    }
  }
}

TEST_F(PaxItemPtrTest, ItemPointerGenericTupleNumber) {
  uint8 table_no = 0;
  uint32 blocks[] = {0, 1, 0xff, 0xfff, MAX_BIT_NUMBER(PAX_BLOCK_BIT_SIZE)};
  uint32 tuple_offset;
  for (auto block : blocks) {
    for (tuple_offset = 0; tuple_offset <= 0xFFFF; tuple_offset++) {
      auto ctid = pax::MakeCTID(table_no, block, tuple_offset);
      EXPECT_EQ(block, pax::GetBlockNumber(ctid));
      EXPECT_EQ(tuple_offset, pax::GetTupleOffset(ctid));

      SetTupleOffset(&ctid, tuple_offset);
      EXPECT_EQ(tuple_offset, pax::GetTupleOffset(ctid));
    }
    for (tuple_offset = MAX_BIT_NUMBER(PAX_TUPLE_BIT_SIZE) - 0xFFFF;
         tuple_offset <= MAX_BIT_NUMBER(PAX_TUPLE_BIT_SIZE); tuple_offset++) {
      auto ctid = pax::MakeCTID(table_no, block, tuple_offset);
      EXPECT_EQ(block, pax::GetBlockNumber(ctid));
      EXPECT_EQ(tuple_offset, pax::GetTupleOffset(ctid));

      SetTupleOffset(&ctid, tuple_offset);
      EXPECT_EQ(tuple_offset, pax::GetTupleOffset(ctid));
    }
  }
}
#endif
}  // namespace pax::tests
