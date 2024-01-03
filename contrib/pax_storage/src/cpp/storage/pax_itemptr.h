#pragma once
#include "comm/cbdb_api.h"
#include "comm/cbdb_wrappers.h"
#include <string>

#ifdef ENABLE_LOCAL_INDEX
namespace pax {
#define PAX_BLOCK_BIT_SIZE 24
#define PAX_TUPLE_BIT_SIZE (48 - (PAX_BLOCK_BIT_SIZE + 1))

static inline ItemPointerData MakeCTID(uint32 block_number,
                                       uint32 tuple_offset) {
  ItemPointerData ctid;

  static_assert(16 < PAX_BLOCK_BIT_SIZE && PAX_BLOCK_BIT_SIZE < 32,
                "bit number of block number < 32");
  static_assert(16 < PAX_TUPLE_BIT_SIZE && PAX_TUPLE_BIT_SIZE < 32,
                "bit number of tuple number < 32");

  Assert(block_number < (1UL << PAX_BLOCK_BIT_SIZE));
  Assert(tuple_offset < (1UL << PAX_TUPLE_BIT_SIZE));

  ctid.ip_blkid.bi_hi = block_number >> (PAX_BLOCK_BIT_SIZE - 16);
  ctid.ip_blkid.bi_lo = block_number << (32 - PAX_BLOCK_BIT_SIZE);
  ctid.ip_blkid.bi_lo |= tuple_offset >> 15;
  ctid.ip_posid = (tuple_offset & 0x7FFF) + 1;
  return ctid;
}

static inline uint32 GetBlockNumber(ItemPointerData ctid) {
  uint32 block_number = ((uint32)ctid.ip_blkid.bi_hi)
                        << (PAX_BLOCK_BIT_SIZE - 16);
  return block_number |
         ((uint32)ctid.ip_blkid.bi_lo) >> (32 - PAX_BLOCK_BIT_SIZE);
}

static inline void SetBlockNumber(ItemPointer ctid, uint32 block_number) {
  Assert(block_number < (1UL << PAX_BLOCK_BIT_SIZE));

  uint32 mask = (1UL << (32 - PAX_BLOCK_BIT_SIZE)) - 1;
  ctid->ip_blkid.bi_hi = block_number >> (PAX_BLOCK_BIT_SIZE - 16);
  ctid->ip_blkid.bi_lo = (ctid->ip_blkid.bi_lo & mask) |
                         (block_number << (32 - PAX_BLOCK_BIT_SIZE));
}

static inline std::string MapToBlockNumber(Relation /* rel */,
                                           ItemPointerData ctid) {
  auto block_number = pax::GetBlockNumber(ctid);
  return std::to_string(block_number);
}

}  // namespace pax

#else // #ifdef ENABLE_LOCAL_INDEX

namespace pax {
#define PAX_TABLE_NUM_BIT_SIZE 5
#define PAX_BLOCK_BIT_SIZE 22
#define PAX_TUPLE_BIT_SIZE \
  (48 - (PAX_TABLE_NUM_BIT_SIZE + PAX_BLOCK_BIT_SIZE + 1))

#define MAX_TABLE_NUM_IN_CTID ((1 << PAX_TABLE_NUM_BIT_SIZE) - 1)
static inline ItemPointerData MakeCTID(uint8 table_no, uint32 block_number,
                                       uint32 tuple_offset) {
  ItemPointerData ctid;

  static_assert(PAX_TABLE_NUM_BIT_SIZE < 16, "bit number of table numer < 16");
  static_assert(16 < PAX_BLOCK_BIT_SIZE && PAX_BLOCK_BIT_SIZE < 32,
                "bit number of block number < 32");
  static_assert(PAX_TABLE_NUM_BIT_SIZE + PAX_BLOCK_BIT_SIZE < 32,
                "bits(table_no) + bits(block) < 32");
  static_assert(16 < PAX_TUPLE_BIT_SIZE && PAX_TUPLE_BIT_SIZE < 32,
                "bits(tuple_offset) < 32");

  Assert(table_no < (1UL << PAX_TABLE_NUM_BIT_SIZE));
  Assert(block_number < (1UL << PAX_BLOCK_BIT_SIZE));
  Assert(tuple_offset < (1UL << PAX_TUPLE_BIT_SIZE));

  ctid.ip_blkid.bi_hi = table_no << (16 - PAX_TABLE_NUM_BIT_SIZE);
  ctid.ip_blkid.bi_hi |=
      block_number >> (PAX_TABLE_NUM_BIT_SIZE + PAX_BLOCK_BIT_SIZE - 16);
  ctid.ip_blkid.bi_lo = (block_number)
                        << (32 - (PAX_TABLE_NUM_BIT_SIZE + PAX_BLOCK_BIT_SIZE));
  ctid.ip_blkid.bi_lo |= tuple_offset >> 15;

  ctid.ip_posid = (tuple_offset & 0x7FFF) + 1;

  return ctid;
}

static inline uint8 GetTableNo(ItemPointerData ctid) {
  return ctid.ip_blkid.bi_hi >> (16 - PAX_TABLE_NUM_BIT_SIZE);
}

static inline void SetTableNo(ItemPointer ctid, uint8 table_no) {
  uint32 shift = 16 - PAX_TABLE_NUM_BIT_SIZE;
  uint32 mask = (1UL << shift) - 1;
  uint16 number = table_no;
  ctid->ip_blkid.bi_hi = (ctid->ip_blkid.bi_hi & mask) | (number << shift);
}

static inline uint32 GetBlockNumber(ItemPointerData ctid) {
  uint32 mask1 = (1UL << (16 - PAX_TABLE_NUM_BIT_SIZE)) - 1;
  uint32 shift1 = PAX_TABLE_NUM_BIT_SIZE + PAX_BLOCK_BIT_SIZE - 16;
  uint32 shift2 = (32 - (PAX_TABLE_NUM_BIT_SIZE + PAX_BLOCK_BIT_SIZE));
  uint32 block_number = (ctid.ip_blkid.bi_hi & mask1) << shift1;
  return block_number | (ctid.ip_blkid.bi_lo >> shift2);
}

static inline void SetBlockNumber(ItemPointer ctid, uint32 block_number) {
  Assert(block_number < (1UL << PAX_BLOCK_BIT_SIZE));

  uint32 mask1 = (1UL << (16 - PAX_TABLE_NUM_BIT_SIZE)) - 1;
  uint32 shift1 = PAX_TABLE_NUM_BIT_SIZE + PAX_BLOCK_BIT_SIZE - 16;
  uint32 shift2 = (32 - (PAX_TABLE_NUM_BIT_SIZE + PAX_BLOCK_BIT_SIZE));
  uint32 mask2 = (1UL << shift2) - 1;
  ctid->ip_blkid.bi_hi =
      (ctid->ip_blkid.bi_hi & ~mask1) | (block_number >> shift1);
  ctid->ip_blkid.bi_lo =
      (ctid->ip_blkid.bi_lo & mask2) | (block_number << shift2);
}

class BlockNumberManager {
 public:
  void InitOpen(Oid relid);
  uint32 GetBlockNumber(Oid relid, const std::string &blockid) const;
  uint8 GetTableNo() const { return table_no_; }

 private:
  uint8 table_no_ = 0;
  uint32 table_index_ = 0;
};

extern std::string MapToBlockNumber(Relation rel, ItemPointerData ctid);
}  // namespace pax

#endif // #ifdef ENABLE_LOCAL_INDEX

namespace pax {
static inline uint32 GetTupleOffsetInternal(ItemPointerData ctid) {
  uint32 mask = (1UL << (PAX_TUPLE_BIT_SIZE - 15)) - 1;
  uint32 hi = ctid.ip_blkid.bi_lo & mask;
  uint32 lo = ctid.ip_posid - 1;
  return (hi << 15) | lo;
}

static inline uint32 GetTupleOffset(ItemPointerData ctid) {
  Assert(ItemPointerIsValid(&ctid));
  return GetTupleOffsetInternal(ctid);
}

static inline void SetTupleOffset(ItemPointer ctid, uint32 offset) {
  Assert(offset < (1UL << PAX_TUPLE_BIT_SIZE));

  uint32 mask = (1UL << (PAX_TUPLE_BIT_SIZE - 15)) - 1;
  ctid->ip_blkid.bi_lo = (ctid->ip_blkid.bi_lo & ~mask) | (offset >> 15);
  ctid->ip_posid = (offset & 0x7FFF) + 1;
}

static inline uint64 CTIDToUint64(ItemPointerData ctid) {
  uint64 ctid_u64 = 0;
  ctid_u64 |= ((uint64)ctid.ip_blkid.bi_hi) << 48;
  ctid_u64 |= ((uint64)ctid.ip_blkid.bi_lo) << 32;
  ctid_u64 |= ctid.ip_posid;

  return ctid_u64;
}

extern std::string GenerateBlockID(Relation relation);

}  // namespace pax
