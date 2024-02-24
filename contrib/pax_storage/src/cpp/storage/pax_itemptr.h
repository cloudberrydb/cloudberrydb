#pragma once
#include "comm/cbdb_api.h"

#include <string>

#include "comm/cbdb_wrappers.h"

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

#ifndef BUILD_PAX_FORMAT
static inline void SetTupleOffset(ItemPointer ctid, uint32 offset) {
  Assert(offset < (1UL << PAX_TUPLE_BIT_SIZE));

  uint32 mask = (1UL << (PAX_TUPLE_BIT_SIZE - 15)) - 1;
  ctid->ip_blkid.bi_lo = (ctid->ip_blkid.bi_lo & ~mask) | (offset >> 15);
  ctid->ip_posid = (offset & 0x7FFF) + 1;
}
#else
// if paxformat.so is compiled separately, it is advisable not to set the tuple offset when reading tuples, 
// other access methods (AMs) may use the pax storage format with a different partitioning scheme for ctid, 
// which can lead to tuple offset overflow.
static inline void SetTupleOffset(ItemPointer ctid, uint32 offset) {}
#endif

static inline uint64 CTIDToUint64(ItemPointerData ctid) {
  uint64 ctid_u64 = 0;
  ctid_u64 |= ((uint64)ctid.ip_blkid.bi_hi) << 48;
  ctid_u64 |= ((uint64)ctid.ip_blkid.bi_lo) << 32;
  ctid_u64 |= ctid.ip_posid;

  return ctid_u64;
}

extern std::string GenerateBlockID(Relation relation);

}  // namespace pax
