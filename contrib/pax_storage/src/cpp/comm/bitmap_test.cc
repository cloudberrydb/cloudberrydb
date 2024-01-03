#include <gtest/gtest.h>

#include "comm/bitmap.h"

namespace pax::tests {
class BitMapTest : public ::testing::Test {};

TEST_F(BitMapTest, Bitmap8) {
  Bitmap8 bm(20);

  ASSERT_TRUE(bm.Empty());
  for (auto i = 0; i <= 128; i++) {
    ASSERT_FALSE(bm.Test(i));  // zeros
    ASSERT_FALSE(bm.Toggle(i));
    ASSERT_TRUE(bm.Test(i));
    ASSERT_TRUE(bm.Toggle(i));
    ASSERT_FALSE(bm.Test(i));

    ASSERT_FALSE(bm.Test(i));  // zeros
    bm.Set(i);
    ASSERT_TRUE(bm.Test(i));
    bm.Set(i);
    ASSERT_TRUE(bm.Test(i));

    bm.Clear(i);
    ASSERT_FALSE(bm.Test(i));
    bm.Clear(i);
    ASSERT_FALSE(bm.Test(i));

    bm.Set(i);
    ASSERT_TRUE(bm.Test(i));
  }
}

TEST_F(BitMapTest, Bitmap8SetN) {
  Bitmap8 bm(10);
  const auto nbits = 128;

  ASSERT_TRUE(bm.Empty());
  for (auto i = 0; i <= nbits; i++) ASSERT_FALSE(bm.Test(i));

  auto fn = [&bm, nbits](uint32 index) {
    bm.ClearAll();
    for (auto i = 0; i <= nbits; i++) ASSERT_FALSE(bm.Test(i));
    bm.SetN(index);
    for (uint32 i = 0; i <= index; i++) ASSERT_TRUE(bm.Test(i));
    for (uint32 i = index + 1; i <= nbits; i++) ASSERT_FALSE(bm.Test(i));
  };
  for (uint32 i = 0; i <= nbits; i++) fn(i);
}

TEST_F(BitMapTest, Bitmap8ClearN) {
  Bitmap8 bm(10);
  const auto nbits = 128;

  ASSERT_TRUE(bm.Empty());
  for (auto i = 0; i <= nbits; i++) ASSERT_FALSE(bm.Test(i));

  auto fn = [&bm, nbits](uint32 index) {
    for (auto i = 0; i <= nbits; i++) {
      bm.Set(i);
      ASSERT_TRUE(bm.Test(i));
    }
    bm.ClearN(index);
    for (uint32 i = 0; i <= index; i++) ASSERT_FALSE(bm.Test(i));
    for (uint32 i = index + 1; i <= nbits; i++) ASSERT_TRUE(bm.Test(i));
  };
  for (uint32 i = 0; i <= nbits; i++) fn(i);
}

TEST_F(BitMapTest, Bitmap64) {
  Bitmap64 bm(100);

  ASSERT_TRUE(bm.Empty());
  for (auto i = 0; i <= 128; i++) {
    ASSERT_FALSE(bm.Test(i));  // zeros
    ASSERT_FALSE(bm.Toggle(i));
    ASSERT_TRUE(bm.Test(i));
    ASSERT_TRUE(bm.Toggle(i));
    ASSERT_FALSE(bm.Test(i));

    bm.Set(i);
    ASSERT_TRUE(bm.Test(i));
    bm.Set(i);
    ASSERT_TRUE(bm.Test(i));

    bm.Clear(i);
    ASSERT_FALSE(bm.Test(i));
    bm.Clear(i);
    ASSERT_FALSE(bm.Test(i));

    bm.Set(i);
    ASSERT_TRUE(bm.Test(i));
  }
}
TEST_F(BitMapTest, Bitmap64SetN) {
  Bitmap64 bm(1);
  const auto nbits = 512;

  ASSERT_TRUE(bm.Empty());
  for (auto i = 0; i <= nbits; i++) ASSERT_FALSE(bm.Test(i));

  auto fn = [&bm, nbits](uint32 index) {
    bm.ClearAll();
    for (auto i = 0; i <= nbits; i++) ASSERT_FALSE(bm.Test(i));
    bm.SetN(index);
    for (uint32 i = 0; i <= index; i++) ASSERT_TRUE(bm.Test(i));
    for (uint32 i = index + 1; i <= nbits; i++) ASSERT_FALSE(bm.Test(i));
  };
  for (uint32 i = 0; i <= nbits; i++) fn(i);
}

TEST_F(BitMapTest, Bitmap64ClearN) {
  Bitmap64 bm(1);
  const auto nbits = 512;

  ASSERT_TRUE(bm.Empty());
  for (auto i = 0; i <= nbits; i++) ASSERT_FALSE(bm.Test(i));

  auto fn = [&bm, &nbits](uint32 index) {
    for (auto i = 0; i <= nbits; i++) {
      bm.Set(i);
      ASSERT_TRUE(bm.Test(i));
    }
    bm.ClearN(index);
    for (uint32 i = 0; i <= index; i++) ASSERT_FALSE(bm.Test(i));
    for (uint32 i = index + 1; i <= nbits; i++) ASSERT_TRUE(bm.Test(i));
  };
  for (uint32 i = 0; i <= nbits; i++) fn(i);
}

TEST_F(BitMapTest, CountBits) {
  const uint32 starts[] = {0, 1, 3, 7};
  const uint32 ends[] = {0, 1, 7, 8, 9, 15, 16, 17};
  Bitmap8 bm(11);

  auto fill_bits = [&bm](uint32 bits) {
    uint32 k = 0;
    bm.ClearAll();
    while (bits) {
      if (bits & 1) bm.Set(k);
      bits = bits >> 1;
      k++;
    }
  };
  auto plain_count = [](uint32 bits, uint32 start, uint32 end) {
    size_t nbits = 0;
    for (auto i = start; i <= end; i++) {
      if (bits & (1ULL << i)) nbits++;
    }
    return nbits;
  };

  for (uint32 i = 0; i < 0x3ffff; i++) {
    fill_bits(i);
    for (auto start : starts) {
      for (auto end : ends) {
        if (end < start) continue;
        ASSERT_EQ(bm.CountBits(start, end), plain_count(i, start, end));
        ASSERT_EQ(bm.CountBits(end), plain_count(i, 0, end));
      }
    }
  }
}

}  // namespace pax::tests
