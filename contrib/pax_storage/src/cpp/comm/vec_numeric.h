#pragma once
#include "comm/cbdb_api.h"

#include <utility>

// The vec numeric defined
#define VEC_SHORT_NUMERIC_STORE_BYTES 16
#define VEC_SHORT_NUMERIC_SCALE_WIDTH 6
#define VEC_SHORT_NUMERIC_SPECIAL_TAG_WIDTH 2
#define VEC_SHORT_NUMERIC_HIGH_WIDTH 64
#define VEC_SHORT_NUMERIC_HEADER_WIDTH \
  (VEC_SHORT_NUMERIC_SCALE_WIDTH + VEC_SHORT_NUMERIC_SPECIAL_TAG_WIDTH)
#define VEC_SHORT_NUMERIC_HIGH_DATA_WIDTH \
  (VEC_SHORT_NUMERIC_HIGH_WIDTH - VEC_SHORT_NUMERIC_HEADER_WIDTH)

#define VEC_SHORT_NUMERIC_LOW_WIDTH 64
#define VEC_SHORT_NUMERIC_WIDTH \
  (VEC_SHORT_NUMERIC_HIGH_WIDTH + VEC_SHORT_NUMERIC_LOW_WIDTH)

// 1 < (2 ^ 119) / 10^(35) ≈ 6.64613997892 < 10
#define VEC_SHORT_NUMERIC_MAX_PRECISION 35

// The maximum in pg short head is 38, but the max precision in vec
// numeric format is 35.
#define VEC_SHORT_NUMERIC_MAX_SCALE VEC_SHORT_NUMERIC_MAX_PRECISION

extern void pg_short_numeric_to_vec_short_numeric(Numeric num, int num_len,
                                                  int64 *n_low_64,
                                                  int64 *n_high_64);
extern Datum vec_short_numeric_to_datum(const int64 *n_high,
                                        const int64 *n_low);