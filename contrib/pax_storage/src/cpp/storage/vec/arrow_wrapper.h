#pragma once

#ifdef VEC_BUILD

// FIXME(jiaqizho): There marco defined in datatime.h
// which include in `cbdb_api.h`. In pax, we always need
// include `cbdb_api.h`.

#undef RESERV
#undef MONTH
#undef YEAR
#undef DAY
#undef JULIAN
#undef TZ
#undef DTZ
#undef DYNTZ
#undef IGNORE_DTF
#undef AMPM
#undef HOUR
#undef MINUTE
#undef SECOND
#undef MILLISECOND
#undef MICROSECOND
#undef IsPowerOf2

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-but-set-variable"

#include <arrow/array/array_decimal.h>
#include <arrow/array/data.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/util/bit_util.h>

#pragma GCC diagnostic pop

#define RESERV 0
#define MONTH 1
#define YEAR 2
#define DAY 3
#define JULIAN 4
#define TZ 5
#define DTZ 6
#define DYNTZ 7
#define IGNORE_DTF 8
#define AMPM 9
#define HOUR 10
#define MINUTE 11
#define SECOND 12
#define MILLISECOND 13
#define MICROSECOND 14

// NOLINTNEXTLINE
#define IsPowerOf2(x) (x > 0 && ((x) & ((x)-1)) == 0)

#endif  // VEC_BUILD
