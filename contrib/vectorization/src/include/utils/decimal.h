/*--------------------------------------------------------------------
 * decimal.h
 *	  Arrow deciaml functions.

 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/utils/decimal.h
 *
 *--------------------------------------------------------------------
 */
#ifndef DECIMAL_H
#define DECIMAL_H

#include "utils/arrow.h"
#include "utils/builtins.h"
#include "utils/numeric.h"

#define DECIMAL128_SIZE (sizeof(int64_t) * 2)
#define NBASE		10000
#define DEC_DIGITS	4			/* decimal digits per NBASE digit */
#define SHORT_HEADER_SIZE 2

static const int64 digits_base[] = {1LL,10LL,100LL,1000LL,10000LL,100000LL,1000000LL,10000000LL,100000000LL,1000000000LL,10000000000LL,
100000000000LL,1000000000000LL,10000000000000LL,100000000000000LL,1000000000000000LL,10000000000000000LL,100000000000000000LL,
1000000000000000000LL};

static const uint16 decimal_scale_shift = 56;
static const uint64 decimal_highval_mask = (1LL << decimal_scale_shift) - 1;

static inline int64 high_bits_with_header(uint64_t scale, int64 high_val) {
  uint64 header = (scale << decimal_scale_shift);
	return header |  (decimal_highval_mask  & high_val);
}


static inline void numeric_special(Numeric num, 
									                    int64 *dec_out){

		if (NUMERIC_IS_PINF(num)){
      dec_out[0] = INT64_MAX;
      dec_out[1] = (1LL << 62) | ((1LL << (decimal_scale_shift - 1)) - 1);
      return;
    }
		else if (NUMERIC_IS_NINF(num)){
      dec_out[0] = INT64_MIN;
      dec_out[1] = (3LL << 62) | ((1LL << decimal_scale_shift) - 1);
      return;
    }
		else{
      dec_out[0] = INT64_MIN;
      dec_out[1] = (2LL << 62) | ((1LL << decimal_scale_shift) - 1);
      return;
    }
}

static inline void numeric_short_to_decimal(Numeric num, 
									                    int64 *dec_out,
                                      int alllen) {
  if(NUMERIC_IS_SPECIAL(num)){
    numeric_special(num, dec_out);
    return;
  }

  int scale = NUMERIC_DSCALE(num);
  int weight = NUMERIC_WEIGHT(num);
  NumericDigit *digits = NUMERIC_DIGITS(num);
  int ndigits = (alllen - SHORT_HEADER_SIZE) / sizeof(NumericDigit);
  int sign = NUMERIC_SIGN(num);
  bool neg = (sign == NUMERIC_NEG);
  int i = 0;
  int128 frac_val = 0;
  int128 val = 0;
  // integer part
  if (weight >= 0 && ndigits > 0) {
    val = digits[0];
    for (i = 1; i <= weight; i++) {
      val *= NBASE;
      if (i < ndigits){
        val += digits[i];
      }
    }
    for(int i = 0; i < scale; i++)
      val *= 10;
  }
  int frac_offset = weight >= 0 ? 0 :(abs(weight) - 1) * 4;
  // fractional part
  while (frac_offset < scale) {
    frac_val =  frac_val * NBASE;
    frac_offset += DEC_DIGITS;
    if(i < ndigits)
      frac_val += digits[i];
    // handle trailing zero
    if (frac_offset > scale) {
      frac_val = frac_val / digits_base[DEC_DIGITS - scale % DEC_DIGITS];
    }
    i++;
  }
  val += frac_val;
  val = neg ? -val : val;
  dec_out[0] = (uint64)val;
  dec_out[1] = high_bits_with_header(scale, val >> 64);
  return;
}

static inline Datum
Decimal256ArrayGetDatum(GArrowDecimal256Array *array, int32 i)
{
	g_autofree gchar *data;

	data = garrow_decimal256_array_format_value(array, i);
	return DirectFunctionCall3(numeric_in, CStringGetDatum(data), 0, -1);

}

static inline Datum
Decimal128ArrayGetDatum(GArrowDecimal128Array *array, int32 i)
{
	g_autofree gchar *data;

	data = garrow_decimal128_array_format_value(array, i);
	return DirectFunctionCall3(numeric_in, CStringGetDatum(data), 0, -1);

}

static inline Datum
Numeric128ArrayGetDatum(GArrowNumeric128Array *array, int32 i)
{
	g_autofree gchar *data;

	data = garrow_numeric128_array_format_value(array, i);
	return DirectFunctionCall3(numeric_in, CStringGetDatum(data), 0, -1);
}

#endif   /* DECIMAL_H */
