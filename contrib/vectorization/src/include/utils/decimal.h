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

Datum Decimal256ToDatum(GArrowDecimal256 *decimal, int32 scale);
Datum Decimal128ToDatum(GArrowDecimal128 *decimal, int32 scale);

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
Int128ArrayGetDatum(GArrowInt128Array *array, int32 i)
{
	g_autofree gchar *data;

	data = garrow_int128_array_format_value(array, i);
	return DirectFunctionCall1(int128_in, CStringGetDatum(data));
}

#endif   /* DECIMAL_H */
