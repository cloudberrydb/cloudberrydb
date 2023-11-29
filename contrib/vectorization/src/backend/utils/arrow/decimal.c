/*-------------------------------------------------------------------------
 *
 * decimal.c
 *
 *	  Arrow decimal functions.
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/arrow/decimal.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/decimal.h"

Datum
Decimal256ToDatum(GArrowDecimal256 *decimal, int32 scale)
{
	g_autofree gchar *str;

	str = garrow_decimal256_to_string_scale(decimal, scale);
	return DirectFunctionCall3(numeric_in, CStringGetDatum(str), 0, -1);
}

Datum
Decimal128ToDatum(GArrowDecimal128 *decimal, int scale)
{
	g_autofree gchar *str;

	str = garrow_decimal128_to_string(decimal);
	return DirectFunctionCall3(numeric_in, CStringGetDatum(str), 0, -1);
}
