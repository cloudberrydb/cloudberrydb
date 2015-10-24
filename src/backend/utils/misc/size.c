/*
 * size.c
 *    routines for manipulating Size data type.
 *
 * Copyright (c) 2014 Pivotal Inc.
 */

#include "postgres.h"
#include "utils/size.h"

/*
 * Add two Size values, checking for overflow
 */
Size
add_size(Size s1, Size s2)
{
	Size		result;

	result = s1 + s2;
	/* We are assuming Size is an unsigned type here... */
	if (result < s1 || result < s2)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("requested shared memory size overflows size_t")));
	return result;
}

/*
 * Multiply two Size values, checking for overflow
 */
Size
mul_size(Size s1, Size s2)
{
	Size		result;

	if (s1 == 0 || s2 == 0)
		return 0;
	result = s1 * s2;
	/* We are assuming Size is an unsigned type here... */
	if (result / s2 != s1)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("requested shared memory size overflows size_t")));
	return result;
}
