/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_type_Time_h
#define __pljava_type_Time_h

#include "pljava/type/Type.h"

#ifdef __cplusplus
extern "C" {
#endif

/*****************************************************************
 * The Time java class represents the native Time or TimeTZ.
 * 
 * @author Thomas Hallgren
 *****************************************************************/
 
#include <utils/timestamp.h>

/**
 * The PostgreSQL TimeTzADT when configured with --enable-integer-datetimes
 */
typedef struct
{
	int64		time;			/* all time units other than months and
								 * years */
	int32		zone;			/* numeric time zone, in seconds */
} TimeTzADT_id;

/**
 * The PostgreSQL TimeTzADT when configured without --enable-integer-datetimes
 */
typedef struct
{
	double		time;			/* all time units other than months and
								 * years */
	int32		zone;			/* numeric time zone, in seconds */
} TimeTzADT_dd;

#ifdef __cplusplus
}
#endif
#endif
