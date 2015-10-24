/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_type_Timestamp_h
#define __pljava_type_Timestamp_h

#include "pljava/type/Type.h"

#ifdef __cplusplus
extern "C" {
#endif

/*****************************************************************
 * The Timestamp java class represents the native Timestamp.
 * 
 * @author Thomas Hallgren
 *****************************************************************/
 
/*
 * Returns the current timezone.
 */
extern int Timestamp_getCurrentTimeZone(void);

/*
 * Returns the timezone fo the given Timestamp. Comes in two variants.
 * The int64 variant will be used when PL/Java is used with a backend
 * compiled with integer datetimes. The double variant will be used when
 * this is not the case.
 */
extern int32 Timestamp_getTimeZone_id(int64 t);
extern int32 Timestamp_getTimeZone_dd(double t);

#ifdef __cplusplus
}
#endif
#endif
