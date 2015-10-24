/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_ErrorData_h
#define __pljava_ErrorData_h

#include "pljava/type/JavaWrapper.h"
#ifdef __cplusplus
extern "C" {
#endif

/*****************************************************************
 * The ErrorData java class represents the native ErrorData.
 * 
 * @author Thomas Hallgren
 *****************************************************************/

/*
 * Create the org.postgresql.pljava.internal.ErrorData that represents
 * the current error obtaind from CopyErrorData().
 */
extern jobject ErrorData_getCurrentError(void);

/*
 * Extract the native ErrorData from a Java ErrorData.
 */
extern ErrorData* ErrorData_getErrorData(jobject jerrorData);


#ifdef __cplusplus
}
#endif
#endif
