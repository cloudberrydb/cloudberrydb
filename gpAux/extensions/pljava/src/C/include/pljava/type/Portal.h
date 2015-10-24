/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_Portal_h
#define __pljava_Portal_h

#include "pljava/type/Type.h"

#ifdef __cplusplus
extern "C" {
#endif

#include <utils/portal.h>

/*****************************************************************
 * The Portal java class extends the NativeStruct and provides JNI
 * access to some of the attributes of the Portal structure.
 * 
 * @author Thomas Hallgren
 *****************************************************************/

/*
 * Create the org.postgresql.pljava.Portal instance
 */
extern jobject Portal_create(Portal portal);

#ifdef __cplusplus
}
#endif
#endif
