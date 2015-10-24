/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_JavaWrapper_h
#define __pljava_JavaWrapper_h

#include "pljava/type/Type.h"

#ifdef __cplusplus
extern "C" {
#endif

/**************************************************************************
 * The JavaWrapper is a Java class that maintains a pointer to a
 * piece of memory allocated in the special JavaMemoryContext.
 *
 * @author Thomas Hallgren
 *************************************************************************/

/*
 * Allocates a new TypeClass and assigns a default coerceObject method used by
 * all JavaWrapper derivates.
 */
extern TypeClass JavaWrapperClass_alloc(const char* name);

#ifdef __cplusplus
}
#endif
#endif
