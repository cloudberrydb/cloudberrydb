/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_TupleDesc_h
#define __pljava_TupleDesc_h

#include "pljava/type/JavaWrapper.h"
#ifdef __cplusplus
extern "C" {
#endif

#include <access/tupdesc.h>

/********************************************************************
 * The TupleDesc java class extends the NativeStruct and provides JNI
 * access to some of the attributes of the TupleDesc structure.
 * 
 * @author Thomas Hallgren
 ********************************************************************/
 
/*
 * Returns the Type of the column at index. If the returned Type
 * is NULL a Java exception has been initiated and the caller
 * should return to Java ASAP.
 */
extern Type TupleDesc_getColumnType(TupleDesc tupleDesc, int index);

/*
 * Create the org.postgresql.pljava.TupleDesc instance
 */
extern jobject TupleDesc_create(TupleDesc tDesc);
extern jobject TupleDesc_internalCreate(TupleDesc tDesc);

#ifdef __cplusplus
}
#endif
#endif
