/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_SQLOutputToTuple_h
#define __pljava_SQLOutputToTuple_h

#include "pljava/PgObject.h"

#ifdef __cplusplus
extern "C" {
#endif

#include <access/tupdesc.h>

/***********************************************************************
 * Provides mapping between java.sql.SQLOutput and a HeapTuple
 * 
 * @author Thomas Hallgren
 *
 ***********************************************************************/

extern jobject SQLOutputToTuple_create(TupleDesc td);

extern HeapTuple SQLOutputToTuple_getTuple(jobject sqlOutput);

#ifdef __cplusplus
} /* end of extern "C" declaration */
#endif
#endif
