/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_SQLInputFromTuple_h
#define __pljava_SQLInputFromTuple_h

#include "pljava/PgObject.h"

#ifdef __cplusplus
extern "C" {
#endif

#include <access/htup.h>

/***********************************************************************
 * Provides mapping between java.sql.SQLInput and a HeapTupleHeader
 * 
 * @author Thomas Hallgren
 *
 ***********************************************************************/

extern jobject SQLInputFromTuple_create(HeapTupleHeader hth, TupleDesc td);

#ifdef __cplusplus
} /* end of extern "C" declaration */
#endif
#endif
