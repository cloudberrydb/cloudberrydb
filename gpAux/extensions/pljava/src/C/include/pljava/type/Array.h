/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
#ifndef __pljava_type_Array_h
#define __pljava_type_Array_h

#include "pljava/type/Type.h"

#ifdef __cplusplus
extern "C" {
#endif

#include <access/tupmacs.h>

/***********************************************************************
 * Array related stuff.
 * 
 * @author Thomas Hallgren
 *
 ***********************************************************************/

extern ArrayType* createArrayType(jsize nElems, size_t elemSize, Oid elemType, bool withNulls);
extern void arraySetNull(bits8* bitmap, int offset, bool flag);
extern bool arrayIsNull(const bits8* bitmap, int offset);
extern Type Array_fromOid(Oid typeId, Type elementType);
extern Type Array_fromOid2(Oid typeId, Type elementType, DatumCoercer coerceDatum, ObjectCoercer coerceObject);

#ifdef __cplusplus
} /* end of extern "C" declaration */
#endif
#endif
