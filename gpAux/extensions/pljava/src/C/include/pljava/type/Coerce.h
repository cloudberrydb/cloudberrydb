/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_type_Coerce_h
#define __pljava_type_Coerce_h

#include "pljava/type/Type.h"

#ifdef __cplusplus
extern "C" {
#endif

/**************************************************************************
 * The Coerce class extends the Type and adds the members necessary to
 * perform standard Postgres input/output type coersion.
 * 
 * @author Thomas Hallgren
 *
 **************************************************************************/

/* Create a type that will use the coerce function denoted by coerceFunctionID
 * to coerce Datums before they are passed to the object coercion function
 * of the originalType.
 */
extern Type Coerce_createIn(Type originalType, Type source, Oid coerceFunctionID);

/* Create a type that will coerce an object into the type denoted by the
 * originalType and then coerce this datum using the coerce function denoted
 * by the coerceFunctionID.
 */
extern Type Coerce_createOut(Type originalType, Type dest, Oid coerceFunctionID);

#ifdef __cplusplus
}
#endif
#endif
