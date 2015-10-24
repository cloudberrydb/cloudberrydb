/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_type_UDT_h
#define __pljava_type_UDT_h

#include "pljava/type/Type.h"

#ifdef __cplusplus
extern "C" {
#endif

/**************************************************************************
 * The UDT class extends the Type and adds the members necessary to
 * perform standard Postgres input/output and send/receive conversion.
 * 
 * @author Thomas Hallgren
 *
 **************************************************************************/

struct UDT_;
typedef struct UDT_* UDT;

extern Datum UDT_input(UDT udt, PG_FUNCTION_ARGS);
extern Datum UDT_output(UDT udt, PG_FUNCTION_ARGS);
extern Datum UDT_receive(UDT udt, PG_FUNCTION_ARGS);
extern Datum UDT_send(UDT udt, PG_FUNCTION_ARGS);

extern bool UDT_isScalar(UDT udt);

extern UDT UDT_registerUDT(jclass clazz, Oid typeId, Form_pg_type pgType, TupleDesc td, bool isJavaBasedScalar);

typedef Datum (*UDTFunction)(UDT udt, PG_FUNCTION_ARGS);

#ifdef __cplusplus
}
#endif
#endif
