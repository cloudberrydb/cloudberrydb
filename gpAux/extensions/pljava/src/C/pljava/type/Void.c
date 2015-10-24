/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include <postgres.h>
#include <utils/memutils.h>
#include <utils/numeric.h>

#include "pljava/type/Type_priv.h"

/*
 * void primitive type.
 */
static Datum _void_invoke(Type self, jclass cls, jmethodID method, jvalue* args, PG_FUNCTION_ARGS)
{
	JNI_callStaticVoidMethodA(cls, method, args);
	fcinfo->isnull = true;
	return 0;
}

static jvalue _void_coerceDatum(Type self, Datum nothing)
{
	jvalue result;
	result.j = 0L;
	return result;
}

static Datum _void_coerceObject(Type self, jobject nothing)
{
	return 0;
}

/* Make this datatype available to the postgres system.
 */
extern void Void_initialize(void);
void Void_initialize(void)
{
	TypeClass cls = TypeClass_alloc("type.void");
	cls->JNISignature = "V";
	cls->javaTypeName = "void";
	cls->invoke       = _void_invoke;
	cls->coerceDatum  = _void_coerceDatum;
	cls->coerceObject = _void_coerceObject;
	Type_registerType("void", TypeClass_allocInstance(cls, VOIDOID));
}
