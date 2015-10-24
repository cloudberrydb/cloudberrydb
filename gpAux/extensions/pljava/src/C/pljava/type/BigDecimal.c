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

#include "pljava/type/String_priv.h"

/*
 * BigDecimal type. We use String conversions here. Perhaps there's
 * room for optimizations such as creating a 2's complement byte
 * array directly from the digits. Don't think we'd gain much though.
 */
static jclass    s_BigDecimal_class;
static jmethodID s_BigDecimal_init;
static jmethodID s_BigDecimal_toString;
static TypeClass s_BigDecimalClass;

static jvalue _BigDecimal_coerceDatum(Type self, Datum arg)
{
	jvalue result = _String_coerceDatum(self, arg);
	if(result.l != 0)
		result.l = JNI_newObject(s_BigDecimal_class, s_BigDecimal_init, result.l);
	return result;
}

static Datum _BigDecimal_coerceObject(Type self, jobject value)
{
	jstring jstr = (jstring)JNI_callObjectMethod(value, s_BigDecimal_toString);
	Datum ret = _String_coerceObject(self, jstr);
	JNI_deleteLocalRef(jstr);
	return ret;
}

static Type BigDecimal_obtain(Oid typeId)
{
	return (Type)StringClass_obtain(s_BigDecimalClass, typeId);
}

/* Make this datatype available to the postgres system.
 */
extern void BigDecimal_initialize(void);
void BigDecimal_initialize(void)
{
	s_BigDecimal_class = JNI_newGlobalRef(PgObject_getJavaClass("java/math/BigDecimal"));
	s_BigDecimal_init = PgObject_getJavaMethod(s_BigDecimal_class, "<init>", "(Ljava/lang/String;)V");
	s_BigDecimal_toString = PgObject_getJavaMethod(s_BigDecimal_class, "toString", "()Ljava/lang/String;");

	s_BigDecimalClass = TypeClass_alloc2("type.BigDecimal", sizeof(struct TypeClass_), sizeof(struct String_));
	s_BigDecimalClass->JNISignature   = "Ljava/math/BigDecimal;";
	s_BigDecimalClass->javaTypeName   = "java.math.BigDecimal";
	s_BigDecimalClass->canReplaceType = _Type_canReplaceType;
	s_BigDecimalClass->coerceDatum    = _BigDecimal_coerceDatum;
	s_BigDecimalClass->coerceObject   = _BigDecimal_coerceObject;

	Type_registerType2(NUMERICOID, "java.math.BigDecimal", BigDecimal_obtain);
}
