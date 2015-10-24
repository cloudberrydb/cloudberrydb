/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include "pljava/type/Type_priv.h"
#include "pljava/type/Array.h"
#include "pljava/Invocation.h"

/* The byte maps to the postgres type "char", i.e. the
 * 8 bit, one byte quantity. The java byte was chosen instead of
 * char since a Java char is UTF-16 and "char" is not in any way
 * subject to character set encodings.
 */
static TypeClass s_byteClass;
static jclass    s_Byte_class;
static jmethodID s_Byte_init;
static jmethodID s_Byte_byteValue;

/*
 * byte primitive type.
 */
static Datum _byte_invoke(Type self, jclass cls, jmethodID method, jvalue* args, PG_FUNCTION_ARGS)
{
	jbyte v = JNI_callStaticByteMethodA(cls, method, args);
	return CharGetDatum(v);
}

static jvalue _byte_coerceDatum(Type self, Datum arg)
{
	jvalue result;
	result.b = DatumGetChar(arg);
	return result;
}

static jvalue _byteArray_coerceDatum(Type self, Datum arg)
{
	jvalue     result;
	ArrayType* v      = DatumGetArrayTypeP(arg);
	jsize      nElems = (jsize)ArrayGetNItems(ARR_NDIM(v), ARR_DIMS(v));
	jbyteArray byteArray = JNI_newByteArray(nElems);

	if(ARR_HASNULL(v))
	{
		jsize idx;
		jboolean isCopy = JNI_FALSE;
		bits8* nullBitMap = ARR_NULLBITMAP(v);
		jbyte* values = (jbyte*)ARR_DATA_PTR(v);
		jbyte* elems  = JNI_getByteArrayElements(byteArray, &isCopy);
		for(idx = 0; idx < nElems; ++idx)
		{
			if(arrayIsNull(nullBitMap, idx))
				elems[idx] = 0;
			else
				elems[idx] = *values++;
		}
		JNI_releaseByteArrayElements(byteArray, elems, JNI_COMMIT);
	}
	else
		JNI_setByteArrayRegion(byteArray, 0, nElems, (jbyte*)ARR_DATA_PTR(v));
	result.l = (jobject)byteArray;
	return result;
}

static Datum _byteArray_coerceObject(Type self, jobject byteArray)
{
	ArrayType* v;
	jsize nElems;

	if(byteArray == 0)
		return 0;

	nElems = JNI_getArrayLength((jarray)byteArray);
	v = createArrayType(nElems, sizeof(jbyte), CHAROID, false);
	JNI_getByteArrayRegion((jbyteArray)byteArray, 0, nElems, (jbyte*)ARR_DATA_PTR(v));	

	PG_RETURN_ARRAYTYPE_P(v);
}

/*
 * java.lang.Byte type.
 */
static bool _Byte_canReplace(Type self, Type other)
{
	TypeClass cls = Type_getClass(other);
	return Type_getClass(self) == cls || cls == s_byteClass;
}

static jvalue _Byte_coerceDatum(Type self, Datum arg)
{
	jvalue result;
	result.l = JNI_newObject(s_Byte_class, s_Byte_init, DatumGetChar(arg));
	return result;
}

static Datum _Byte_coerceObject(Type self, jobject byteObj)
{
	return CharGetDatum(byteObj == 0 ? 0 : JNI_callByteMethod(byteObj, s_Byte_byteValue));
}

static Type _byte_createArrayType(Type self, Oid arrayTypeId)
{
	return Array_fromOid2(arrayTypeId, self, _byteArray_coerceDatum, _byteArray_coerceObject);
}

/* Make this datatype available to the postgres system.
 */
extern void Byte_initialize(void);
void Byte_initialize(void)
{
	Type t_byte;
	Type t_Byte;
	TypeClass cls;

	s_Byte_class = JNI_newGlobalRef(PgObject_getJavaClass("java/lang/Byte"));
	s_Byte_init = PgObject_getJavaMethod(s_Byte_class, "<init>", "(B)V");
	s_Byte_byteValue = PgObject_getJavaMethod(s_Byte_class, "byteValue", "()B");

	cls = TypeClass_alloc("type.Byte");
	cls->canReplaceType = _Byte_canReplace;
	cls->JNISignature = "Ljava/lang/Byte;";
	cls->javaTypeName = "java.lang.Byte";
	cls->coerceDatum  = _Byte_coerceDatum;
	cls->coerceObject = _Byte_coerceObject;
	t_Byte = TypeClass_allocInstance(cls, CHAROID);

	cls = TypeClass_alloc("type.byte");
	cls->JNISignature = "B";
	cls->javaTypeName = "byte";
	cls->invoke       = _byte_invoke;
	cls->coerceDatum  = _byte_coerceDatum;
	cls->coerceObject = _Byte_coerceObject;
	cls->createArrayType = _byte_createArrayType;
	s_byteClass = cls;

	t_byte = TypeClass_allocInstance(cls, CHAROID);
	t_byte->objectType = t_Byte;
	Type_registerType("byte", t_byte);
	Type_registerType("java.lang.Byte", t_Byte);
}
