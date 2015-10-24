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

static TypeClass s_intClass;
static jclass    s_Integer_class;
static jmethodID s_Integer_init;
static jmethodID s_Integer_intValue;

/*
 * int primitive type.
 */
static Datum _int_invoke(Type self, jclass cls, jmethodID method, jvalue* args, PG_FUNCTION_ARGS)
{
	jint iv = JNI_callStaticIntMethodA(cls, method, args);
	return Int32GetDatum(iv);
}

static jvalue _int_coerceDatum(Type self, Datum arg)
{
	jvalue result;
	result.i = DatumGetInt32(arg);
	return result;
}

static jvalue _intArray_coerceDatum(Type self, Datum arg)
{
	jvalue     result;
	ArrayType* v        = DatumGetArrayTypeP(arg);
	jsize      nElems   = (jsize)ArrayGetNItems(ARR_NDIM(v), ARR_DIMS(v));
	jintArray  intArray = JNI_newIntArray(nElems);

	if(ARR_HASNULL(v))
	{
		jsize idx;
		jboolean isCopy = JNI_FALSE;
		bits8* nullBitMap = ARR_NULLBITMAP(v);
		jint* values = (jint*)ARR_DATA_PTR(v);
		jint* elems  = JNI_getIntArrayElements(intArray, &isCopy);
		for(idx = 0; idx < nElems; ++idx)
		{
			if(arrayIsNull(nullBitMap, idx))
				elems[idx] = 0;
			else
				elems[idx] = *values++;
		}
		JNI_releaseIntArrayElements(intArray, elems, JNI_COMMIT);
	}
	else
		JNI_setIntArrayRegion(intArray, 0, nElems, (jint*)ARR_DATA_PTR(v));
	result.l = (jobject)intArray;
	return result;
}

static Datum _intArray_coerceObject(Type self, jobject intArray)
{
	ArrayType* v;
	jsize nElems;

	if(intArray == 0)
		return 0;

	nElems = JNI_getArrayLength((jarray)intArray);
	v = createArrayType(nElems, sizeof(jint), INT4OID, false);
	JNI_getIntArrayRegion((jintArray)intArray, 0, nElems, (jint*)ARR_DATA_PTR(v));	

	PG_RETURN_ARRAYTYPE_P(v);
}

/*
 * java.lang.Integer type.
 */
static bool _Integer_canReplace(Type self, Type other)
{
	TypeClass cls = Type_getClass(other);
	return Type_getClass(self) == cls || cls == s_intClass;
}

static jvalue _Integer_coerceDatum(Type self, Datum arg)
{
	jvalue result;
	result.l = JNI_newObject(s_Integer_class, s_Integer_init, DatumGetInt32(arg));
	return result;
}

static Datum _Integer_coerceObject(Type self, jobject intObj)
{
	return Int32GetDatum(intObj == 0 ? 0 : JNI_callIntMethod(intObj, s_Integer_intValue));
}

static Type _int_createArrayType(Type self, Oid arrayTypeId)
{
	return Array_fromOid2(arrayTypeId, self, _intArray_coerceDatum, _intArray_coerceObject);
}

/* Make this datatype available to the postgres system.
 */
extern void Integer_initialize(void);
void Integer_initialize(void)
{
	Type t_int;
	Type t_Integer;
	TypeClass cls;

	s_Integer_class = JNI_newGlobalRef(PgObject_getJavaClass("java/lang/Integer"));
	s_Integer_init = PgObject_getJavaMethod(s_Integer_class, "<init>", "(I)V");
	s_Integer_intValue = PgObject_getJavaMethod(s_Integer_class, "intValue", "()I");

	cls = TypeClass_alloc("type.Integer");
	cls->canReplaceType = _Integer_canReplace;
	cls->JNISignature = "Ljava/lang/Integer;";
	cls->javaTypeName = "java.lang.Integer";
	cls->coerceDatum  = _Integer_coerceDatum;
	cls->coerceObject = _Integer_coerceObject;
	t_Integer = TypeClass_allocInstance(cls, INT4OID);

	cls = TypeClass_alloc("type.int");
	cls->JNISignature = "I";
	cls->javaTypeName = "int";
	cls->invoke       = _int_invoke;
	cls->coerceDatum  = _int_coerceDatum;
	cls->coerceObject = _Integer_coerceObject;
	cls->createArrayType = _int_createArrayType;
	s_intClass = cls;

	t_int = TypeClass_allocInstance(cls, INT4OID);
	t_int->objectType = t_Integer;
	Type_registerType("int", t_int);
	Type_registerType("java.lang.Integer", t_Integer);
}
