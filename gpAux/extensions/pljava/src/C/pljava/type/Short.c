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

static TypeClass s_shortClass;
static jclass    s_Short_class;
static jmethodID s_Short_init;
static jmethodID s_Short_shortValue;

/*
 * short primitive type.
 */
static Datum _short_invoke(Type self, jclass cls, jmethodID method, jvalue* args, PG_FUNCTION_ARGS)
{
	jshort v = JNI_callStaticShortMethodA(cls, method, args);
	return Int16GetDatum(v);
}

static jvalue _short_coerceDatum(Type self, Datum arg)
{
	jvalue result;
	result.s = DatumGetInt16(arg);
	return result;
}

static jvalue _shortArray_coerceDatum(Type self, Datum arg)
{
	jvalue     result;
	ArrayType* v      = DatumGetArrayTypeP(arg);
	jsize      nElems = (jsize)ArrayGetNItems(ARR_NDIM(v), ARR_DIMS(v));
	jshortArray shortArray = JNI_newShortArray(nElems);

	if(ARR_HASNULL(v))
	{
		jsize idx;
		jboolean isCopy = JNI_FALSE;
		bits8* nullBitMap = ARR_NULLBITMAP(v);
		jshort* values = (jshort*)ARR_DATA_PTR(v);
		jshort* elems  = JNI_getShortArrayElements(shortArray, &isCopy);
		for(idx = 0; idx < nElems; ++idx)
		{
			if(arrayIsNull(nullBitMap, idx))
				elems[idx] = 0;
			else
				elems[idx] = *values++;
		}
		JNI_releaseShortArrayElements(shortArray, elems, JNI_COMMIT);
	}
	else
		JNI_setShortArrayRegion(shortArray, 0, nElems, (jshort*)ARR_DATA_PTR(v));
	result.l = (jobject)shortArray;
	return result;
}

static Datum _shortArray_coerceObject(Type self, jobject shortArray)
{
	ArrayType* v;
	jsize nElems;

	if(shortArray == 0)
		return 0;

	nElems = JNI_getArrayLength((jarray)shortArray);
	v = createArrayType(nElems, sizeof(jshort), INT2OID, false);
	JNI_getShortArrayRegion((jshortArray)shortArray, 0, nElems, (jshort*)ARR_DATA_PTR(v));	

	PG_RETURN_ARRAYTYPE_P(v);
}

/*
 * java.lang.Short type.
 */
static bool _Short_canReplace(Type self, Type other)
{
	TypeClass cls = Type_getClass(other);
	return Type_getClass(self) == cls || cls == s_shortClass;
}

static jvalue _Short_coerceDatum(Type self, Datum arg)
{
	jvalue result;
	result.l = JNI_newObject(s_Short_class, s_Short_init, DatumGetInt16(arg));
	return result;
}

static Datum _Short_coerceObject(Type self, jobject shortObj)
{
	return Int16GetDatum(shortObj == 0 ? 0 : JNI_callShortMethod(shortObj, s_Short_shortValue));
}

static Type _short_createArrayType(Type self, Oid arrayTypeId)
{
	return Array_fromOid2(arrayTypeId, self, _shortArray_coerceDatum, _shortArray_coerceObject);
}

/* Make this datatype available to the postgres system.
 */
extern void Short_initialize(void);
void Short_initialize(void)
{
	Type t_short;
	Type t_Short;
	TypeClass cls;

	s_Short_class = JNI_newGlobalRef(PgObject_getJavaClass("java/lang/Short"));
	s_Short_init = PgObject_getJavaMethod(s_Short_class, "<init>", "(S)V");
	s_Short_shortValue = PgObject_getJavaMethod(s_Short_class, "shortValue", "()S");

	cls = TypeClass_alloc("type.Short");
	cls->canReplaceType = _Short_canReplace;
	cls->JNISignature = "Ljava/lang/Short;";
	cls->javaTypeName = "java.lang.Short";
	cls->coerceDatum  = _Short_coerceDatum;
	cls->coerceObject = _Short_coerceObject;
	t_Short = TypeClass_allocInstance(cls, INT2OID);

	cls = TypeClass_alloc("type.short");
	cls->JNISignature = "S";
	cls->javaTypeName = "short";
	cls->invoke       = _short_invoke;
	cls->coerceDatum  = _short_coerceDatum;
	cls->coerceObject = _Short_coerceObject;
	cls->createArrayType = _short_createArrayType;
	s_shortClass = cls;

	t_short = TypeClass_allocInstance(cls, INT2OID);
	t_short->objectType = t_Short;
	Type_registerType("short", t_short);
	Type_registerType("java.lang.Short", t_Short);
}
