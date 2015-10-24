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

static TypeClass s_doubleClass;
static jclass    s_Double_class;
static jmethodID s_Double_init;
static jmethodID s_Double_doubleValue;

/*
 * double primitive type.
 */
static Datum _asDatum(jdouble v)
{
	MemoryContext currCtx = Invocation_switchToUpperContext();
	Datum ret = Float8GetDatum(v);
	MemoryContextSwitchTo(currCtx);
	return ret;
}

static Datum _double_invoke(Type self, jclass cls, jmethodID method, jvalue* args, PG_FUNCTION_ARGS)
{
	return _asDatum(JNI_callStaticDoubleMethodA(cls, method, args));
}

static jvalue _double_coerceDatum(Type self, Datum arg)
{
	jvalue result;
	result.d = DatumGetFloat8(arg);
	return result;
}

static jvalue _doubleArray_coerceDatum(Type self, Datum arg)
{
	jvalue     result;
	ArrayType* v      = DatumGetArrayTypeP(arg);
	jsize      nElems = (jsize)ArrayGetNItems(ARR_NDIM(v), ARR_DIMS(v));
	jdoubleArray doubleArray = JNI_newDoubleArray(nElems);

	if(ARR_HASNULL(v))
	{
		jsize idx;
		jboolean isCopy = JNI_FALSE;
		bits8* nullBitMap = ARR_NULLBITMAP(v);
		jdouble* values = (jdouble*)ARR_DATA_PTR(v);
		jdouble* elems  = JNI_getDoubleArrayElements(doubleArray, &isCopy);
		for(idx = 0; idx < nElems; ++idx)
		{
			if(arrayIsNull(nullBitMap, idx))
				elems[idx] = 0;
			else
				elems[idx] = *values++;
		}
		JNI_releaseDoubleArrayElements(doubleArray, elems, JNI_COMMIT);
	}
	else
		JNI_setDoubleArrayRegion(doubleArray, 0, nElems, (jdouble*)ARR_DATA_PTR(v));
	result.l = (jobject)doubleArray;
	return result;
}

static Datum _doubleArray_coerceObject(Type self, jobject doubleArray)
{
	ArrayType* v;
	jsize nElems;

	if(doubleArray == 0)
		return 0;

	nElems = JNI_getArrayLength((jarray)doubleArray);
	v = createArrayType(nElems, sizeof(jdouble), FLOAT8OID, false);
	JNI_getDoubleArrayRegion((jdoubleArray)doubleArray, 0, nElems, (jdouble*)ARR_DATA_PTR(v));	

	PG_RETURN_ARRAYTYPE_P(v);
}

/*
 * java.lang.Double type.
 */
static bool _Double_canReplace(Type self, Type other)
{
	TypeClass cls = Type_getClass(other);
	return Type_getClass(self) == cls || cls == s_doubleClass;
}

static jvalue _Double_coerceDatum(Type self, Datum arg)
{
	jvalue result;
	result.l = JNI_newObject(s_Double_class, s_Double_init, DatumGetFloat8(arg));
	return result;
}

static Datum _Double_coerceObject(Type self, jobject doubleObj)
{
	return _asDatum(doubleObj == 0 ? 0 : JNI_callDoubleMethod(doubleObj, s_Double_doubleValue));
}

static Type _double_createArrayType(Type self, Oid arrayTypeId)
{
	return Array_fromOid2(arrayTypeId, self, _doubleArray_coerceDatum, _doubleArray_coerceObject);
}

/* Make this datatype available to the postgres system.
 */
extern void Double_initialize(void);
void Double_initialize(void)
{
	Type t_double;
	Type t_Double;
	TypeClass cls;

	s_Double_class = JNI_newGlobalRef(PgObject_getJavaClass("java/lang/Double"));
	s_Double_init = PgObject_getJavaMethod(s_Double_class, "<init>", "(D)V");
	s_Double_doubleValue = PgObject_getJavaMethod(s_Double_class, "doubleValue", "()D");

	cls = TypeClass_alloc("type.Double");
	cls->canReplaceType = _Double_canReplace;
	cls->JNISignature = "Ljava/lang/Double;";
	cls->javaTypeName = "java.lang.Double";
	cls->coerceDatum  = _Double_coerceDatum;
	cls->coerceObject = _Double_coerceObject;
	t_Double = TypeClass_allocInstance(cls, FLOAT8OID);

	cls = TypeClass_alloc("type.double");
	cls->JNISignature = "D";
	cls->javaTypeName = "double";
	cls->invoke       = _double_invoke;
	cls->coerceDatum  = _double_coerceDatum;
	cls->coerceObject = _Double_coerceObject;
	cls->createArrayType = _double_createArrayType;
	s_doubleClass = cls;

	t_double = TypeClass_allocInstance(cls, FLOAT8OID);
	t_double->objectType = t_Double;
	Type_registerType("double", t_double);
	Type_registerType("java.lang.Double", t_Double);
}
