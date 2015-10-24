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

static TypeClass s_floatClass;
static jclass    s_Float_class;
static jmethodID s_Float_init;
static jmethodID s_Float_floatValue;

/*
 * float primitive type.
 */
static Datum _asDatum(jfloat v)
{
	MemoryContext currCtx = Invocation_switchToUpperContext();
	Datum ret = Float4GetDatum(v);
	MemoryContextSwitchTo(currCtx);
	return ret;
}

static Datum _float_invoke(Type self, jclass cls, jmethodID method, jvalue* args, PG_FUNCTION_ARGS)
{
	return _asDatum(JNI_callStaticFloatMethodA(cls, method, args));
}

static jvalue _float_coerceDatum(Type self, Datum arg)
{
	jvalue result;
	result.f = DatumGetFloat4(arg);
	return result;
}

static jvalue _floatArray_coerceDatum(Type self, Datum arg)
{
	jvalue     result;
	ArrayType* v      = DatumGetArrayTypeP(arg);
	jsize      nElems = (jsize)ArrayGetNItems(ARR_NDIM(v), ARR_DIMS(v));
	jfloatArray floatArray = JNI_newFloatArray(nElems);

	if(ARR_HASNULL(v))
	{
		jsize idx;
		jboolean isCopy = JNI_FALSE;
		bits8* nullBitMap = ARR_NULLBITMAP(v);
		jfloat* values = (jfloat*)ARR_DATA_PTR(v);
		jfloat* elems  = JNI_getFloatArrayElements(floatArray, &isCopy);
		for(idx = 0; idx < nElems; ++idx)
		{
			if(arrayIsNull(nullBitMap, idx))
				elems[idx] = 0;
			else
				elems[idx] = *values++;
		}
		JNI_releaseFloatArrayElements(floatArray, elems, JNI_COMMIT);
	}
	else
		JNI_setFloatArrayRegion(floatArray, 0, nElems, (jfloat*)ARR_DATA_PTR(v));
	result.l = (jobject)floatArray;
	return result;
}

static Datum _floatArray_coerceObject(Type self, jobject floatArray)
{
	ArrayType* v;
	jsize nElems;

	if(floatArray == 0)
		return 0;

	nElems = JNI_getArrayLength((jarray)floatArray);
	v = createArrayType(nElems, sizeof(jfloat), FLOAT4OID, false);
	JNI_getFloatArrayRegion((jfloatArray)floatArray, 0, nElems, (jfloat*)ARR_DATA_PTR(v));	

	PG_RETURN_ARRAYTYPE_P(v);
}

/*
 * java.lang.Float type.
 */
static bool _Float_canReplace(Type self, Type other)
{
	TypeClass cls = Type_getClass(other);
	return Type_getClass(self) == cls || cls == s_floatClass;
}

static jvalue _Float_coerceDatum(Type self, Datum arg)
{
	jvalue result;
	result.l = JNI_newObject(s_Float_class, s_Float_init, DatumGetFloat4(arg));
	return result;
}

static Datum _Float_coerceObject(Type self, jobject floatObj)
{
	return _asDatum(floatObj == 0 ? 0.0 : JNI_callFloatMethod(floatObj, s_Float_floatValue));
}

static Type _float_createArrayType(Type self, Oid arrayTypeId)
{
	return Array_fromOid2(arrayTypeId, self, _floatArray_coerceDatum, _floatArray_coerceObject);
}

/* Make this datatype available to the postgres system.
 */
extern void Float_initialize(void);
void Float_initialize(void)
{
	Type t_float;
	Type t_Float;
	TypeClass cls;

	s_Float_class = JNI_newGlobalRef(PgObject_getJavaClass("java/lang/Float"));
	s_Float_init = PgObject_getJavaMethod(s_Float_class, "<init>", "(F)V");
	s_Float_floatValue = PgObject_getJavaMethod(s_Float_class, "floatValue", "()F");

	cls = TypeClass_alloc("type.Float");
	cls->canReplaceType = _Float_canReplace;
	cls->JNISignature = "Ljava/lang/Float;";
	cls->javaTypeName = "java.lang.Float";
	cls->coerceDatum  = _Float_coerceDatum;
	cls->coerceObject = _Float_coerceObject;
	t_Float = TypeClass_allocInstance(cls, FLOAT4OID);

	cls = TypeClass_alloc("type.float");
	cls->JNISignature = "F";
	cls->javaTypeName = "float";
	cls->invoke       = _float_invoke;
	cls->coerceDatum  = _float_coerceDatum;
	cls->coerceObject = _Float_coerceObject;
	cls->createArrayType = _float_createArrayType;
	s_floatClass = cls;

	t_float = TypeClass_allocInstance(cls, FLOAT4OID);
	t_float->objectType = t_Float;
	Type_registerType("float", t_float);
	Type_registerType("java.lang.Float", t_Float);
}
