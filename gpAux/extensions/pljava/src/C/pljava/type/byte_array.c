/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include "pljava/Exception.h"
#include "pljava/type/Type_priv.h"

static jclass s_byteArray_class;
static jclass s_BlobValue_class;
static jmethodID s_BlobValue_length;
static jmethodID s_BlobValue_getContents;

/*
 * byte[] type. Copies data to/from a bytea struct.
 */
static jvalue _byte_array_coerceDatum(Type self, Datum arg)
{
	jvalue result;
	bytea* bytes  = DatumGetByteaP(arg);
	jsize  length = VARSIZE(bytes) - VARHDRSZ;
	jbyteArray ba = JNI_newByteArray(length);
	JNI_setByteArrayRegion(ba, 0, length, (jbyte*)VARDATA(bytes)); 
	result.l = ba;
	return result;
}

static Datum _byte_array_coerceObject(Type self, jobject byteArray)
{
	bytea* bytes = 0;
	if(byteArray == 0)
		return 0;

	if(JNI_isInstanceOf(byteArray, s_byteArray_class))
	{
		jsize  length    = JNI_getArrayLength((jarray)byteArray);
		int32  byteaSize = length + VARHDRSZ;

		bytes = (bytea*)palloc(byteaSize);
		SET_VARSIZE(bytes, byteaSize);
		JNI_getByteArrayRegion((jbyteArray)byteArray, 0, length, (jbyte*)VARDATA(bytes));
	}
	else if(JNI_isInstanceOf(byteArray, s_BlobValue_class))
	{
		jobject byteBuffer;
		int32 byteaSize;
		jlong length = JNI_callLongMethod(byteArray, s_BlobValue_length);

		byteaSize = (int32)(length + VARHDRSZ);
		bytes = (bytea*)palloc(byteaSize);
		SET_VARSIZE(bytes, byteaSize);

		byteBuffer = JNI_newDirectByteBuffer((void*)VARDATA(bytes), length);
		if(byteBuffer != 0)
			JNI_callVoidMethod(byteArray, s_BlobValue_getContents, byteBuffer);
		JNI_deleteLocalRef(byteBuffer);
	}
	else
	{
		Exception_throwIllegalArgument("Not coercable to bytea");
	}

	PG_RETURN_BYTEA_P(bytes);
}

/* Make this datatype available to the postgres system.
 */
extern void byte_array_initialize(void);
void byte_array_initialize(void)
{
	TypeClass cls = TypeClass_alloc("type.byte[]");
	cls->JNISignature = "[B";
	cls->javaTypeName = "byte[]";
	cls->coerceDatum  = _byte_array_coerceDatum;
	cls->coerceObject = _byte_array_coerceObject;
	Type_registerType("byte[]", TypeClass_allocInstance(cls, BYTEAOID));

	s_byteArray_class = JNI_newGlobalRef(PgObject_getJavaClass("[B"));
	s_BlobValue_class = JNI_newGlobalRef(PgObject_getJavaClass("org/postgresql/pljava/jdbc/BlobValue"));
	s_BlobValue_length = PgObject_getJavaMethod(s_BlobValue_class, "length", "()J");
	s_BlobValue_getContents = PgObject_getJavaMethod(s_BlobValue_class, "getContents", "(Ljava/nio/ByteBuffer;)V");
}

