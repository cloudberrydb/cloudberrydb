/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include "org_postgresql_pljava_internal_ErrorData.h"
#include "pljava/Exception.h"
#include "pljava/type/Type_priv.h"
#include "pljava/type/ErrorData.h"
#include "pljava/type/String.h"

static jclass    s_ErrorData_class;
static jmethodID s_ErrorData_init;
static jmethodID s_ErrorData_getNativePointer;

jobject ErrorData_getCurrentError(void)
{
	Ptr2Long p2l;
	jobject jed;

	MemoryContext curr = MemoryContextSwitchTo(JavaMemoryContext);
	ErrorData* errorData = CopyErrorData();
	MemoryContextSwitchTo(curr);

	p2l.longVal = 0L; /* ensure that the rest is zeroed out */
	p2l.ptrVal = errorData;
	jed = JNI_newObject(s_ErrorData_class, s_ErrorData_init, p2l.longVal);
	return jed;
}

ErrorData* ErrorData_getErrorData(jobject jed)
{	
	Ptr2Long p2l;
	p2l.longVal = JNI_callLongMethod(jed, s_ErrorData_getNativePointer);
	return (ErrorData*)p2l.ptrVal;
}

/* Make this datatype available to the postgres system.
 */
extern void ErrorData_initialize(void);
void ErrorData_initialize(void)
{
	JNINativeMethod methods[] = {
		{
		"_getErrorLevel",
	  	"(J)I",
	  	Java_org_postgresql_pljava_internal_ErrorData__1getErrorLevel
		},
		{
		"_isOutputToServer",
	  	"(J)Z",
	  	Java_org_postgresql_pljava_internal_ErrorData__1isOutputToServer
		},
		{
		"_isOutputToClient",
	  	"(J)Z",
	  	Java_org_postgresql_pljava_internal_ErrorData__1isOutputToClient
		},
		{
		"_isShowFuncname",
	  	"(J)Z",
	  	Java_org_postgresql_pljava_internal_ErrorData__1isShowFuncname
		},
		{
		"_getFilename",
	  	"(J)Ljava/lang/String;",
	  	Java_org_postgresql_pljava_internal_ErrorData__1getFilename
		},
		{
		"_getLineno",
	  	"(J)I",
	  	Java_org_postgresql_pljava_internal_ErrorData__1getLineno
		},
		{
		"_getFuncname",
	  	"(J)Ljava/lang/String;",
	  	Java_org_postgresql_pljava_internal_ErrorData__1getFuncname
		},
		{
		"_getSqlState",
	  	"(J)Ljava/lang/String;",
	  	Java_org_postgresql_pljava_internal_ErrorData__1getSqlState
		},
		{
		"_getMessage",
	  	"(J)Ljava/lang/String;",
	  	Java_org_postgresql_pljava_internal_ErrorData__1getMessage
		},
		{
		"_getDetail",
	  	"(J)Ljava/lang/String;",
	  	Java_org_postgresql_pljava_internal_ErrorData__1getDetail
		},
		{
		"_getHint",
	  	"(J)Ljava/lang/String;",
	  	Java_org_postgresql_pljava_internal_ErrorData__1getHint
		},
		{
		"_getContextMessage",
	  	"(J)Ljava/lang/String;",
	  	Java_org_postgresql_pljava_internal_ErrorData__1getContextMessage
		},
		{
		"_getCursorPos",
	  	"(J)I",
	  	Java_org_postgresql_pljava_internal_ErrorData__1getCursorPos
		},
		{
		"_getInternalPos",
	  	"(J)I",
	  	Java_org_postgresql_pljava_internal_ErrorData__1getInternalPos
		},
		{
		"_getInternalQuery",
	  	"(J)Ljava/lang/String;",
	  	Java_org_postgresql_pljava_internal_ErrorData__1getInternalQuery
		},
		{
		"_getSavedErrno",
	  	"(J)I",
	  	Java_org_postgresql_pljava_internal_ErrorData__1getSavedErrno
		},
		{
		"_free",
	  	"(J)V",
	  	Java_org_postgresql_pljava_internal_ErrorData__1free
		},
		{ 0, 0, 0 }
	};

	s_ErrorData_class = JNI_newGlobalRef(PgObject_getJavaClass("org/postgresql/pljava/internal/ErrorData"));
	PgObject_registerNatives2(s_ErrorData_class, methods);
	s_ErrorData_init = PgObject_getJavaMethod(s_ErrorData_class, "<init>", "(J)V");
	s_ErrorData_getNativePointer = PgObject_getJavaMethod(s_ErrorData_class, "getNativePointer", "()J");
}

/****************************************
 * JNI methods
 ****************************************/

/*
 * Class:     org_postgresql_pljava_internal_ErrorData
 * Method:    getErrorLevel
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL
Java_org_postgresql_pljava_internal_ErrorData__1getErrorLevel(JNIEnv* env, jclass cls, jlong _this)
{
	Ptr2Long p2l;
	p2l.longVal = _this;
	return ((ErrorData*)p2l.ptrVal)->elevel;
}

/*
 * Class:     org_postgresql_pljava_internal_ErrorData
 * Method:    getMessage
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_postgresql_pljava_internal_ErrorData__1getMessage(JNIEnv* env, jclass cls, jlong _this)
{
	jstring result = 0;
	BEGIN_NATIVE_NO_ERRCHECK
	Ptr2Long p2l;
	p2l.longVal = _this;
	result = String_createJavaStringFromNTS(((ErrorData*)p2l.ptrVal)->message);
	END_NATIVE
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_ErrorData
 * Method:    getSqlState
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_postgresql_pljava_internal_ErrorData__1getSqlState(JNIEnv* env, jclass cls, jlong _this)
{
	jstring result = 0;
	BEGIN_NATIVE_NO_ERRCHECK
	char buf[6];
	int errCode;
	int idx;
	Ptr2Long p2l;
	p2l.longVal = _this;

	/* unpack MAKE_SQLSTATE code
	 */
	errCode = ((ErrorData*)p2l.ptrVal)->sqlerrcode;
	for (idx = 0; idx < 5; ++idx)
	{
		buf[idx] = PGUNSIXBIT(errCode);
		errCode >>= 6;
	}
	buf[idx] = 0;
	result = String_createJavaStringFromNTS(buf);

	END_NATIVE
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_ErrorData
 * Method:    isOutputToServer
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_postgresql_pljava_internal_ErrorData__1isOutputToServer(JNIEnv* env, jclass cls, jlong _this)
{
	Ptr2Long p2l;
	p2l.longVal = _this;
	return (jboolean)((ErrorData*)p2l.ptrVal)->output_to_server;
}

/*
 * Class:     org_postgresql_pljava_internal_ErrorData
 * Method:    isOutputToClient
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_postgresql_pljava_internal_ErrorData__1isOutputToClient(JNIEnv* env, jclass cls, jlong _this)
{
	Ptr2Long p2l;
	p2l.longVal = _this;
	return (jboolean)((ErrorData*)p2l.ptrVal)->output_to_client;
}

/*
 * Class:     org_postgresql_pljava_internal_ErrorData
 * Method:    isShowFuncname
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_postgresql_pljava_internal_ErrorData__1isShowFuncname(JNIEnv* env, jclass cls, jlong _this)
{
	Ptr2Long p2l;
	p2l.longVal = _this;
	return (jboolean)((ErrorData*)p2l.ptrVal)->show_funcname;
}

/*
 * Class:     org_postgresql_pljava_internal_ErrorData
 * Method:    getFilename
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_postgresql_pljava_internal_ErrorData__1getFilename(JNIEnv* env, jclass cls, jlong _this)
{
	jstring result = 0;
	BEGIN_NATIVE_NO_ERRCHECK
	Ptr2Long p2l;
	p2l.longVal = _this;
	result = String_createJavaStringFromNTS(((ErrorData*)p2l.ptrVal)->filename);
	END_NATIVE
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_ErrorData
 * Method:    getLineno
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_postgresql_pljava_internal_ErrorData__1getLineno(JNIEnv* env, jclass cls, jlong _this)
{
	Ptr2Long p2l;
	p2l.longVal = _this;
	return (jint)((ErrorData*)p2l.ptrVal)->lineno;
}

/*
 * Class:     org_postgresql_pljava_internal_ErrorData
 * Method:    getFuncname
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_postgresql_pljava_internal_ErrorData__1getFuncname(JNIEnv* env, jclass cls, jlong _this)
{
	jstring result = 0;
	BEGIN_NATIVE_NO_ERRCHECK
	Ptr2Long p2l;
	p2l.longVal = _this;
	result = String_createJavaStringFromNTS(((ErrorData*)p2l.ptrVal)->funcname);
	END_NATIVE
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_ErrorData
 * Method:    getDetail
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_postgresql_pljava_internal_ErrorData__1getDetail(JNIEnv* env, jclass cls, jlong _this)
{
	jstring result = 0;
	BEGIN_NATIVE_NO_ERRCHECK
	Ptr2Long p2l;
	p2l.longVal = _this;
	result = String_createJavaStringFromNTS(((ErrorData*)p2l.ptrVal)->detail);
	END_NATIVE
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_ErrorData
 * Method:    getHint
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_postgresql_pljava_internal_ErrorData__1getHint(JNIEnv* env, jclass cls, jlong _this)
{
	jstring result = 0;
	BEGIN_NATIVE_NO_ERRCHECK
	Ptr2Long p2l;
	p2l.longVal = _this;
	result = String_createJavaStringFromNTS(((ErrorData*)p2l.ptrVal)->hint);
	END_NATIVE
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_ErrorData
 * Method:    getContextMessage
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_postgresql_pljava_internal_ErrorData__1getContextMessage(JNIEnv* env, jclass cls, jlong _this)
{
	jstring result = 0;
	BEGIN_NATIVE_NO_ERRCHECK
	Ptr2Long p2l;
	p2l.longVal = _this;
	result = String_createJavaStringFromNTS(((ErrorData*)p2l.ptrVal)->context);
	END_NATIVE
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_ErrorData
 * Method:    getCursorPos
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_postgresql_pljava_internal_ErrorData__1getCursorPos(JNIEnv* env, jclass cls, jlong _this)
{
	Ptr2Long p2l;
	p2l.longVal = _this;
	return (jint)((ErrorData*)p2l.ptrVal)->cursorpos;
}

/*
 * Class:     org_postgresql_pljava_internal_ErrorData
 * Method:    getInternalPos
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_postgresql_pljava_internal_ErrorData__1getInternalPos(JNIEnv* env, jclass cls, jlong _this)
{
	Ptr2Long p2l;
	p2l.longVal = _this;
	return (jint)((ErrorData*)p2l.ptrVal)->internalpos;
}

/*
 * Class:     org_postgresql_pljava_internal_ErrorData
 * Method:    getInternalQuery
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_postgresql_pljava_internal_ErrorData__1getInternalQuery(JNIEnv* env, jclass cls, jlong _this)
{
	jstring result = 0;

	BEGIN_NATIVE_NO_ERRCHECK
	Ptr2Long p2l;
	p2l.longVal = _this;
	result = String_createJavaStringFromNTS(((ErrorData*)p2l.ptrVal)->internalquery);
	END_NATIVE

	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_ErrorData
 * Method:    getSavedErrno
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_postgresql_pljava_internal_ErrorData__1getSavedErrno(JNIEnv* env, jclass cls, jlong _this)
{
	Ptr2Long p2l;
	p2l.longVal = _this;
	return (jint)((ErrorData*)p2l.ptrVal)->saved_errno;
}

/*
 * Class:     org_postgresql_pljava_internal_ErrorData
 * Method:    _free
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_postgresql_pljava_internal_ErrorData__1free(JNIEnv* env, jobject _this, jlong pointer)
{
	BEGIN_NATIVE_NO_ERRCHECK
	Ptr2Long p2l;
	p2l.longVal = pointer;
	FreeErrorData(p2l.ptrVal);
	END_NATIVE
}
