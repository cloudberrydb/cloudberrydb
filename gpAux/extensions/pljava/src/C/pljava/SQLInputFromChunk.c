/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include <postgres.h>
#include "pljava/SQLInputFromChunk.h"

#include "org_postgresql_pljava_jdbc_SQLInputFromChunk.h"

static jclass    s_SQLInputFromChunk_class;
static jmethodID s_SQLInputFromChunk_init;
static jmethodID s_SQLInputFromChunk_close;

jobject SQLInputFromChunk_create(void* data, size_t sz)
{
	Ptr2Long p2l;
	p2l.longVal = 0L; /* ensure that the rest is zeroed out */
	p2l.ptrVal = data;
	return JNI_newObject(s_SQLInputFromChunk_class, s_SQLInputFromChunk_init, p2l.longVal, (jint)sz);
}

void SQLInputFromChunk_close(jobject stream)
{
	JNI_callVoidMethod(stream, s_SQLInputFromChunk_close);
}

/* Make this datatype available to the postgres system.
 */
extern void SQLInputFromChunk_initialize(void);
void SQLInputFromChunk_initialize(void)
{
	JNINativeMethod methods[] = {
		{
		"_readByte",
	  	"(JI)I",
	  	Java_org_postgresql_pljava_jdbc_SQLInputFromChunk__1readByte
		},
		{
		"_readBytes",
	  	"(JI[BI)V",
	  	Java_org_postgresql_pljava_jdbc_SQLInputFromChunk__1readBytes
		},
		{ 0, 0, 0 }};

	s_SQLInputFromChunk_class = JNI_newGlobalRef(PgObject_getJavaClass("org/postgresql/pljava/jdbc/SQLInputFromChunk"));
	PgObject_registerNatives2(s_SQLInputFromChunk_class, methods);
	s_SQLInputFromChunk_init = PgObject_getJavaMethod(s_SQLInputFromChunk_class, "<init>", "(JI)V");
	s_SQLInputFromChunk_close = PgObject_getJavaMethod(s_SQLInputFromChunk_class, "close", "()V");
}

/****************************************
 * JNI methods
 ****************************************/
 
/*
 * Class:     org_postgresql_pljava_jdbc_SQLInputFromChunk
 * Method:    _readByte
 * Signature: (JI)I
 */
JNIEXPORT jint JNICALL
Java_org_postgresql_pljava_jdbc_SQLInputFromChunk__1readByte(JNIEnv* env, jclass cls, jlong _this, jint pos)
{
	Ptr2Long p2l;
	p2l.longVal = _this;

	/* Bounds checking has already been made */
	return (jint)((unsigned char*)p2l.ptrVal)[pos];
}

/*
 * Class:     org_postgresql_pljava_jdbc_SQLInputFromChunk
 * Method:    _readBytes
 * Signature: (JI[BI)V
 */
JNIEXPORT void JNICALL
Java_org_postgresql_pljava_jdbc_SQLInputFromChunk__1readBytes(JNIEnv* env, jclass cls, jlong _this, jint pos, jbyteArray ba, jint len)
{
	BEGIN_NATIVE
	Ptr2Long p2l;
	p2l.longVal = _this;
	/* Bounds checking has already been made */
	JNI_setByteArrayRegion(ba, 0, len, ((jbyte*)p2l.ptrVal) + pos);
	END_NATIVE
}
