/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include <postgres.h>
#include "pljava/SQLOutputToChunk.h"

#include "org_postgresql_pljava_jdbc_SQLOutputToChunk.h"

static jclass    s_SQLOutputToChunk_class;
static jmethodID s_SQLOutputToChunk_init;
static jmethodID s_SQLOutputToChunk_close;

jobject SQLOutputToChunk_create(StringInfo data)
{
	Ptr2Long p2l;
	p2l.longVal = 0L; /* ensure that the rest is zeroed out */
	p2l.ptrVal = data;
	return JNI_newObject(s_SQLOutputToChunk_class, s_SQLOutputToChunk_init, p2l.longVal);
}

void SQLOutputToChunk_close(jobject stream)
{
	JNI_callVoidMethod(stream, s_SQLOutputToChunk_close);
}

/* Make this datatype available to the postgres system.
 */
extern void SQLOutputToChunk_initialize(void);
void SQLOutputToChunk_initialize(void)
{
	JNINativeMethod methods[] = {
		{
		"_writeByte",
	  	"(JI)V",
	  	Java_org_postgresql_pljava_jdbc_SQLOutputToChunk__1writeByte
		},
		{
		"_writeBytes",
	  	"(J[BI)V",
	  	Java_org_postgresql_pljava_jdbc_SQLOutputToChunk__1writeBytes
		},
		{ 0, 0, 0 }};

	s_SQLOutputToChunk_class = JNI_newGlobalRef(PgObject_getJavaClass("org/postgresql/pljava/jdbc/SQLOutputToChunk"));
	PgObject_registerNatives2(s_SQLOutputToChunk_class, methods);
	s_SQLOutputToChunk_init = PgObject_getJavaMethod(s_SQLOutputToChunk_class, "<init>", "(J)V");
	s_SQLOutputToChunk_close = PgObject_getJavaMethod(s_SQLOutputToChunk_class, "close", "()V");
}

/****************************************
 * JNI methods
 ****************************************/

/*
 * Class:     org_postgresql_pljava_jdbc_SQLOutputToChunk
 * Method:    _writeByte
 * Signature: (JI)V
 */
JNIEXPORT void JNICALL
Java_org_postgresql_pljava_jdbc_SQLOutputToChunk__1writeByte(JNIEnv* env, jclass cls, jlong _this, jint b)
{
	Ptr2Long p2l;
	unsigned char byte = (unsigned char)b;
	p2l.longVal = _this;

	BEGIN_NATIVE
	appendBinaryStringInfo((StringInfo)p2l.ptrVal, (char*)&byte, 1);
	END_NATIVE
}

/*
 * Class:     org_postgresql_pljava_internal_MemoryChunkInputStream
 * Method:    _readBytes
 * Signature: (JI[BII)V
 */
#define BYTE_BUF_SIZE 1024
JNIEXPORT void JNICALL
Java_org_postgresql_pljava_jdbc_SQLOutputToChunk__1writeBytes(JNIEnv* env, jclass cls, jlong _this, jbyteArray ba, jint len)
{
	Ptr2Long p2l;
	jbyte buffer[BYTE_BUF_SIZE];
	int off = 0;
	p2l.longVal = _this;

	BEGIN_NATIVE
	while(len > 0)
	{
		int copySize = len;
		if(copySize > BYTE_BUF_SIZE)
			copySize = BYTE_BUF_SIZE;
		JNI_getByteArrayRegion(ba, off, copySize, buffer);
		appendBinaryStringInfo((StringInfo)p2l.ptrVal, (const char*)buffer, copySize);
		off += copySize;
		len -= copySize;
	}
	END_NATIVE
}
