/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include "org_postgresql_pljava_internal_JavaWrapper.h"
#include "pljava/type/Type_priv.h"
#include "pljava/type/JavaWrapper.h"
#include "pljava/Backend.h"
#include "pljava/Exception.h"

static jclass    s_JavaWrapper_class;
static jfieldID  s_JavaWrapper_m_pointer;

MemoryContext JavaMemoryContext;

static jlong _getPointer(jobject managed)
{
	if(managed == 0)
	{
		Exception_throw(ERRCODE_INTERNAL_ERROR, "Null JavaWrapper object");
		return 0;
	}

	return JNI_getLongField(managed, s_JavaWrapper_m_pointer);
}

static Datum _JavaWrapper_coerceObject(Type self, jobject nStruct)
{
	Ptr2Long p2l;
	p2l.longVal = _getPointer(nStruct);
	return PointerGetDatum(p2l.ptrVal);
}

TypeClass JavaWrapperClass_alloc(const char* name)
{
	TypeClass self = TypeClass_alloc(name);
	self->coerceObject = _JavaWrapper_coerceObject;
	return self;
}

extern void JavaWrapper_initialize(void);
void JavaWrapper_initialize(void)
{
	JNINativeMethod methods[] =
	{
		{
		"_free",
	  	"(J)V",
	  	Java_org_postgresql_pljava_internal_JavaWrapper__1free
		},
		{ 0, 0, 0 }
	};

	s_JavaWrapper_class = JNI_newGlobalRef(PgObject_getJavaClass("org/postgresql/pljava/internal/JavaWrapper"));
	PgObject_registerNatives2(s_JavaWrapper_class, methods);
	s_JavaWrapper_m_pointer = PgObject_getJavaField(s_JavaWrapper_class, "m_pointer", "J");

	JavaMemoryContext = AllocSetContextCreate(TopMemoryContext,
									 "PL/Java",
									 ALLOCSET_DEFAULT_MINSIZE,
									 ALLOCSET_DEFAULT_INITSIZE,
									 ALLOCSET_DEFAULT_MAXSIZE);
}

/*
 * Class:     org_postgresql_pljava_internal_JavaWrapper
 * Method:    _free
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_postgresql_pljava_internal_JavaWrapper__1free(JNIEnv* env, jobject _this, jlong pointer)
{
	BEGIN_NATIVE_NO_ERRCHECK
	Ptr2Long p2l;
	p2l.longVal = pointer;
	pfree(p2l.ptrVal);
	END_NATIVE
}
