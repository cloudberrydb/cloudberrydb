/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include <postgres.h>
#include <executor/spi.h>

#include "pljava/PgObject_priv.h"
#include "pljava/type/String.h"

static bool      s_loopLock = false;
static jclass    s_Class_class = 0;
static jmethodID s_Class_getName = 0;

/* effectiveClassPath is set at initialization time (in Backend.c)
 */
const char* effectiveClassPath;

void PgObject_free(PgObject object)
{
	Finalizer finalizer = object->m_class->finalize;
	if(finalizer != 0)
		finalizer(object);
	pfree(object);
}

PgObject PgObjectClass_allocInstance(PgObjectClass clazz, MemoryContext ctx)
{
	Size sz = clazz->instanceSize;
	PgObject infant = (PgObject)MemoryContextAlloc(ctx, sz);
	memset(infant, 0, sz);
	infant->m_class = clazz;
	return infant;
}

void PgObjectClass_init(PgObjectClass clazz, const char* name, Size instanceSize, Finalizer finalizer)
{
	clazz->name = name;
	clazz->instanceSize = instanceSize;
	clazz->finalize = finalizer;
}

PgObjectClass PgObjectClass_create(const char* name, Size instanceSize, Finalizer finalizer)
{
	PgObjectClass self = (PgObjectClass)MemoryContextAlloc(TopMemoryContext, sizeof(struct PgObjectClass_));
	memset(self, 0, sizeof(struct PgObjectClass_));
	PgObjectClass_init(self, name, instanceSize, finalizer);
	return self;
}

PgObjectClass PgObject_getClass(PgObject self)
{
	return self->m_class;
}

const char* PgObjectClass_getName(PgObjectClass self)
{
	return self->name;
}

void _PgObject_pureVirtualCalled(PgObject object)
{
	ereport(ERROR, (errmsg("Pure virtual method called")));
}

static char* PgObject_getClassName(jclass cls)
{
	jstring jstr;
	char* tmp;

	if(s_Class_getName == 0)
	{
		if(s_loopLock)
			return "<exception while obtaining Class.getName()>";
		s_loopLock = true;
		s_Class_class = (jclass)JNI_newGlobalRef(PgObject_getJavaClass("java/lang/Class"));
		s_Class_getName = PgObject_getJavaMethod(s_Class_class, "getName", "()Ljava/lang/String;");
		s_loopLock = false;
	}

	jstr = (jstring)JNI_callObjectMethod(cls, s_Class_getName);
	tmp = String_createNTS(jstr);
	JNI_deleteLocalRef(jstr);
	return tmp;
}

void PgObject_throwMemberError(jclass cls, const char* memberName, const char* signature, bool isMethod, bool isStatic)
{
	JNI_exceptionDescribe();
	JNI_exceptionClear();
	ereport(ERROR, (
		errmsg("Unable to find%s %s %s.%s with signature %s",
			(isStatic ? " static" : ""),
			(isMethod ? "method" : "field"),
			PgObject_getClassName(cls),
			memberName,
			signature)));
}

jclass PgObject_getJavaClass(const char* className)
{
	jclass cls = JNI_findClass(className);
	if(cls == 0)
	{
		if(JNI_exceptionCheck())
		{
			JNI_exceptionDescribe();
			JNI_exceptionClear();
		}
		ereport(ERROR, (
			errmsg("Unable to load class %s using CLASSPATH '%s'",
				className, effectiveClassPath == 0 ? "null" : effectiveClassPath)));
	}
	return cls;
}

void PgObject_registerNatives(const char* className, JNINativeMethod* methods)
{
	jclass cls = PgObject_getJavaClass(className);
	PgObject_registerNatives2(cls, methods);
	JNI_deleteLocalRef(cls);
}

void PgObject_registerNatives2(jclass cls, JNINativeMethod* methods)
{
#ifndef GCJ
	jint nMethods = 0;
	JNINativeMethod* m = methods;
	while(m->name != 0)
	{
		m++;
		nMethods++;
	}

	if(JNI_registerNatives(cls, methods, nMethods) != 0)
	{
		JNI_exceptionDescribe();
		JNI_exceptionClear();
		ereport(ERROR, (
			errmsg("Unable to register native methods")));
	}
#endif
}

jmethodID PgObject_getJavaMethod(jclass cls, const char* methodName, const char* signature)
{
	jmethodID m = JNI_getMethodID(cls, methodName, signature);
	if(m == 0)
		PgObject_throwMemberError(cls, methodName, signature, true, false);
	return m;
}

jmethodID PgObject_getStaticJavaMethod(jclass cls, const char* methodName, const char* signature)
{
	jmethodID m = JNI_getStaticMethodID(cls, methodName, signature);
	if(m == 0)
		PgObject_throwMemberError(cls, methodName, signature, true, true);
	return m;
}

jfieldID PgObject_getJavaField(jclass cls, const char* fieldName, const char* signature)
{
	jfieldID m = JNI_getFieldID(cls, fieldName, signature);
	if(m == 0)
		PgObject_throwMemberError(cls, fieldName, signature, false, false);
	return m;
}

jfieldID PgObject_getStaticJavaField(jclass cls, const char* fieldName, const char* signature)
{
	jfieldID m = JNI_getStaticFieldID(cls, fieldName, signature);
	if(m == 0)
		PgObject_throwMemberError(cls, fieldName, signature, false, true);
	return m;
}

HeapTuple PgObject_getValidTuple(int cacheId, Oid tupleId, const char* tupleType)
{
	HeapTuple tuple = SearchSysCache(cacheId, ObjectIdGetDatum(tupleId), 0, 0, 0);
	if(!HeapTupleIsValid(tuple))
		ereport(ERROR, (errmsg("cache lookup failed for %s %u", tupleType, tupleId)));
	return tuple;
}
