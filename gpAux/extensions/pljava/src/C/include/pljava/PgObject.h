/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_PgObject_h
#define __pljava_PgObject_h

#include "pljava/JNICalls.h"

#ifdef __cplusplus
extern "C" {
#endif

/***********************************************************************
 * The PgObject class is the abstract base class for all classes in the
 * Pl/Java system. It provides the basic instance to class mapping,
 * a virtual destructor, and some convenience methods used when finding
 * Java classes and their members.
 * 
 * @author Thomas Hallgren
 *
 ***********************************************************************/
struct PgObject_;
typedef struct PgObject_* PgObject;

struct PgObjectClass_;
typedef struct PgObjectClass_* PgObjectClass;

/*
 * The effectiveClassPath is set at initialization time (in Backend.c)
 */
extern const char* effectiveClassPath;

/*
 * Calles the virtual finalizer and deallocates memory occupided by the
 * PgObject structure.
 */
extern void PgObject_free(PgObject object);

/*
 * Misc JNIEnv mappings.
 */
extern jobject PgObject_callObjectMethod(jobject object, jmethodID field, ...);
extern void PgObject_exceptionClear(void);
extern void PgObject_exceptionDescribe(void);
extern jint PgObject_getIntField(jobject object, jfieldID field);
extern jobject PgObject_newGlobalRef(jobject object);

/*
 * Obtains a java class. Calls elog(ERROR, ...) on failure so that
 * there is no return if the method fails.
 */
extern jclass PgObject_getJavaClass(const char* className);

/*
 * Obtains a java method. Calls elog(ERROR, ...) on failure so that
 * there is no return if the method fails.
 */
extern jmethodID PgObject_getJavaMethod(jclass cls, const char* methodName, const char* signature);

/*
 * Obtains a static java method. Calls elog(ERROR, ...) on failure so that
 * there is no return if the method fails.
 */
extern jmethodID PgObject_getStaticJavaMethod(jclass cls, const char* methodName, const char* signature);

/*
 * Obtain a HeapTuple from the system cache and throw an excption
 * on failure.
 */
extern HeapTuple PgObject_getValidTuple(int cacheId, Oid tupleId, const char* tupleType);

/*
 * Obtains a java field. Calls elog(ERROR, ...) on failure so that
 * there is no return if the method fails.
 */
extern jfieldID PgObject_getJavaField(jclass cls, const char* fieldName, const char* signature);

extern jobject PgObject_newJavaObject(jclass cls, jmethodID ctor, ...);

/*
 * Obtains a static java field. Calls elog(ERROR, ...) on failure so that
 * there is no return if the method fails.
 */
extern jfieldID PgObject_getStaticJavaField(jclass cls, const char* fieldName, const char* signature);

/*
 * Register native methods with a class. Last entry in the methods array must
 * have all values set to NULL.
 */
extern void PgObject_registerNatives(const char* className, JNINativeMethod* methods);

extern void PgObject_registerNatives2(jclass cls, JNINativeMethod* methods);

/*
 * Returns the class for the instance.
 */
extern PgObjectClass PgObject_getClass(PgObject self);

/*
 * Returns the name of the class
 */
extern const char* PgObjectClass_getName(PgObjectClass self);

#ifdef __cplusplus
}
#endif
#endif
