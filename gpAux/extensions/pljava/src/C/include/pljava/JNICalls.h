/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_JNICalls_h
#define __pljava_JNICalls_h

#include "pljava/pljava.h"

#ifdef __cplusplus
extern "C" {
#endif

#define BEGIN_NATIVE_NO_ERRCHECK if(beginNativeNoErrCheck(env)) {
#define BEGIN_NATIVE if(beginNative(env)) {
#define END_NATIVE JNI_setEnv(0); }

/***********************************************************************
 * All calls to and from the JVM uses this header. The calls are implemented
 * using a fence mechanism that prevents multiple threads to access
 * the backend simultaniously.
 * 
 * @author Thomas Hallgren
 *
 ***********************************************************************/

/*
 * Entry guards for when the JVM calls into native code
 */
extern bool beginNative(JNIEnv* env);
extern bool beginNativeNoErrCheck(JNIEnv* env);

extern jclass    ServerException_class;
extern jmethodID ServerException_getErrorData;
extern jmethodID ServerException_init;

extern jclass    Class_class;
extern jmethodID Class_getName;

extern jclass    Throwable_class;
extern jmethodID Throwable_getMessage;
extern jmethodID Throwable_printStackTrace;

extern jclass    IllegalArgumentException_class;
extern jmethodID IllegalArgumentException_init;

extern jclass    SQLException_class;
extern jmethodID SQLException_init;
extern jmethodID SQLException_getSQLState;

extern jclass    UnsupportedOperationException_class;
extern jmethodID UnsupportedOperationException_init;

/*
 * Misc JNIEnv mappings. See <jni.h> for more info.
 */
extern jboolean     JNI_callBooleanMethod(jobject object, jmethodID methodID, ...);
extern jboolean     JNI_callBooleanMethodV(jobject object, jmethodID methodID, va_list args);
extern jbyte        JNI_callByteMethod(jobject object, jmethodID methodID, ...);
extern jbyte        JNI_callByteMethodV(jobject object, jmethodID methodID, va_list args);
extern jdouble      JNI_callDoubleMethod(jobject object, jmethodID methodID, ...);
extern jdouble      JNI_callDoubleMethodV(jobject object, jmethodID methodID, va_list args);
extern jfloat       JNI_callFloatMethod(jobject object, jmethodID methodID, ...);
extern jfloat       JNI_callFloatMethodV(jobject object, jmethodID methodID, va_list args);
extern jint         JNI_callIntMethod(jobject object, jmethodID methodID, ...);
extern jint         JNI_callIntMethodV(jobject object, jmethodID methodID, va_list args);
extern jlong        JNI_callLongMethod(jobject object, jmethodID methodID, ...);
extern jlong        JNI_callLongMethodV(jobject object, jmethodID methodID, va_list args);
extern jobject      JNI_callObjectMethod(jobject object, jmethodID methodID, ...);
extern jobject      JNI_callObjectMethodV(jobject object, jmethodID methodID, va_list args);
extern jshort       JNI_callShortMethod(jobject object, jmethodID methodID, ...);
extern jshort       JNI_callShortMethodV(jobject object, jmethodID methodID, va_list args);
extern jboolean     JNI_callStaticBooleanMethodA(jclass clazz, jmethodID methodID, jvalue* args);
extern jbyte        JNI_callStaticByteMethodA(jclass clazz, jmethodID methodID, jvalue* args);
extern jdouble      JNI_callStaticDoubleMethodA(jclass clazz, jmethodID methodID, jvalue* args);
extern jfloat       JNI_callStaticFloatMethodA(jclass clazz, jmethodID methodID, jvalue* args);
extern jint         JNI_callStaticIntMethodA(jclass clazz, jmethodID methodID, jvalue* args);
extern jlong        JNI_callStaticLongMethod(jclass clazz, jmethodID methodID, ...);
extern jlong        JNI_callStaticLongMethodA(jclass clazz, jmethodID methodID, jvalue* args);
extern jlong        JNI_callStaticLongMethodV(jclass clazz, jmethodID methodID, va_list args);
extern jobject      JNI_callStaticObjectMethod(jclass clazz, jmethodID methodID, ...);
extern jobject      JNI_callStaticObjectMethodA(jclass clazz, jmethodID methodID, jvalue* args);
extern jobject      JNI_callStaticObjectMethodV(jclass clazz, jmethodID methodID, va_list args);
extern jshort       JNI_callStaticShortMethodA(jclass clazz, jmethodID methodID, jvalue* args);
extern void         JNI_callStaticVoidMethod(jclass clazz, jmethodID methodID, ...);
extern void         JNI_callStaticVoidMethodA(jclass clazz, jmethodID methodID, jvalue* args);
extern void         JNI_callStaticVoidMethodV(jclass clazz, jmethodID methodID, va_list args);
extern void         JNI_callVoidMethod(jobject object, jmethodID methodID, ...);
extern void         JNI_callVoidMethodV(jobject object, jmethodID methodID, va_list args);
extern jint         JNI_createVM(JavaVM** javaVM, JavaVMInitArgs* vmArgs);
extern void         JNI_deleteGlobalRef(jobject object);
extern void         JNI_deleteLocalRef(jobject object);
extern void         JNI_deleteWeakGlobalRef(jweak object);
extern jint         JNI_destroyVM(JavaVM *vm);
extern jboolean     JNI_exceptionCheck(void);
extern void         JNI_exceptionClear(void);
extern void         JNI_exceptionDescribe(void);
extern jthrowable   JNI_exceptionOccurred(void);
extern jclass       JNI_findClass(const char* className);
extern jsize        JNI_getArrayLength(jarray array);
extern jbyte*       JNI_getByteArrayElements(jbyteArray array, jboolean* isCopy);
extern void         JNI_getByteArrayRegion(jbyteArray array, jsize start, jsize len, jbyte* buf);
extern jboolean*    JNI_getBooleanArrayElements(jbooleanArray array, jboolean* isCopy);
extern void         JNI_getBooleanArrayRegion(jbooleanArray array, jsize start, jsize len, jboolean* buf);
extern jfieldID     JNI_getFieldID(jclass clazz, const char* name, const char* sig);
extern jdouble*     JNI_getDoubleArrayElements(jdoubleArray array, jboolean* isCopy);
extern void         JNI_getDoubleArrayRegion(jdoubleArray array, jsize start, jsize len, jdouble* buf);
extern jfloat*      JNI_getFloatArrayElements(jfloatArray array, jboolean* isCopy);
extern void         JNI_getFloatArrayRegion(jfloatArray array, jsize start, jsize len, jfloat* buf);
extern jint*        JNI_getIntArrayElements(jintArray array, jboolean* isCopy);
extern void         JNI_getIntArrayRegion(jintArray array, jsize start, jsize len, jint* buf);
extern jint         JNI_getIntField(jobject object, jfieldID field);
extern jlong*       JNI_getLongArrayElements(jlongArray array, jboolean* isCopy);
extern void         JNI_getLongArrayRegion(jlongArray array, jsize start, jsize len, jlong* buf);
extern jlong        JNI_getLongField(jobject object, jfieldID field);
extern jmethodID    JNI_getMethodID(jclass clazz, const char* name, const char* sig);
extern jobject      JNI_getObjectArrayElement(jobjectArray array, jsize index);
extern jclass       JNI_getObjectClass(jobject obj);
extern jshort*      JNI_getShortArrayElements(jshortArray array, jboolean* isCopy);
extern void         JNI_getShortArrayRegion(jshortArray array, jsize start, jsize len, jshort* buf);
extern jfieldID     JNI_getStaticFieldID(jclass clazz, const char* name, const char* sig);
extern jmethodID    JNI_getStaticMethodID(jclass clazz, const char* name, const char* sig);
extern jmethodID    JNI_getStaticMethodIDOrNull(jclass clazz, const char* name, const char* sig);
extern jobject      JNI_getStaticObjectField(jclass clazz, jfieldID field);
extern const char*  JNI_getStringUTFChars(jstring string, jboolean* isCopy);
extern jboolean     JNI_hasNullArrayElement(jobjectArray array);
extern jboolean     JNI_isCallingJava(void);
extern jboolean     JNI_isInstanceOf(jobject obj, jclass clazz);
extern jbyteArray   JNI_newByteArray(jsize length);
extern jbooleanArray JNI_newBooleanArray(jsize length);
extern jobject      JNI_newDirectByteBuffer(void* address, jlong capacity);
extern jdoubleArray JNI_newDoubleArray(jsize length);
extern jfloatArray  JNI_newFloatArray(jsize length);
extern jobject      JNI_newGlobalRef(jobject object);
extern jintArray    JNI_newIntArray(jsize length);
extern jobject      JNI_newLocalRef(jobject object);
extern jlongArray   JNI_newLongArray(jsize length);
extern jobject      JNI_newObject(jclass clazz, jmethodID ctor, ...);
extern jobject      JNI_newObjectV(jclass clazz, jmethodID ctor, va_list args);
extern jobjectArray JNI_newObjectArray(jsize length, jclass elementClass, jobject initialElement);
extern jshortArray  JNI_newShortArray(jsize length);
extern jstring      JNI_newStringUTF(const char* bytes);
extern jobject      JNI_newWeakGlobalRef(jobject object);
extern jint         JNI_pushLocalFrame(jint capacity);
extern jobject      JNI_popLocalFrame(jobject result);
extern jint         JNI_registerNatives(jclass clazz, const JNINativeMethod* methods, jint nMethods);
extern void         JNI_releaseByteArrayElements(jbyteArray array, jbyte* elems, jint mode);
extern void         JNI_releaseBooleanArrayElements(jbooleanArray array, jboolean* elems, jint mode);
extern void         JNI_releaseDoubleArrayElements(jdoubleArray array, jdouble* elems, jint mode);
extern void         JNI_releaseFloatArrayElements(jfloatArray array, jfloat* elems, jint mode);
extern void         JNI_releaseIntArrayElements(jintArray array, jint* elems, jint mode);
extern void         JNI_releaseLongArrayElements(jlongArray array, jlong* elems, jint mode);
extern void         JNI_releaseShortArrayElements(jshortArray array, jshort* elems, jint mode);
extern void         JNI_releaseStringUTFChars(jstring string, const char *utf);
extern void         JNI_setByteArrayRegion(jbyteArray array, jsize start, jsize len, jbyte* buf);
extern void         JNI_setBooleanArrayRegion(jbooleanArray array, jsize start, jsize len, jboolean* buf);
extern JNIEnv*      JNI_setEnv(JNIEnv* env);
extern void         JNI_setDoubleArrayRegion(jdoubleArray array, jsize start, jsize len, jdouble* buf);
extern void         JNI_setFloatArrayRegion(jfloatArray array, jsize start, jsize len, jfloat* buf);
extern void         JNI_setIntArrayRegion(jintArray array, jsize start, jsize len, jint* buf);
extern void         JNI_setLongArrayRegion(jlongArray array, jsize start, jsize len, jlong* buf);
extern void         JNI_setShortArrayRegion(jshortArray array, jsize start, jsize len, jshort* buf);
extern void         JNI_setLongField(jobject object, jfieldID field, jlong value);
extern void         JNI_setObjectArrayElement(jobjectArray array, jsize index, jobject value);
extern void			JNI_setThreadLock(jobject lockObject);
extern jint         JNI_throw(jthrowable obj);

#ifdef __cplusplus
}
#endif
#endif
