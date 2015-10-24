/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_type_Type_priv_h
#define __pljava_type_Type_priv_h

#include "pljava/PgObject_priv.h"
#include "pljava/HashMap.h"
#include "pljava/type/Type.h"

#ifdef __cplusplus
extern "C" {
#endif

/* This is the "abstract" Type class. The Type is responsible for
 * value coercsions between Java types and PostGreSQL types.
 * 
 * @author Thomas Hallgren
 */
struct TypeClass_
{
	struct PgObjectClass_ extendedClass;

	/*
	 * Contains the JNI compliant signature for the type.
	 */
	const char* JNISignature;

	/*
	 * Contains the Java type name.
	 */
	const char* javaTypeName;

	/*
	 * The Java class that represents this type. Will be NULL for
	 * primitive types.
	 */
	jclass javaClass;

	/*
	 * Set to true if this type represents a dynamic type (anyelement or
	 * collection/iterator of anyelement)
	 */
	bool dynamic;

	/*
	 * Set to true if the invocation will create an out parameter (ResultSet typically)
	 * to collect the return value. If so, the real return value will be a bool.
	 */
	bool outParameter;

	/*
	 * Creates the array type for this type.
	 */
	Type (*createArrayType)(Type self, Oid arrayType);

	/*
	 * Returns the real type for a dynamic type. A non dynamic type will
	 * return itself.
	 */
	Type (*getRealType)(Type self, Oid realTypeID, jobject typeMap);

	/*
	 * Returns true if this type uses the same postgres type the other type.
	 * This is used when explicit java signatures are declared functions to
	 * verify that the declared Java type is compatible with the SQL type.
	 * 
	 * At present, the type argument must be either equal to self, or if
	 * self is a Boolean, Character, or any Number, the primitive that
	 * corresponds to that number (i.e. java.lang.Short == short).
	 */
	bool (*canReplaceType)(Type self, Type type);

	/*
	 * Translate a given Datum into a jvalue accorging to the type represented
	 * by this instance.
	 */
	DatumCoercer coerceDatum;

	/*
	 * Translate a given Object into a Datum accorging to the type represented
	 * by this instance.
	 */
	ObjectCoercer coerceObject;

	/*
	 * Calls a java method using one of the Call<type>MethodA routines where
	 * <type> corresponds to the type represented by this instance and
	 * coerces the returned value into a Datum.
	 * 
	 * The method will set the value pointed to by the wasNull parameter
	 * to true if the Java method returned null. The method expects that
	 * the wasNull parameter is set to false by the caller prior to the
	 * call.
	 */
	Datum (*invoke)(Type self, jclass clazz, jmethodID method, jvalue* args, PG_FUNCTION_ARGS);

	jobject (*getSRFProducer)(Type self, jclass clazz, jmethodID method, jvalue* args);
	jobject (*getSRFCollector)(Type self, PG_FUNCTION_ARGS);
	bool (*hasNextSRF)(Type self, jobject producer, jobject collector, jint counter);
	Datum (*nextSRF)(Type self, jobject producer, jobject collector);
	void (*closeSRF)(Type self, jobject producer);
	const char* (*getJNISignature)(Type self);
	const char* (*getJNIReturnSignature)(Type self, bool forMultiCall, bool useAltRepr);

	/*
	 * Returns the TupleDesc that corresponds to this type.
	 */
	TupleDesc (*getTupleDesc)(Type self, PG_FUNCTION_ARGS);
};

struct Type_
{
	TypeClass typeClass;

	/*
	 * The Oid that identifies this type.
	 */
	Oid       typeId;

	/*
	 * Points to the array type where this type is the element type.
	 * If the type has no corresponding array type, this type will be NULL.
	 */
	Type  arrayType;

	/*
	 * If the type is an array type, this is the element type.
	 */
	Type  elementType;

	/*
	 * Points to the object type that corresponds to this type
	 * if this type is a primitive. For non primitives, this attribute
	 * will be NULL.
	 */
	Type  objectType;

	/*
	 * Oid keyed hash map of coercion routines that can front this type when doing
	 * parameter input coercion.
	 */
	HashMap inCoercions;

	/*
	 * Oid keyed hash map of coercion routines that can front this type when doing
	 * coercion of output results.
	 */
	HashMap outCoercions;

	int16 length;
	bool  byValue;
	char  align;
};

/*
 * Default version of canReplaceType. Returns true when
 * self and other are equal.
 */
extern bool _Type_canReplaceType(Type self, Type other);

/*
 * Default version of invoke. Will make a JNI CallObjectMethod call and then
 * a call to self->coerceObject to create the Datum.
 */
extern Datum _Type_invoke(Type self, jclass cls, jmethodID method, jvalue* args, PG_FUNCTION_ARGS);

/*
 * Return the m_oid member of the Type. This is the default version of
 * Type_getTupleDesc.
 */
extern TupleDesc _Type_getTupleDesc(Type self, PG_FUNCTION_ARGS);

/*
 * Store a Type keyed by its Oid in the cache.
 */
extern void Type_cacheByOid(Oid typeId, Type type);

/*
 * Create a TypeClass with default sizes for TypeClass and Type.
 */
extern TypeClass TypeClass_alloc(const char* className);

/*
 * Create a TypeClass for a specific TypeClass size and a specific Type size.
 */
extern TypeClass TypeClass_alloc2(const char* className, Size classSize, Size instanceSize);

/*
 * Types are always allocated in global context.
 */
extern Type TypeClass_allocInstance(TypeClass cls, Oid typeId);
extern Type TypeClass_allocInstance2(TypeClass cls, Oid typeId, Form_pg_type pgType);

#ifdef __cplusplus
}
#endif
#endif
/*
Yet to implement
LOG:  Type name = 'abstime'
LOG:  Type name = 'box'
LOG:  Type name = 'cid'
LOG:  Type name = 'lseg'
LOG:  Type name = 'path'
LOG:  Type name = 'point'
LOG:  Type name = 'reltime'
LOG:  Type name = 'tid'
LOG:  Type name = 'tinterval'
LOG:  Type name = 'xid'
*/
