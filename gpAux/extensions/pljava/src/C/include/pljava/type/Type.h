/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_type_Type_h
#define __pljava_type_Type_h

#include "pljava/PgObject.h"

#ifdef __cplusplus
extern "C" {
#endif

#include <catalog/pg_type.h>

/*********************************************************************
 * The Type class is responsible for data type conversions between the
 * Postgres Datum and the Java jvalue. A Type can also perform optimized
 * JNI calls that are type dependent (returning primitives) such as
 * CallIntMethod(...) or CallBooleanMethod(...). Consequently, the Type
 * of the return value of a function is responsible for its invocation.
 *
 * Types that are not mapped will default to a java.lang.String mapping
 * and use the Form_pg_type text conversion routines.
 *
 * @author Thomas Hallgren
 *
 *********************************************************************/
struct Type_;
typedef struct Type_* Type;

struct TypeClass_;
typedef struct TypeClass_* TypeClass;

/*
 * Returns the TypeClass
 */
extern TypeClass Type_getClass(Type self);

/*
 * Returns true if the Type is primitive (i.e. not a real object in
 * the Java domain).
 */
extern bool Type_isPrimitive(Type self);

/*
 * Returns true if this type uses the same postgres type the other type.
 * This is used when explicit java signatures are declared functions to
 * verify that the declared Java type is compatible with the SQL type.
 *
 * At present, the type argument must be either equal to self, or if
 * self is a Boolean, Character, or any Number, the primitive that
 * corresponds to that number (i.e. java.lang.Short == short).
 */
extern bool Type_canReplaceType(Type self, Type type);

/*
 * Translate a given Datum into a jvalue accorging to the type represented
 * by this instance.
 */
extern jvalue Type_coerceDatum(Type self, Datum datum);

/*
 * Translate a given Object into a Datum accorging to the type represented
 * by this instance.
 */
extern Datum Type_coerceObject(Type self, jobject object);

/*
 * Return a Type based on a Postgres Oid. Creates a new type if
 * necessary.
 */
extern Type Type_fromOid(Oid typeId, jobject typeMap);

/*
 * Return a Type from the Oid cache based on a Postgres Oid. This method
 * returns NULL if no such Type is cached.
 */
extern Type Type_fromOidCache(Oid typeId);

/*
 * Return a coerce type that can front this type when doing parameter coercion
 */
extern Type Type_getCoerceIn(Type self, Type other);

/*
 * Return a coerce type that this type can hand over to when doing result value
 * coercion
 */
extern Type Type_getCoerceOut(Type self, Type other);

/*
 * Returns true if the type represents the dynamic (any) type.
 */
extern bool Type_isDynamic(Type self);

/*
 * Returns the type alignment (i.e. pg_type->typalign).
 */
extern char Type_getAlign(Type self);

/*
 * Returns the type length (i.e. pg_type->typlen).
 */
extern int16 Type_getLength(Type self);

/*
 * Returns the type length (i.e. pg_type->typlen).
 */
extern jclass Type_getJavaClass(Type self);

/*
 * Returns true if the type is passed by value (i.e. pg_type->typbyval).
 */
extern bool Type_isByValue(Type self);

/*
 * Returns true if the invocation will create an out parameter (ResultSet typically)
 * to collect the return value. If so, the real return value will be a bool.
 */
extern bool Type_isOutParameter(Type self);

/*
 * Returns the real type for a dynamic type. A non dynamic type will
 * return itself.
 */
extern Type Type_getRealType(Type self, Oid realTypeID, jobject typeMap);

/*
 * Return a Type based on a PostgreSQL Oid. If the found
 * type is a primitive, return it's object corresponcance
 */
extern Type Type_objectTypeFromOid(Oid typeId, jobject typeMap);

/*
 * Return a Type based on a default SQL type and a java type name.
 */
extern Type Type_fromJavaType(Oid dfltType, const char* javaTypeName);

/*
 * Returns the Java type name for the Type.
 */
extern const char* Type_getJavaTypeName(Type self);

/*
 * Returns the JNI signature for the Type.
 */
extern const char* Type_getJNISignature(Type self);

/*
 * Returns the JNI signature used when returning instances
 * of this type.
 */
extern const char* Type_getJNIReturnSignature(Type self, bool forMultiCall, bool useAltRepr);

/*
 * Returns the array Type. The type is created if it doesn't exist
 */
extern Type Type_getArrayType(Type self, Oid arrayTypeId);

/*
 * Returns the element Type if this type is an array.
 */
extern Type Type_getElementType(Type self);

/*
 * Returns the object Type if the type is primitive and NULL if not.
 */
extern Type Type_getObjectType(Type self);

/*
 * Returns the Oid associated with this type.
 */
extern Oid Type_getOid(Type self);

/*
 * Returns the TupleDesc associated with this type.
 */
extern TupleDesc Type_getTupleDesc(Type self, PG_FUNCTION_ARGS);

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
extern Datum Type_invoke(Type self, jclass clazz, jmethodID method, jvalue* args, PG_FUNCTION_ARGS);

/*
 * Calls a Set Returning Function (SRF).
 */
extern Datum Type_invokeSRF(Type self, jclass clazz, jmethodID method, jvalue* args, PG_FUNCTION_ARGS);

/*
 * Obtains the Java object that acts as the SRF producer. This instance will be
 * called once for each row that should be produced.
 */
extern jobject Type_getSRFProducer(Type self, jclass clazz, jmethodID method, jvalue* args);

/*
 * Obtains the optional Java object that will act as the value collector for
 * the SRF producer. The collector typically manifest itself as an OUT
 * parameter of type java.sql.ResultSet in calls to the SRF producer.
 */
extern jobject Type_getSRFCollector(Type self, PG_FUNCTION_ARGS);

/*
 * Called to determine if the producer will produce another row.
 */
extern bool Type_hasNextSRF(Type self, jobject producer, jobject collector, jint counter);

/*
 * Converts the next row into a Datum of the expected type.
 */
extern Datum Type_nextSRF(Type self, jobject producer, jobject collector);

/*
 * Called at the end of an SRF iteration.
 */
extern void Type_closeSRF(Type self, jobject producer);

/*
 * Function used when obtaining a type based on an Oid
 * structure. In most cases, this function should return a
 * singleton. The only current exception from this is the
 * String since it makes use of functions stored in the
 * Form_pg_type structure.
 */
typedef Type (*TypeObtainer)(Oid typeId);

/*
 * Function that can coerce a Datum into a jvalue
 */
typedef jvalue (*DatumCoercer)(Type, Datum);

/*
 * Function that can coerce a jobject into a Datum
 */
typedef Datum (*ObjectCoercer)(Type, jobject);

/*
 * Register this type as the default mapping for a postgres type.
 */
extern void Type_registerType(const char* javaTypeName, Type type);
extern void Type_registerType2(Oid typeId, const char* javaTypeName, TypeObtainer obtainer);

#ifdef __cplusplus
}
#endif
#endif
