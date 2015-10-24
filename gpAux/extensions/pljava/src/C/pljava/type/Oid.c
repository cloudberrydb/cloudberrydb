/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include <postgres.h>

#define Type PGType
#include <parser/parse_type.h>
#undef Type

#include "org_postgresql_pljava_internal_Oid.h"
#include "java_sql_Types.h"
#include "pljava/type/Type_priv.h"
#include "pljava/type/Oid.h"
#include "pljava/type/String.h"
#include "pljava/Exception.h"
#include "pljava/Invocation.h"

static jclass    s_Oid_class;
static jmethodID s_Oid_init;
static jmethodID s_Oid_registerType;
static jfieldID  s_Oid_m_native;
static jobject   s_OidOid;

/*
 * org.postgresql.pljava.type.Oid type.
 */
jobject Oid_create(Oid oid)
{
	jobject joid;
	if(OidIsValid(oid))
		joid = JNI_newObject(s_Oid_class, s_Oid_init, oid);
	else
		joid = 0;
	return joid;
}

Oid Oid_getOid(jobject joid)
{
	if(joid == 0)
		return InvalidOid;
	return ObjectIdGetDatum(JNI_getIntField(joid, s_Oid_m_native));
}

Oid Oid_forSqlType(int sqlType)
{
	Oid typeId;

	switch(sqlType)
	{
		case java_sql_Types_BIT:
			typeId = BITOID;
			break;
		case java_sql_Types_TINYINT:
			typeId = CHAROID;
			break;
		case java_sql_Types_SMALLINT:
			typeId = INT2OID;
			break;
		case java_sql_Types_INTEGER:
			typeId = INT4OID;
			break;
		case java_sql_Types_BIGINT:
			typeId = INT8OID;
			break;
		case java_sql_Types_FLOAT:
		case java_sql_Types_REAL:
			typeId = FLOAT4OID;
			break;
		case java_sql_Types_DOUBLE:
			typeId = FLOAT8OID;
			break;
		case java_sql_Types_NUMERIC:
		case java_sql_Types_DECIMAL:
			typeId = NUMERICOID;
			break;
		case java_sql_Types_DATE:
			typeId = DATEOID;
			break;
		case java_sql_Types_TIME:
			typeId = TIMEOID;
			break;
		case java_sql_Types_TIMESTAMP:
			typeId = TIMESTAMPOID;
			break;
		case java_sql_Types_BOOLEAN:
			typeId = BOOLOID;
			break;
		case java_sql_Types_BINARY:
		case java_sql_Types_VARBINARY:
		case java_sql_Types_LONGVARBINARY:
		case java_sql_Types_BLOB:
			typeId = BYTEAOID;
			break;
		case java_sql_Types_CHAR:
		case java_sql_Types_VARCHAR:
		case java_sql_Types_LONGVARCHAR:
		case java_sql_Types_CLOB:
		case java_sql_Types_DATALINK:
			typeId = TEXTOID;
			break;
/*		case java_sql_Types_NULL:
		case java_sql_Types_OTHER:
		case java_sql_Types_JAVA_OBJECT:
		case java_sql_Types_DISTINCT:
		case java_sql_Types_STRUCT:
		case java_sql_Types_ARRAY:
		case java_sql_Types_REF: */
		default:
			typeId = InvalidOid;	/* Not yet mapped */
			break;
	}
	return typeId;
}

static jvalue _Oid_coerceDatum(Type self, Datum arg)
{
	jvalue result;
	result.l = Oid_create(DatumGetObjectId(arg));
	return result;
}

static Datum _Oid_coerceObject(Type self, jobject oidObj)
{
	return Oid_getOid(oidObj);
}

/* Make this datatype available to the postgres system.
 */
extern void Oid_initialize(void);
void Oid_initialize(void)
{
	TypeClass cls;
	JNINativeMethod methods[] = {
		{
		"_forTypeName",
	  	"(Ljava/lang/String;)I",
	  	Java_org_postgresql_pljava_internal_Oid__1forTypeName
		},
		{
		"_forSqlType",
	  	"(I)I",
	  	Java_org_postgresql_pljava_internal_Oid__1forSqlType
		},
		{
		"_getTypeId",
		"()Lorg/postgresql/pljava/internal/Oid;",
		Java_org_postgresql_pljava_internal_Oid__1getTypeId
		},
		{
		"_getJavaClassName",
		"(I)Ljava/lang/String;",
	  	Java_org_postgresql_pljava_internal_Oid__1getJavaClassName
		},
		{ 0, 0, 0 }};

	jobject tmp;

	s_Oid_class = JNI_newGlobalRef(PgObject_getJavaClass("org/postgresql/pljava/internal/Oid"));
	PgObject_registerNatives2(s_Oid_class, methods);
	s_Oid_init = PgObject_getJavaMethod(s_Oid_class, "<init>", "(I)V");
	s_Oid_m_native = PgObject_getJavaField(s_Oid_class, "m_native", "I");

	cls = TypeClass_alloc("type.Oid");
	cls->JNISignature   = "Lorg/postgresql/pljava/internal/Oid;";
	cls->javaTypeName   = "org.postgresql.pljava.internal.Oid";
	cls->coerceDatum    = _Oid_coerceDatum;
	cls->coerceObject   = _Oid_coerceObject;
	Type_registerType("org.postgresql.pljava.internal.Oid", TypeClass_allocInstance(cls, OIDOID));

	tmp = Oid_create(OIDOID);
	s_OidOid = JNI_newGlobalRef(tmp);
	JNI_deleteLocalRef(tmp);

	s_Oid_registerType = PgObject_getStaticJavaMethod(
				s_Oid_class, "registerType",
				"(Ljava/lang/Class;Lorg/postgresql/pljava/internal/Oid;)V");

	JNI_callStaticVoidMethod(s_Oid_class, s_Oid_registerType, s_Oid_class, s_OidOid);
}

/*
 * Class:     org_postgresql_pljava_internal_Oid
 * Method:    _forSqlType
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL
Java_org_postgresql_pljava_internal_Oid__1forSqlType(JNIEnv* env, jclass cls, jint sqlType)
{
	Oid typeId = InvalidOid;
	BEGIN_NATIVE
	typeId = Oid_forSqlType(sqlType);
	if(typeId == InvalidOid)
		Exception_throw(ERRCODE_INTERNAL_ERROR, "No such SQL type: %d", (int)sqlType);
	END_NATIVE
	return typeId;
}

/*
 * Class:     org_postgresql_pljava_internal_Oid
 * Method:    _forTypeName
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL
Java_org_postgresql_pljava_internal_Oid__1forTypeName(JNIEnv* env, jclass cls, jstring typeString)
{
	Oid typeId = InvalidOid;
	BEGIN_NATIVE
	char* typeNameOrOid = String_createNTS(typeString);
	if(typeNameOrOid != 0)
	{
		PG_TRY();
		{
			int32 typmod = 0;
			parseTypeString(typeNameOrOid, &typeId, &typmod);
		}
		PG_CATCH();
		{
			Exception_throw_ERROR("parseTypeString");
		}
		PG_END_TRY();
		pfree(typeNameOrOid);
	}
	END_NATIVE
	return typeId;
}


/*
 * Class:     org_postgresql_pljava_internal_Oid
 * Method:    _getTypeId
 * Signature: ()Lorg/postgresql/pljava/internal/Oid;
 */
JNIEXPORT jobject JNICALL
Java_org_postgresql_pljava_internal_Oid__1getTypeId(JNIEnv* env, jclass cls)
{
	return s_OidOid;
}

/*
 * Class:     org_postgresql_pljava_internal_Oid
 * Method:    _getJavaClassName
 * Signature: (I)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL
Java_org_postgresql_pljava_internal_Oid__1getJavaClassName(JNIEnv* env, jclass cls, jint oid)
{
	jstring result = 0;
	BEGIN_NATIVE
	if(!OidIsValid((Oid)oid))
	{
		Exception_throw(ERRCODE_DATA_EXCEPTION, "Invalid OID \"%d\"", (int)oid);
	}
	else
	{
		Type type = Type_objectTypeFromOid((Oid)oid, Invocation_getTypeMap());
		result = String_createJavaStringFromNTS(Type_getJavaTypeName(type));
	}
	END_NATIVE
	return result;
}
