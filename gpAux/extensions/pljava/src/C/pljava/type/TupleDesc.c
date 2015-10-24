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
#include <funcapi.h>

#include "org_postgresql_pljava_internal_TupleDesc.h"
#include "pljava/Backend.h"
#include "pljava/Exception.h"
#include "pljava/Invocation.h"
#include "pljava/type/Type_priv.h"
#include "pljava/type/String.h"
#include "pljava/type/Tuple.h"
#include "pljava/type/TupleDesc.h"
#include "pljava/type/Oid.h"

static jclass    s_TupleDesc_class;
static jmethodID s_TupleDesc_init;

/*
 * org.postgresql.pljava.TupleDesc type.
 */
jobject TupleDesc_create(TupleDesc td)
{
	jobject jtd = 0;
	if(td != 0)
	{
		MemoryContext curr = MemoryContextSwitchTo(JavaMemoryContext);
		jtd = TupleDesc_internalCreate(td);
		MemoryContextSwitchTo(curr);
	}
	return jtd;
}

jobject TupleDesc_internalCreate(TupleDesc td)
{
	jobject jtd;
	Ptr2Long tdH;

	td = CreateTupleDescCopyConstr(td);
	tdH.longVal = 0L; /* ensure that the rest is zeroed out */
	tdH.ptrVal = td;
	jtd = JNI_newObject(s_TupleDesc_class, s_TupleDesc_init, tdH.longVal, (jint)td->natts);
	return jtd;
}

Type TupleDesc_getColumnType(TupleDesc tupleDesc, int index)
{
	Type type;
	Oid typeId = SPI_gettypeid(tupleDesc, index);
	if(!OidIsValid(typeId))
	{
		Exception_throw(ERRCODE_INVALID_DESCRIPTOR_INDEX,
			"Invalid attribute index \"%d\"", (int)index);
		type = 0;
	}
	else
		type = Type_objectTypeFromOid(typeId, Invocation_getTypeMap());
	return type;
}

static jvalue _TupleDesc_coerceDatum(Type self, Datum arg)
{
	jvalue result;
	result.l = TupleDesc_create((TupleDesc)DatumGetPointer(arg));
	return result;
}

/* Make this datatype available to the postgres system.
 */
extern void TupleDesc_initialize(void);
void TupleDesc_initialize(void)
{
	TypeClass cls;
	JNINativeMethod methods[] = {
		{
		"_getColumnName",
	  	"(JI)Ljava/lang/String;",
	  	Java_org_postgresql_pljava_internal_TupleDesc__1getColumnName
		},
		{
		"_getColumnIndex",
		"(JLjava/lang/String;)I",
		Java_org_postgresql_pljava_internal_TupleDesc__1getColumnIndex
		},
		{
		"_formTuple",
		"(J[Ljava/lang/Object;)Lorg/postgresql/pljava/internal/Tuple;",
		Java_org_postgresql_pljava_internal_TupleDesc__1formTuple
		},
		{
		"_getOid",
		"(JI)Lorg/postgresql/pljava/internal/Oid;",
		Java_org_postgresql_pljava_internal_TupleDesc__1getOid
		},
		{
		"_free",
		"(J)V",
		Java_org_postgresql_pljava_internal_TupleDesc__1free
		},
		{ 0, 0, 0 }};

	s_TupleDesc_class = JNI_newGlobalRef(PgObject_getJavaClass("org/postgresql/pljava/internal/TupleDesc"));
	PgObject_registerNatives2(s_TupleDesc_class, methods);
	s_TupleDesc_init = PgObject_getJavaMethod(s_TupleDesc_class, "<init>", "(JI)V");

	cls = JavaWrapperClass_alloc("type.TupleDesc");
	cls->JNISignature = "Lorg/postgresql/pljava/internal/TupleDesc;";
	cls->javaTypeName = "org.postgresql.pljava.internal.TupleDesc";
	cls->coerceDatum  = _TupleDesc_coerceDatum;
	Type_registerType("org.postgresql.pljava.internal.TupleDesc", TypeClass_allocInstance(cls, InvalidOid));
}

/****************************************
 * JNI methods
 ****************************************/

/*
 * Class:     org_postgresql_pljava_internal_TupleDesc
 * Method:    _getColumnName
 * Signature: (JI)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL
Java_org_postgresql_pljava_internal_TupleDesc__1getColumnName(JNIEnv* env, jclass cls, jlong _this, jint index)
{
	jstring result = 0;

	BEGIN_NATIVE
	PG_TRY();
	{
		char* name;
		Ptr2Long p2l;
		p2l.longVal = _this;
		name = SPI_fname((TupleDesc)p2l.ptrVal, (int)index);
		if(name == 0)
		{
			Exception_throw(ERRCODE_INVALID_DESCRIPTOR_INDEX,
				"Invalid attribute index \"%d\"", (int)index);
		}
		else
		{
			result = String_createJavaStringFromNTS(name);
			pfree(name);
		}
	}
	PG_CATCH();
	{
		Exception_throw_ERROR("SPI_fname");
	}
	PG_END_TRY();
	END_NATIVE
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_TupleDesc
 * Method:    _getColumnIndex
 * Signature: (JLjava/lang/String;)I;
 */
JNIEXPORT jint JNICALL
Java_org_postgresql_pljava_internal_TupleDesc__1getColumnIndex(JNIEnv* env, jclass cls, jlong _this, jstring colName)
{
	jint result = 0;

	BEGIN_NATIVE
	char* name = String_createNTS(colName);
	if(name != 0)
	{
		Ptr2Long p2l;
		p2l.longVal = _this;
		PG_TRY();
		{
			result = SPI_fnumber((TupleDesc)p2l.ptrVal, name);
			if(result == SPI_ERROR_NOATTRIBUTE)
			{
				Exception_throw(ERRCODE_UNDEFINED_COLUMN,
					"Tuple has no attribute \"%s\"", name);
			}
			pfree(name);
		}
		PG_CATCH();
		{
			Exception_throw_ERROR("SPI_fnumber");
		}
		PG_END_TRY();
	}
	END_NATIVE
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_TupleDesc
 * Method:    _formTuple
 * Signature: (J[Ljava/lang/Object;)Lorg/postgresql/pljava/internal/Tuple;
 */
JNIEXPORT jobject JNICALL
Java_org_postgresql_pljava_internal_TupleDesc__1formTuple(JNIEnv* env, jclass cls, jlong _this, jobjectArray jvalues)
{
	jobject result = 0;

	BEGIN_NATIVE
	Ptr2Long p2l;
	p2l.longVal = _this;
	PG_TRY();
	{
		jint   idx;
		HeapTuple tuple;
		MemoryContext curr;
		TupleDesc self = (TupleDesc)p2l.ptrVal;
		int    count   = self->natts;
		Datum* values  = (Datum*)palloc(count * sizeof(Datum));
		bool*  nulls   = (bool *)palloc(count * sizeof(bool));
		jobject typeMap = Invocation_getTypeMap();

		memset(values, 0,  count * sizeof(Datum));
		memset(nulls, true, count);	/* all values null initially */
	
		for(idx = 0; idx < count; ++idx)
		{
			jobject value = JNI_getObjectArrayElement(jvalues, idx);
			if(value != 0)
			{
				Type type = Type_fromOid(SPI_gettypeid(self, idx + 1), typeMap);
				values[idx] = Type_coerceObject(type, value);
				nulls[idx] = false;
			}
		}

		curr = MemoryContextSwitchTo(JavaMemoryContext);
		tuple = heap_form_tuple(self, values, nulls);
		result = Tuple_internalCreate(tuple, false);
		MemoryContextSwitchTo(curr);
		pfree(values);
		pfree(nulls);
	}
	PG_CATCH();
	{
		Exception_throw_ERROR("heap_form_tuple");
	}
	PG_END_TRY();
	END_NATIVE
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_TupleDesc
 * Method:    _free
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_postgresql_pljava_internal_TupleDesc__1free(JNIEnv* env, jobject _this, jlong pointer)
{
	BEGIN_NATIVE_NO_ERRCHECK
	Ptr2Long p2l;
	p2l.longVal = pointer;
	FreeTupleDesc((TupleDesc)p2l.ptrVal);
	END_NATIVE
}

/*
 * Class:     org_postgresql_pljava_internal_TupleDesc
 * Method:    _getOid
 * Signature: (JI)Lorg/postgresql/pljava/internal/Oid;
 */
JNIEXPORT jobject JNICALL
Java_org_postgresql_pljava_internal_TupleDesc__1getOid(JNIEnv* env, jclass cls, jlong _this, jint index)
{
	jobject result = 0;
	
	BEGIN_NATIVE
	Ptr2Long p2l;
	p2l.longVal = _this;
	PG_TRY();
	{
		Oid typeId = SPI_gettypeid((TupleDesc)p2l.ptrVal, (int)index);
		if(!OidIsValid(typeId))
		{
			Exception_throw(ERRCODE_INVALID_DESCRIPTOR_INDEX,
				"Invalid attribute index \"%d\"", (int)index);
		}
		else
		{
			result = Oid_create(typeId);
		}
	}
	PG_CATCH();
	{
		Exception_throw_ERROR("SPI_gettypeid");
	}
	PG_END_TRY();
	END_NATIVE

	return result;
}
