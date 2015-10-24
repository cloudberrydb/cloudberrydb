/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include <postgres.h>
#include <executor/tuptable.h>
#include <utils/guc.h>

#include "org_postgresql_pljava_internal_ExecutionPlan.h"
#include "pljava/Invocation.h"
#include "pljava/Exception.h"
#include "pljava/Function.h"
#include "pljava/SPI.h"
#include "pljava/type/Oid.h"
#include "pljava/type/Portal.h"
#include "pljava/type/String.h"

/* Class 07 - Dynamic SQL Error */
#define ERRCODE_PARAMETER_COUNT_MISMATCH	MAKE_SQLSTATE('0','7', '0','0','1')

/* Make this datatype available to the postgres system.
 */
extern void ExecutionPlan_initialize(void);
void ExecutionPlan_initialize(void)
{
	JNINativeMethod methods[] =
	{
		{
		"_cursorOpen",
		"(JJLjava/lang/String;[Ljava/lang/Object;)Lorg/postgresql/pljava/internal/Portal;",
		Java_org_postgresql_pljava_internal_ExecutionPlan__1cursorOpen
		},
		{
		"_isCursorPlan",
		"(J)Z",
		Java_org_postgresql_pljava_internal_ExecutionPlan__1isCursorPlan
		},
		{
		"_execute",
		"(JJ[Ljava/lang/Object;I)I",
		Java_org_postgresql_pljava_internal_ExecutionPlan__1execute
		},
		{
		"_prepare",
		"(JLjava/lang/String;[Lorg/postgresql/pljava/internal/Oid;)J",
		Java_org_postgresql_pljava_internal_ExecutionPlan__1prepare
		},
		{
		"_invalidate",
		"(J)V",
		Java_org_postgresql_pljava_internal_ExecutionPlan__1invalidate
		},
		{ 0, 0, 0 }
	};
	PgObject_registerNatives("org/postgresql/pljava/internal/ExecutionPlan", methods);
}

static bool coerceObjects(void* ePlan, jobjectArray jvalues, Datum** valuesPtr, char** nullsPtr)
{
	char*  nulls = 0;
	Datum* values = 0;

	int count = SPI_getargcount(ePlan);
	if((jvalues == 0 && count != 0)
	|| (jvalues != 0 && count != JNI_getArrayLength(jvalues)))
	{
		Exception_throw(ERRCODE_PARAMETER_COUNT_MISMATCH,
			"Number of values does not match number of arguments for prepared plan");
		return false;
	}

	if(count > 0)
	{
		int idx;
		jobject typeMap = Invocation_getTypeMap();
		values = (Datum*)palloc(count * sizeof(Datum));
		for(idx = 0; idx < count; ++idx)
		{
			Oid typeId = SPI_getargtypeid(ePlan, idx);
			Type type = Type_fromOid(typeId, typeMap);
			jobject value = JNI_getObjectArrayElement(jvalues, idx);
			if(value != 0)
			{
				values[idx] = Type_coerceObject(type, value);
				JNI_deleteLocalRef(value);
			}
			else
			{
				values[idx] = 0;
				if(nulls == 0)
				{
					nulls = (char*)palloc(count+1);
					memset(nulls, ' ', count);	/* all values non-null initially */
					nulls[count] = 0;
					*nullsPtr = nulls;
				}
				nulls[idx] = 'n';
			}
		}
	}
	*valuesPtr = values;
	*nullsPtr = nulls;
	return true;
}

/****************************************
 * JNI methods
 ****************************************/
/*
 * Class:     org_postgresql_pljava_internal_ExecutionPlan
 * Method:    _cursorOpen
 * Signature: (JJLjava/lang/String;[Ljava/lang/Object;)Lorg/postgresql/pljava/internal/Portal;
 */
JNIEXPORT jobject JNICALL
Java_org_postgresql_pljava_internal_ExecutionPlan__1cursorOpen(JNIEnv* env, jclass clazz, jlong _this, jlong threadId, jstring cursorName, jobjectArray jvalues)
{
	jobject jportal = 0;
	if(_this != 0)
	{
		BEGIN_NATIVE
		STACK_BASE_VARS
		STACK_BASE_PUSH(threadId)
		PG_TRY();
		{
			Ptr2Long p2l;
			Datum*  values  = 0;
			char*   nulls   = 0;
			p2l.longVal = _this;
			if(coerceObjects(p2l.ptrVal, jvalues, &values, &nulls))
			{
				Portal portal;
				char* name = 0;
				if(cursorName != 0)
					name = String_createNTS(cursorName);

				Invocation_assertConnect();
				portal = SPI_cursor_open(
					name, p2l.ptrVal, values, nulls, Function_isCurrentReadOnly());
				if(name != 0)
					pfree(name);
				if(values != 0)
					pfree(values);
				if(nulls != 0)
					pfree(nulls);
			
				jportal = Portal_create(portal);
			}
		}
		PG_CATCH();
		{
			Exception_throw_ERROR("SPI_cursor_open");
		}
		PG_END_TRY();
		STACK_BASE_POP()
		END_NATIVE
	}
	return jportal;
}

/*
 * Class:     org_postgresql_pljava_internal_ExecutionPlan
 * Method:    _isCursorPlan
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL
Java_org_postgresql_pljava_internal_ExecutionPlan__1isCursorPlan(JNIEnv* env, jclass clazz, jlong _this)
{
	jboolean result = JNI_FALSE;

	if(_this != 0)
	{
		BEGIN_NATIVE
		PG_TRY();
		{
			Ptr2Long p2l;
			p2l.longVal = _this;
			Invocation_assertConnect();
			result = (jboolean)SPI_is_cursor_plan(p2l.ptrVal);
		}
		PG_CATCH();
		{
			Exception_throw_ERROR("SPI_is_cursor_plan");
		}
		PG_END_TRY();
		END_NATIVE
	}
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_ExecutionPlan
 * Method:    _execute
 * Signature: (JJ[Ljava/lang/Object;I)V
 */
JNIEXPORT jint JNICALL
Java_org_postgresql_pljava_internal_ExecutionPlan__1execute(JNIEnv* env, jclass clazz, jlong _this, jlong threadId, jobjectArray jvalues, jint count)
{
	jint result = 0;
	if(_this != 0)
	{
		BEGIN_NATIVE
		STACK_BASE_VARS
		STACK_BASE_PUSH(threadId)
		PG_TRY();
		{
			Ptr2Long p2l;
			Datum* values = 0;
			char*  nulls  = 0;
			p2l.longVal = _this;
			if(coerceObjects(p2l.ptrVal, jvalues, &values, &nulls))
			{
				Invocation_assertConnect();
				result = (jint)SPI_execute_plan(
					p2l.ptrVal, values, nulls, Function_isCurrentReadOnly(), (int)count);
				if(result < 0)
					Exception_throwSPI("execute_plan", result);

				if(values != 0)
					pfree(values);
				if(nulls != 0)
					pfree(nulls);
			}
		}
		PG_CATCH();
		{
			Exception_throw_ERROR("SPI_execute_plan");
		}
		PG_END_TRY();
		STACK_BASE_POP()
		END_NATIVE
	}
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_ExecutionPlan
 * Method:    _prepare
 * Signature: (JLjava/lang/String;[Lorg/postgresql/pljava/internal/Oid;)J;
 */
JNIEXPORT jlong JNICALL
Java_org_postgresql_pljava_internal_ExecutionPlan__1prepare(JNIEnv* env, jclass clazz, jlong threadId, jstring jcmd, jobjectArray paramTypes)
{
	jlong result = 0;
	BEGIN_NATIVE
	STACK_BASE_VARS
	STACK_BASE_PUSH(threadId)
	PG_TRY();
	{
		char* cmd;
		void* ePlan;
		int paramCount = 0;
		Oid* paramOids = 0;

		if(paramTypes != 0)
		{
			paramCount = JNI_getArrayLength(paramTypes);
			if(paramCount > 0)
			{
				int idx;
				paramOids = (Oid*)palloc(paramCount * sizeof(Oid));
				for(idx = 0; idx < paramCount; ++idx)
				{
					jobject joid = JNI_getObjectArrayElement(paramTypes, idx);
					paramOids[idx] = Oid_getOid(joid);
					JNI_deleteLocalRef(joid);
				}
			}
		}

		cmd   = String_createNTS(jcmd);
		Invocation_assertConnect();
		ePlan = SPI_prepare(cmd, paramCount, paramOids);
		pfree(cmd);

		if(ePlan == 0)
			Exception_throwSPI("prepare", SPI_result);
		else
		{
			Ptr2Long p2l;
			
			/* Make the plan durable
			 */
			p2l.longVal = 0L; /* ensure that the rest is zeroed out */
			p2l.ptrVal = SPI_saveplan(ePlan);
			result = p2l.longVal;
			SPI_freeplan(ePlan);	/* Get rid of the original, nobody can see it anymore */
		}
	}
	PG_CATCH();
	{
		Exception_throw_ERROR("SPI_prepare");
	}
	PG_END_TRY();
	STACK_BASE_POP()
	END_NATIVE
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_ExecutionPlan
 * Method:    _invalidate
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_postgresql_pljava_internal_ExecutionPlan__1invalidate(JNIEnv* env, jclass clazz, jlong _this)
{
	/* The plan is not cached as a normal JavaHandle since its made
	 * persistent.
	 */
	if(_this != 0)
	{
		BEGIN_NATIVE_NO_ERRCHECK
		PG_TRY();
		{
			Ptr2Long p2l;
			p2l.longVal = _this;
			SPI_freeplan(p2l.ptrVal);
		}
		PG_CATCH();
		{
			Exception_throw_ERROR("SPI_freeplan");
		}
		PG_END_TRY();
		END_NATIVE
	}
}
