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
#include <executor/tuptable.h>

#include "org_postgresql_pljava_internal_PgSavepoint.h"
#include "pljava/Exception.h"
#include "pljava/type/String.h"
#include "pljava/SPI.h"

extern void PgSavepoint_initialize(void);
void PgSavepoint_initialize(void)
{
	JNINativeMethod methods[] =
	{
		{
		"_set",
	  	"(Ljava/lang/String;)J",
	  	Java_org_postgresql_pljava_internal_PgSavepoint__1set
		},
		{
		"_release",
		"(J)V",
		Java_org_postgresql_pljava_internal_PgSavepoint__1release
		},
		{
		"_rollback",
		"(J)V",
		Java_org_postgresql_pljava_internal_PgSavepoint__1rollback
		},
		{
		"_getName",
		"(J)Ljava/lang/String;",
		Java_org_postgresql_pljava_internal_PgSavepoint__1getName
		},
		{
		"_getId",
		"(J)I",
		Java_org_postgresql_pljava_internal_PgSavepoint__1getId
		},
		{ 0, 0, 0 }
	};
	PgObject_registerNatives("org/postgresql/pljava/internal/PgSavepoint", methods);
}

/****************************************
 * JNI methods
 ****************************************/
/*
 * Class:     org_postgresql_pljava_internal_PgSavepoint
 * Method:    _set
 * Signature: (Ljava/lang/String;)J;
 */
JNIEXPORT jlong JNICALL
Java_org_postgresql_pljava_internal_PgSavepoint__1set(JNIEnv* env, jclass cls, jstring jname)
{
	jlong result = 0;
	BEGIN_NATIVE
	PG_TRY();
	{
		Ptr2Long p2l;
		char* name = String_createNTS(jname);
		MemoryContext currCtx = MemoryContextSwitchTo(JavaMemoryContext);
		p2l.longVal = 0L; /* ensure that the rest is zeroed out */
		p2l.ptrVal = SPI_setSavepoint(name);
		result = p2l.longVal;
		MemoryContextSwitchTo(currCtx);
		pfree(name);
	}
	PG_CATCH();
	{
		Exception_throw_ERROR("SPI_setSavepoint");
	}
	PG_END_TRY();
	END_NATIVE
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_PgSavepoint
 * Method:    _getName
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL
Java_org_postgresql_pljava_internal_PgSavepoint__1getName(JNIEnv* env, jclass clazz, jlong _this)
{
	jstring result = 0;
	if(_this != 0)
	{
		BEGIN_NATIVE
		Ptr2Long p2l;
		p2l.longVal = _this;
		result = String_createJavaStringFromNTS(((Savepoint*)p2l.ptrVal)->name);
		END_NATIVE
	}
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_PgSavepoint
 * Method:    _getId
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL
Java_org_postgresql_pljava_internal_PgSavepoint__1getId(JNIEnv* env, jclass clazz, jlong _this)
{
	jint result = (jint)InvalidSubTransactionId;
	if(_this != 0)
	{
		Ptr2Long p2l;
		p2l.longVal = _this;
		result = (jint)((Savepoint*)p2l.ptrVal)->xid;
	}
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_PgSavepoint
 * Method:    _release
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_postgresql_pljava_internal_PgSavepoint__1release(JNIEnv* env, jclass clazz, jlong _this)
{
	if(_this != 0)
	{
		BEGIN_NATIVE
		Ptr2Long p2l;
		p2l.longVal = _this;
		PG_TRY();
		{
			SPI_releaseSavepoint((Savepoint*)p2l.ptrVal);
		}
		PG_CATCH();
		{
			Exception_throw_ERROR("SPI_releaseSavepoint");
		}
		PG_END_TRY();
		END_NATIVE
	}
}

/*
 * Class:     org_postgresql_pljava_internal_PgSavepoint
 * Method:    _rollback
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_postgresql_pljava_internal_PgSavepoint__1rollback(JNIEnv* env, jclass clazz, jlong _this)
{
	if(_this != 0)
	{
		BEGIN_NATIVE
		Ptr2Long p2l;
		p2l.longVal = _this;
		PG_TRY();
		{
			SPI_rollbackSavepoint((Savepoint*)p2l.ptrVal);
		}
		PG_CATCH();
		{
			Exception_throw_ERROR("SPI_rollbackSavepoint");
		}
		PG_END_TRY();
		END_NATIVE
	}
}
