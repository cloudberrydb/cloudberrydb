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

#include "pljava/Backend.h"
#include "pljava/Invocation.h"
#include "pljava/Exception.h"
#include "pljava/type/String.h"
#include "pljava/type/ErrorData.h"

jclass Class_class;
jmethodID Class_getName;

jclass    ServerException_class;
jmethodID ServerException_getErrorData;
jmethodID ServerException_init;

jclass    Throwable_class;
jmethodID Throwable_getMessage;
jmethodID Throwable_printStackTrace;

jclass    IllegalArgumentException_class;
jmethodID IllegalArgumentException_init;

jclass    SQLException_class;
jmethodID SQLException_init;
jmethodID SQLException_getSQLState;

jclass    UnsupportedOperationException_class;
jmethodID UnsupportedOperationException_init;

void
Exception_featureNotSupported(const char* requestedFeature, const char* introVersion)
{
	jstring jmsg;
	jobject ex;
	StringInfoData buf;
	initStringInfo(&buf);
	PG_TRY();
	{
		appendStringInfoString(&buf, "Feature: ");
		appendStringInfoString(&buf, requestedFeature);
		appendStringInfoString(&buf, " lacks support in PostgreSQL version ");
		appendStringInfo(&buf, "%d.%d", PGSQL_MAJOR_VER, PGSQL_MINOR_VER);
		appendStringInfoString(&buf, ". It was introduced in version ");
		appendStringInfoString(&buf, introVersion);
	
		ereport(DEBUG3, (errmsg("%s", buf.data)));
		jmsg = String_createJavaStringFromNTS(buf.data);
	
		ex = JNI_newObject(UnsupportedOperationException_class, UnsupportedOperationException_init, jmsg);
		JNI_deleteLocalRef(jmsg);
		JNI_throw(ex);
	}
	PG_CATCH();
	{
		ereport(WARNING, 
				(errcode(ERRCODE_INTERNAL_ERROR), 
				 errmsg("Exception while generating exception: %s", buf.data)));
	}
	PG_END_TRY();
	pfree(buf.data);
}

void Exception_throw(int errCode, const char* errMessage, ...)
{
    char buf[1024];
	va_list args;
	jstring message;
	jstring sqlState;
	jobject ex;
	int idx;

	va_start(args, errMessage);
	vsnprintf(buf, sizeof(buf), errMessage, args);
	ereport(DEBUG3, (errcode(errCode), errmsg("%s", buf)));

	PG_TRY();
	{
		message = String_createJavaStringFromNTS(buf);
	
		/* unpack MAKE_SQLSTATE code
		 */
		for (idx = 0; idx < 5; ++idx)
		{
			buf[idx] = PGUNSIXBIT(errCode);
			errCode >>= 6;
		}
		buf[idx] = 0;
	
		sqlState = String_createJavaStringFromNTS(buf);
	
		ex = JNI_newObject(SQLException_class, SQLException_init, message, sqlState);
	
		JNI_deleteLocalRef(message);
		JNI_deleteLocalRef(sqlState);
		JNI_throw(ex);
	}
	PG_CATCH();
	{
		ereport(WARNING, (errcode(errCode), errmsg("Exception while generating exception: %s", buf)));
	}
	PG_END_TRY();
	va_end(args);
}

void Exception_throwIllegalArgument(const char* errMessage, ...)
{
    char buf[1024];
	va_list args;
	jstring message;
	jobject ex;

	va_start(args, errMessage);
	vsnprintf(buf, sizeof(buf), errMessage, args);
	ereport(DEBUG3, (errmsg("%s", buf)));

	PG_TRY();
	{
		message = String_createJavaStringFromNTS(buf);
	
		ex = JNI_newObject(IllegalArgumentException_class, IllegalArgumentException_init, message);
	
		JNI_deleteLocalRef(message);
		JNI_throw(ex);
	}
	PG_CATCH();
	{
		ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Exception while generating exception: %s", buf)));
	}
	PG_END_TRY();
	va_end(args);
}

void Exception_throwSPI(const char* function, int errCode)
{
	Exception_throw(ERRCODE_INTERNAL_ERROR,
		"SPI function SPI_%s failed with error %s", function,
			SPI_result_code_string(errCode));
}

void Exception_throw_ERROR(const char* funcName)
{
	jobject ex;
	PG_TRY();
	{
		jobject ed = ErrorData_getCurrentError();
	
		FlushErrorState();
	
		ex = JNI_newObject(ServerException_class, ServerException_init, ed);
		currentInvocation->errorOccured = true;

		elog(DEBUG1, "Exception in function %s", funcName);

		JNI_deleteLocalRef(ed);
		JNI_throw(ex);
	}
	PG_CATCH();
	{
		elog(WARNING, "Exception while generating exception");
	}
	PG_END_TRY();
}

extern void Exception_initialize(void);
void Exception_initialize(void)
{
	Class_class = (jclass)JNI_newGlobalRef(PgObject_getJavaClass("java/lang/Class"));

	Throwable_class = (jclass)JNI_newGlobalRef(PgObject_getJavaClass("java/lang/Throwable"));
	Throwable_getMessage = PgObject_getJavaMethod(Throwable_class, "getMessage", "()Ljava/lang/String;");
	Throwable_printStackTrace = PgObject_getJavaMethod(Throwable_class, "printStackTrace", "()V");

	IllegalArgumentException_class = (jclass)JNI_newGlobalRef(PgObject_getJavaClass("java/lang/IllegalArgumentException"));
	IllegalArgumentException_init = PgObject_getJavaMethod(IllegalArgumentException_class, "<init>", "(Ljava/lang/String;)V");

	SQLException_class = (jclass)JNI_newGlobalRef(PgObject_getJavaClass("java/sql/SQLException"));
	SQLException_init = PgObject_getJavaMethod(SQLException_class, "<init>", "(Ljava/lang/String;Ljava/lang/String;)V");
	SQLException_getSQLState = PgObject_getJavaMethod(SQLException_class, "getSQLState", "()Ljava/lang/String;");

	UnsupportedOperationException_class = (jclass)JNI_newGlobalRef(PgObject_getJavaClass("java/lang/UnsupportedOperationException"));
	UnsupportedOperationException_init = PgObject_getJavaMethod(UnsupportedOperationException_class, "<init>", "(Ljava/lang/String;)V");

	Class_getName = PgObject_getJavaMethod(Class_class, "getName", "()Ljava/lang/String;");
}

extern void Exception_initialize2(void);
void Exception_initialize2(void)
{
	ServerException_class = (jclass)JNI_newGlobalRef(PgObject_getJavaClass("org/postgresql/pljava/internal/ServerException"));
	ServerException_init = PgObject_getJavaMethod(ServerException_class, "<init>", "(Lorg/postgresql/pljava/internal/ErrorData;)V");

	ServerException_getErrorData = PgObject_getJavaMethod(ServerException_class, "getErrorData", "()Lorg/postgresql/pljava/internal/ErrorData;");
}
