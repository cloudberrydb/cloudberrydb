/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include <postgres.h>
#include <utils/date.h>
#include <utils/datetime.h>

#include "pljava/type/Type_priv.h"
#include "pljava/type/Timestamp.h"

#define EPOCH_DIFF (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)

static jclass    s_Date_class;
static jmethodID s_Date_init;
static jmethodID s_Date_getTime;

/*
 * Date data type. Postgres will pass and expect number of days since
 * Jan 01 2000. Java uses number of millisecs since midnight Jan 01 1970.
 */
static jvalue _Date_coerceDatum(Type self, Datum arg)
{
	DateADT pgDate = DatumGetDateADT(arg);
	int64 ts = (int64)pgDate * INT64CONST(86400000000);
	int   tz = Timestamp_getTimeZone_id(ts);
	
	jlong date = (jlong)(pgDate + EPOCH_DIFF);

	jvalue result;
	date *= 86400L;	// Convert to seconds
	date += tz;		// Add local timezone
	result.l = JNI_newObject(s_Date_class, s_Date_init, date * 1000);
	return result;
}

static Datum _Date_coerceObject(Type self, jobject date)
{
	jlong milliSecs = JNI_callLongMethod(date, s_Date_getTime) - INT64CONST(86400000) * EPOCH_DIFF;
	jlong secs = milliSecs / 1000 - Timestamp_getTimeZone_id(milliSecs * 1000);
	return DateADTGetDatum((DateADT)(secs / 86400));
}

/* Make this datatype available to the postgres system.
 */
extern void Date_initialize(void);
void Date_initialize(void)
{
	TypeClass cls = TypeClass_alloc("type.Date");
	cls->JNISignature = "Ljava/sql/Date;";
	cls->javaTypeName = "java.sql.Date";
	cls->coerceDatum  = _Date_coerceDatum;
	cls->coerceObject = _Date_coerceObject;
	Type_registerType("java.sql.Date", TypeClass_allocInstance(cls, DATEOID));

	s_Date_class = JNI_newGlobalRef(PgObject_getJavaClass("java/sql/Date"));
	s_Date_init = PgObject_getJavaMethod(s_Date_class, "<init>", "(J)V");
	s_Date_getTime = PgObject_getJavaMethod(s_Date_class, "getTime",  "()J");
}
