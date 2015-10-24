/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include <postgres.h>
#include <utils/nabstime.h>
#include <utils/date.h>
#include <utils/datetime.h>

#include "pljava/Backend.h"
#include "pljava/type/Type_priv.h"
#include "pljava/type/Time.h"
#include "pljava/type/Timestamp.h"

/*
 * Time type. Postgres will pass (and expect in return) a local Time.
 * The Java java.sql.Time is UTC time and not a perfect fit. Perhaps
 * a LocalTime object should be added to the Java domain?
 */
static jclass    s_Time_class;
static jmethodID s_Time_init;
static jmethodID s_Time_getTime;

static jlong msecsAtMidnight(void)
{
	AbsoluteTime now = GetCurrentAbsoluteTime() / 86400;
	return INT64CONST(1000) * (jlong)(now * 86400);
}

static jvalue Time_coerceDatumTZ_dd(Type self, double t, bool tzAdjust)
{
	jlong mSecs;
	jvalue result;
	if(tzAdjust)
		t += Timestamp_getCurrentTimeZone();/* Adjust from local time to UTC */
	t *= 1000.0;						/* Convert to millisecs */
	mSecs = (jlong)floor(t);
	result.l = JNI_newObject(s_Time_class, s_Time_init, mSecs + msecsAtMidnight());
	return result;
}

static jvalue Time_coerceDatumTZ_id(Type self, int64 t, bool tzAdjust)
{
	jvalue result;
	jlong mSecs = t / 1000;			/* Convert to millisecs */
	if(tzAdjust)
		mSecs += Timestamp_getCurrentTimeZone() * 1000;/* Adjust from local time to UTC */
	result.l = JNI_newObject(s_Time_class, s_Time_init, mSecs + msecsAtMidnight());
	return result;
}

static jlong Time_getMillisecsToday(Type self, jobject jt, bool tzAdjust)
{
	jlong mSecs = JNI_callLongMethod(jt, s_Time_getTime);
	if(tzAdjust)
		mSecs -= ((jlong)Timestamp_getCurrentTimeZone()) * 1000L; /* Adjust from UTC to local time */
	mSecs %= 86400000; /* Strip everything above 24 hours */
	return mSecs;
}

static double Time_coerceObjectTZ_dd(Type self, jobject jt, bool tzAdjust)
{
	jlong mSecs = Time_getMillisecsToday(self, jt, tzAdjust);
	return ((double)mSecs) / 1000.0; /* Convert to seconds */
}

static int64 Time_coerceObjectTZ_id(Type self, jobject jt, bool tzAdjust)
{
	jlong mSecs = Time_getMillisecsToday(self, jt, tzAdjust);
	return mSecs * 1000L; /* Convert millisecs to microsecs */
}

static jvalue _Time_coerceDatum(Type self, Datum arg)
{
	return integerDateTimes
		? Time_coerceDatumTZ_id(self, DatumGetInt64(arg), true)
		: Time_coerceDatumTZ_dd(self, DatumGetFloat8(arg), true);
}

static Datum _Time_coerceObject(Type self, jobject time)
{
	return integerDateTimes
		? Int64GetDatum(Time_coerceObjectTZ_id(self, time, true))
		: Float8GetDatum(Time_coerceObjectTZ_dd(self, time, true));
}

/* 
 * Time with time zone. Postgres will pass local time and an associated
 * time zone. In the future, we might create a special java object for
 * this. For now, we just convert to UTC and pass a Time object.
 */
static jvalue _Timetz_coerceDatum(Type self, Datum arg)
{
	jvalue val;
	if(integerDateTimes)
	{
		TimeTzADT_id* tza = (TimeTzADT_id*)DatumGetPointer(arg);
		int64 t = tza->time + (int64)tza->zone * 1000000; /* Convert to UTC */
		val = Time_coerceDatumTZ_id(self, t, false);
	}
	else
	{
		TimeTzADT_dd* tza = (TimeTzADT_dd*)DatumGetPointer(arg);
		double t = tza->time + tza->zone; /* Convert to UTC */
		val = Time_coerceDatumTZ_dd(self, t, false);
	}
	return val;
}

static Datum _Timetz_coerceObject(Type self, jobject time)
{
	Datum datum;
	if(integerDateTimes)
	{
		TimeTzADT_id* tza = (TimeTzADT_id*)palloc(sizeof(TimeTzADT_id));
		tza->time = Time_coerceObjectTZ_id(self, time, false);
		tza->zone = Timestamp_getCurrentTimeZone();
		tza->time -= (int64)tza->zone * 1000000; /* Convert UTC to local time */
		datum = PointerGetDatum(tza);
	}
	else
	{
		TimeTzADT_dd* tza = (TimeTzADT_dd*)palloc(sizeof(TimeTzADT_dd));
		tza->time = Time_coerceObjectTZ_dd(self, time, false);
		tza->zone = Timestamp_getCurrentTimeZone();
		tza->time -= tza->zone; /* Convert UTC to local time */
		datum = PointerGetDatum(tza);
	}
	return datum;
}

extern void Time_initialize(void);
void Time_initialize(void)
{
	TypeClass cls;
	s_Time_class = JNI_newGlobalRef(PgObject_getJavaClass("java/sql/Time"));
	s_Time_init = PgObject_getJavaMethod(s_Time_class, "<init>", "(J)V");
	s_Time_getTime = PgObject_getJavaMethod(s_Time_class, "getTime", "()J");

	cls = TypeClass_alloc("type.Time");
	cls->JNISignature = "Ljava/sql/Time;";
	cls->javaTypeName = "java.sql.Time";
	cls->coerceDatum  = _Time_coerceDatum;
	cls->coerceObject = _Time_coerceObject;
	Type_registerType(0, TypeClass_allocInstance(cls, TIMEOID));

	cls = TypeClass_alloc("type.Timetz");
	cls->JNISignature = "Ljava/sql/Time;";
	cls->javaTypeName = "java.sql.Time";
	cls->coerceDatum  = _Timetz_coerceDatum;
	cls->coerceObject = _Timetz_coerceObject;
	Type_registerType("java.sql.Time", TypeClass_allocInstance(cls, TIMETZOID));
}
