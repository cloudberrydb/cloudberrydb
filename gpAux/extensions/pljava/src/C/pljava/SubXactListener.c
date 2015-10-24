/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include "pljava/Backend.h"
#include "pljava/Exception.h"
#include "pljava/SPI.h"
#include "org_postgresql_pljava_internal_SubXactListener.h"

static jclass s_SubXactListener_class;
static jmethodID s_SubXactListener_onStart;
static jmethodID s_SubXactListener_onCommit;
static jmethodID s_SubXactListener_onAbort;

static void subXactCB(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid, void* arg)
{
	Ptr2Long p2l;
	p2l.longVal = 0L; /* ensure that the rest is zeroed out */
	p2l.ptrVal = arg;
	switch(event)
	{
		case SUBXACT_EVENT_START_SUB:
			{
			Ptr2Long infant2l;
			infant->xid = mySubid;
			infant2l.longVal = 0L; /* ensure that the rest is zeroed out */
			infant2l.ptrVal = infant;
			JNI_callStaticVoidMethod(s_SubXactListener_class, s_SubXactListener_onStart, p2l.longVal, infant2l.longVal, parentSubid);
			}
			break;
		case SUBXACT_EVENT_COMMIT_SUB:
			JNI_callStaticVoidMethod(s_SubXactListener_class, s_SubXactListener_onCommit, p2l.longVal, mySubid, parentSubid);
			break;
		case SUBXACT_EVENT_ABORT_SUB:
			JNI_callStaticVoidMethod(s_SubXactListener_class, s_SubXactListener_onAbort, p2l.longVal, mySubid, parentSubid);
	}
}

extern void SubXactListener_initialize(void);
void SubXactListener_initialize(void)
{
	JNINativeMethod methods[] = {
		{
		"_register",
	  	"(J)V",
	  	Java_org_postgresql_pljava_internal_SubXactListener__1register
		},
		{
		"_unregister",
	  	"(J)V",
	  	Java_org_postgresql_pljava_internal_SubXactListener__1unregister
		},
		{ 0, 0, 0 }};

	PgObject_registerNatives("org/postgresql/pljava/internal/SubXactListener", methods);

	s_SubXactListener_class = JNI_newGlobalRef(PgObject_getJavaClass("org/postgresql/pljava/internal/SubXactListener"));
	s_SubXactListener_onAbort  = PgObject_getStaticJavaMethod(s_SubXactListener_class, "onAbort",  "(JII)V");
	s_SubXactListener_onCommit = PgObject_getStaticJavaMethod(s_SubXactListener_class, "onCommit", "(JII)V");
	s_SubXactListener_onStart  = PgObject_getStaticJavaMethod(s_SubXactListener_class, "onStart",  "(JJI)V");
}

/*
 * Class:     org_postgresql_pljava_internal_SubXactListener
 * Method:    _register
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_postgresql_pljava_internal_SubXactListener__1register(JNIEnv* env, jclass cls, jlong listenerId)
{
	BEGIN_NATIVE
	PG_TRY();
	{
		Ptr2Long p2l;
		p2l.longVal = listenerId;
		RegisterSubXactCallback(subXactCB, p2l.ptrVal);
	}
	PG_CATCH();
	{
		Exception_throw_ERROR("RegisterSubXactCallback");
	}
	PG_END_TRY();
	END_NATIVE
}

/*
 * Class:     org_postgresql_pljava_internal_SubXactListener
 * Method:    _unregister
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_postgresql_pljava_internal_SubXactListener__1unregister(JNIEnv* env, jclass cls, jlong listenerId)
{
	BEGIN_NATIVE
	PG_TRY();
	{
		Ptr2Long p2l;
		p2l.longVal = listenerId;
		UnregisterSubXactCallback(subXactCB, p2l.ptrVal);
	}
	PG_CATCH();
	{
		Exception_throw_ERROR("UnregisterSubXactCallback");
	}
	PG_END_TRY();
	END_NATIVE
}
