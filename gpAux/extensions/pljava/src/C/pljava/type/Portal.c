/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include <postgres.h>
#include <commands/portalcmds.h>
#include <executor/spi.h>
#include <executor/tuptable.h>

#include "org_postgresql_pljava_internal_Portal.h"
#include "pljava/Backend.h"
#include "pljava/Exception.h"
#include "pljava/Invocation.h"
#include "pljava/HashMap.h"
#include "pljava/type/Type_priv.h"
#include "pljava/type/TupleDesc.h"
#include "pljava/type/Portal.h"
#include "pljava/type/String.h"

static jclass    s_Portal_class;
static jmethodID s_Portal_init;
static jfieldID  s_Portal_pointer;

typedef void (*PortalCleanupProc)(Portal portal);

static HashMap s_portalMap = 0;
static PortalCleanupProc s_originalCleanupProc = 0;

static void _pljavaPortalCleanup(Portal portal)
{
	jobject jportal = (jobject)HashMap_getByOpaque(s_portalMap, portal);
	if(jportal != 0)
	{
		/*
		 * Remove this object from the cache and clear its
		 * handle.
		 */
		HashMap_removeByOpaque(s_portalMap, portal);
		JNI_setLongField(jportal, s_Portal_pointer, 0);
	}
	portal->cleanup = s_originalCleanupProc;
	if(s_originalCleanupProc != 0)
	{
		(*s_originalCleanupProc)(portal);
	}
}

/*
 * org.postgresql.pljava.type.Tuple type.
 */
jobject Portal_create(Portal portal)
{
	jobject jportal = 0;
	if(portal != 0)
	{
		jportal = (jobject)HashMap_getByOpaque(s_portalMap, portal);
		if(jportal == 0)
		{
			Ptr2Long p2l;
			p2l.longVal = 0L; /* ensure that the rest is zeroed out */
			p2l.ptrVal = portal;

			/* We need to know when a portal is dropped so that we
			* don't attempt to drop it twice.
			*/
			if(s_originalCleanupProc == 0)
				s_originalCleanupProc = portal->cleanup;

			jportal = JNI_newObject(s_Portal_class, s_Portal_init, p2l.longVal);
			HashMap_putByOpaque(s_portalMap, portal, JNI_newGlobalRef(jportal));

			/*
			 * Fail the day the backend decides to utilize the pointer for multiple
			 * purposes.
			 */
			Assert(portal->cleanup == s_originalCleanupProc);
			portal->cleanup = _pljavaPortalCleanup;
		}
	}
	return jportal;
}

/* Make this datatype available to the postgres system.
 */
extern void Portal_initialize(void);
void Portal_initialize(void)
{
	JNINativeMethod methods[] =
	{
		{
		"_getName",
		"(J)Ljava/lang/String;",
		Java_org_postgresql_pljava_internal_Portal__1getName
		},
		{
		"_getPortalPos",
	  	"(J)I",
	  	Java_org_postgresql_pljava_internal_Portal__1getPortalPos
		},
		{
		"_getTupleDesc",
		"(J)Lorg/postgresql/pljava/internal/TupleDesc;",
		Java_org_postgresql_pljava_internal_Portal__1getTupleDesc
		},
		{
		"_fetch",
	  	"(JZI)I",
	  	Java_org_postgresql_pljava_internal_Portal__1fetch
		},
		{
		"_close",
	  	"(J)V",
	  	Java_org_postgresql_pljava_internal_Portal__1close
		},
		{
		"_isAtEnd",
	  	"(J)Z",
	  	Java_org_postgresql_pljava_internal_Portal__1isAtEnd
		},
		{
		"_isAtStart",
	  	"(J)Z",
	  	Java_org_postgresql_pljava_internal_Portal__1isAtStart
		},
		{
		"_isPosOverflow",
	  	"(J)Z",
	  	Java_org_postgresql_pljava_internal_Portal__1isPosOverflow
		},
		{
		"_move",
	  	"(JZI)I",
	  	Java_org_postgresql_pljava_internal_Portal__1move
		},
		{ 0, 0, 0 }
	};

	s_Portal_class = JNI_newGlobalRef(PgObject_getJavaClass("org/postgresql/pljava/internal/Portal"));
	PgObject_registerNatives2(s_Portal_class, methods);
	s_Portal_init = PgObject_getJavaMethod(s_Portal_class, "<init>", "(J)V");
	s_Portal_pointer = PgObject_getJavaField(s_Portal_class, "m_pointer", "J");
	s_portalMap = HashMap_create(13, TopMemoryContext);
}

/****************************************
 * JNI methods
 ****************************************/

/*
 * Class:     org_postgresql_pljava_internal_Portal
 * Method:    _getPortalPos
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL
Java_org_postgresql_pljava_internal_Portal__1getPortalPos(JNIEnv* env, jclass clazz, jlong _this)
{
	jint result = 0;
	if(_this != 0)
	{
		Ptr2Long p2l;
		p2l.longVal = _this;
		result = (jint)((Portal)p2l.ptrVal)->portalPos;
	}
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_Portal
 * Method:    _fetch
 * Signature: (JZI)I
 */
JNIEXPORT jint JNICALL
Java_org_postgresql_pljava_internal_Portal__1fetch(JNIEnv* env, jclass clazz, jlong _this, jboolean forward, jint count)
{
	jint result = 0;
	if(_this != 0)
	{
		BEGIN_NATIVE
		Ptr2Long p2l;
		p2l.longVal = _this;
		PG_TRY();
		{
			SPI_cursor_fetch((Portal)p2l.ptrVal, forward == JNI_TRUE, (int)count);
			result = (jint)SPI_processed;
		}
		PG_CATCH();
		{
			Exception_throw_ERROR("SPI_cursor_fetch");
		}
		PG_END_TRY();
		END_NATIVE
	}
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_Portal
 * Method:    _getName
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL
Java_org_postgresql_pljava_internal_Portal__1getName(JNIEnv* env, jclass clazz, jlong _this)
{
	jstring result = 0;
	if(_this != 0)
	{
		BEGIN_NATIVE
		Ptr2Long p2l;
		p2l.longVal = _this;
		result = String_createJavaStringFromNTS(((Portal)p2l.ptrVal)->name);
		END_NATIVE
	}
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_Portal
 * Method:    _getTupleDesc
 * Signature: (J)Lorg/postgresql/pljava/internal/TupleDesc;
 */
JNIEXPORT jobject JNICALL
Java_org_postgresql_pljava_internal_Portal__1getTupleDesc(JNIEnv* env, jclass clazz, jlong _this)
{
	jobject result = 0;
	if(_this != 0)
	{
		BEGIN_NATIVE
		Ptr2Long p2l;
		p2l.longVal = _this;
		result = TupleDesc_create(((Portal)p2l.ptrVal)->tupDesc);
		END_NATIVE
	}
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_Portal
 * Method:    _invalidate
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_postgresql_pljava_internal_Portal__1close(JNIEnv* env, jclass clazz, jlong _this)
{
	/* We don't use error checking here since we don't want an exception
	 * caused by another exception when we attempt to close.
	 */
	if(_this != 0)
	{
		Ptr2Long p2l;
		p2l.longVal = _this;
		BEGIN_NATIVE_NO_ERRCHECK
		Portal portal = (Portal)p2l.ptrVal;

		/* Reset our own cleanup callback if needed. No need to come in
		 * the backway
		 */
		HashMap_removeByOpaque(s_portalMap, portal);
		if(portal->cleanup == _pljavaPortalCleanup)
			portal->cleanup = s_originalCleanupProc;

		if(!(currentInvocation->errorOccured || currentInvocation->inExprContextCB))
			SPI_cursor_close(portal);
		END_NATIVE
	}
}

/*
 * Class:     org_postgresql_pljava_internal_Portal
 * Method:    _isAtStart
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL
Java_org_postgresql_pljava_internal_Portal__1isAtStart(JNIEnv* env, jclass clazz, jlong _this)
{
	jboolean result = JNI_FALSE;
	if(_this != 0)
	{
		Ptr2Long p2l;
		p2l.longVal = _this;
		result = (jboolean)((Portal)p2l.ptrVal)->atStart;
	}
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_Portal
 * Method:    _isAtEnd
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL
Java_org_postgresql_pljava_internal_Portal__1isAtEnd(JNIEnv* env, jclass clazz, jlong _this)
{
	jboolean result = JNI_FALSE;
	if(_this != 0)
	{
		Ptr2Long p2l;
		p2l.longVal = _this;
		result = (jboolean)((Portal)p2l.ptrVal)->atEnd;
	}
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_Portal
 * Method:    _isPosOverflow
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL
Java_org_postgresql_pljava_internal_Portal__1isPosOverflow(JNIEnv* env, jclass clazz, jlong _this)
{
	jboolean result = JNI_FALSE;
	if(_this != 0)
	{
		Ptr2Long p2l;
		p2l.longVal = _this;
		result = (jboolean)((Portal)p2l.ptrVal)->posOverflow;
	}
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_Portal
 * Method:    _move
 * Signature: (ZI)I
 */
JNIEXPORT jint JNICALL
Java_org_postgresql_pljava_internal_Portal__1move(JNIEnv* env, jclass clazz, jlong _this, jboolean forward, jint count)
{
	jint result = 0;
	if(_this != 0)
	{
		BEGIN_NATIVE
		Ptr2Long p2l;
		p2l.longVal = _this;
		PG_TRY();
		{
			SPI_cursor_move((Portal)p2l.ptrVal, forward == JNI_TRUE, (int)count);
			result = (jint)SPI_processed;
		}
		PG_CATCH();
		{
			Exception_throw_ERROR("SPI_cursor_move");
		}
		PG_END_TRY();
		END_NATIVE
	}
	return result;
}
