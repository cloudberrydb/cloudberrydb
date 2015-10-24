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

#include "org_postgresql_pljava_jdbc_Invocation.h"
#include "pljava/Invocation.h"
#include "pljava/Function.h"
#include "pljava/PgObject.h"
#include "pljava/JNICalls.h"
#include "pljava/Backend.h"

#define LOCAL_FRAME_SIZE 128

struct CallLocal_
{
	/**
	 * Pointer to the call local structure.
	 */
	void*       pointer;

	/**
	 * The invocation where this CallLocal was allocated
	 */
	Invocation* invocation;

	/**
	 * Next CallLocal in a double linked list
	 */
	CallLocal*	next;

	/**
	 * Previous CallLocal in a double linked list
	 */
	CallLocal*  prev;
};

static jmethodID    s_Invocation_onExit;
static unsigned int s_callLevel = 0;

Invocation* currentInvocation;

extern void Invocation_initialize(void);
void Invocation_initialize(void)
{
	jclass cls;
	JNINativeMethod invocationMethods[] =
	{
		{
		"_getCurrent",
		"()Lorg/postgresql/pljava/jdbc/Invocation;",
		Java_org_postgresql_pljava_jdbc_Invocation__1getCurrent
		},
		{
		"_getNestingLevel",
		"()I",
		Java_org_postgresql_pljava_jdbc_Invocation__1getNestingLevel
		},
		{
		"_clearErrorCondition",
		"()V",
		Java_org_postgresql_pljava_jdbc_Invocation__1clearErrorCondition
		},
		{
		"_register",
		"()V",
		Java_org_postgresql_pljava_jdbc_Invocation__1register
		},
		{ 0, 0, 0 }
	};

	cls = PgObject_getJavaClass("org/postgresql/pljava/jdbc/Invocation");
	PgObject_registerNatives2(cls, invocationMethods);
	s_Invocation_onExit = PgObject_getJavaMethod(cls, "onExit", "()V");
	JNI_deleteLocalRef(cls);
}

void Invocation_assertConnect(void)
{
	if(!currentInvocation->hasConnected)
	{
		SPI_connect();
		currentInvocation->hasConnected = true;
	}
}

void Invocation_assertDisconnect(void)
{
	if(currentInvocation->hasConnected)
	{
		SPI_finish();
		currentInvocation->hasConnected = false;
	}
}

jobject Invocation_getTypeMap(void)
{
	Function f = currentInvocation->function;
	return f == 0 ? 0 : Function_getTypeMap(f);
}

void Invocation_pushBootContext(Invocation* ctx)
{
	ctx->invocation      = 0;
	ctx->function        = 0;
	ctx->trusted         = false;
	ctx->hasConnected    = false;
	ctx->upperContext    = CurrentMemoryContext;
	ctx->errorOccured    = false;
	ctx->inExprContextCB = false;
	ctx->previous        = 0;
	ctx->callLocals      = 0;
	currentInvocation    = ctx;
	++s_callLevel;
}

void Invocation_popBootContext(void)
{
	currentInvocation = 0;
	--s_callLevel;
}

void Invocation_pushInvocation(Invocation* ctx, bool trusted)
{
	JNI_pushLocalFrame(LOCAL_FRAME_SIZE);
	ctx->invocation      = 0;
	ctx->function        = 0;
	ctx->trusted         = trusted;
	ctx->hasConnected    = false;
	ctx->upperContext    = CurrentMemoryContext;
	ctx->errorOccured    = false;
	ctx->inExprContextCB = false;
	ctx->previous        = currentInvocation;
	ctx->callLocals      = 0;
	currentInvocation   = ctx;
	Backend_setJavaSecurity(trusted);
	++s_callLevel;
}

void Invocation_popInvocation(bool wasException)
{
	CallLocal* cl;
	Invocation* ctx = currentInvocation->previous;

	if(currentInvocation->invocation != 0)
	{
		if(!wasException)
			JNI_callVoidMethod(currentInvocation->invocation, s_Invocation_onExit);
		JNI_deleteGlobalRef(currentInvocation->invocation);
	}

	if(currentInvocation->hasConnected)
		SPI_finish();

	JNI_popLocalFrame(0);
	if(ctx != 0)
	{
		PG_TRY();
		{
			Backend_setJavaSecurity(ctx->trusted);
		}
		PG_CATCH();
		{
			elog(FATAL, "Failed to reinstate untrusted security after a trusted call or vice versa");
		}
		PG_END_TRY();
		MemoryContextSwitchTo(ctx->upperContext);
	}
	
	/**
	 * Reset all local wrappers that has been allocated during this call. Yank them
	 * from the double linked list but do *not* remove them.
	 */
	cl = currentInvocation->callLocals;
	if(cl != 0)
	{
		CallLocal* first = cl;
		do
		{
			cl->pointer = 0;
			cl->invocation = 0;
			cl = cl->next;
		} while(cl != first);
	}
	currentInvocation = ctx;
	--s_callLevel;
}

void Invocation_freeLocalWrapper(jlong wrapper)
{
	Ptr2Long p2l;
	Invocation* ctx;
	CallLocal* cl;
	CallLocal* prev;

	p2l.longVal = wrapper;
	cl = (CallLocal*)p2l.ptrVal;
	prev = cl->prev;
	if(prev != cl)
	{
		/* Disconnect
		 */
		CallLocal* next = cl->next;
		prev->next = next;
		next->prev = prev;
	}

	/* If this CallLocal is freed before its owning invocation was
	 * popped then there's a risk that this is the first CallLocal
	 * in the list.
	 */
	ctx = cl->invocation;
	if(ctx != 0 && ctx->callLocals == cl)
	{
		if(prev == cl)
			prev = 0;
		ctx->callLocals = prev;
	}
	pfree(cl);	
}

void* Invocation_getWrappedPointer(jlong wrapper)
{
	Ptr2Long p2l;
	p2l.longVal = wrapper;
	return ((CallLocal*)p2l.ptrVal)->pointer;
}

jlong Invocation_createLocalWrapper(void* pointer)
{
	/* Create a local wrapper for the pointer
	 */
	Ptr2Long p2l;
	CallLocal* cl = (CallLocal*)MemoryContextAlloc(JavaMemoryContext, sizeof(CallLocal));
	CallLocal* prev = currentInvocation->callLocals;
	if(prev == 0)
	{
		currentInvocation->callLocals = cl;
		cl->prev = cl;
		cl->next = cl;
	}
	else
	{
		CallLocal* next = prev->next;
		cl->prev = prev;
		cl->next = next;
		prev->next = cl;
		next->prev = cl;
	}	
	cl->pointer = pointer;
	cl->invocation = currentInvocation;
	p2l.longVal = 0L; /* ensure that the rest is zeroed out */
	p2l.ptrVal = cl;
	return p2l.longVal;
}

MemoryContext
Invocation_switchToUpperContext(void)
{
	return MemoryContextSwitchTo(currentInvocation->upperContext);
}

/*
 * Class:     org_postgresql_pljava_jdbc_Invocation
 * Method:    _getNestingLevel
 * Signature: ()I
 */
JNIEXPORT jint JNICALL
Java_org_postgresql_pljava_jdbc_Invocation__1getNestingLevel(JNIEnv* env, jclass cls)
{
	return s_callLevel;
}

/*
 * Class:     org_postgresql_pljava_jdbc_Invocation
 * Method:    _getCurrent
 * Signature: ()Lorg/postgresql/pljava/jdbc/Invocation;
 */
JNIEXPORT jobject JNICALL
Java_org_postgresql_pljava_jdbc_Invocation__1getCurrent(JNIEnv* env, jclass cls)
{
	return currentInvocation->invocation;
}

/*
 * Class:     org_postgresql_pljava_jdbc_Invocation
 * Method:    _clearErrorCondition
 * Signature: ()V
 */
JNIEXPORT void JNICALL
Java_org_postgresql_pljava_jdbc_Invocation__1clearErrorCondition(JNIEnv* env, jclass cls)
{
	currentInvocation->errorOccured = false;
}

/*
 * Class:     org_postgresql_pljava_jdbc_Invocation
 * Method:    _register
 * Signature: ()V
 */
JNIEXPORT void JNICALL
Java_org_postgresql_pljava_jdbc_Invocation__1register(JNIEnv* env, jobject _this)
{
	currentInvocation->invocation = (*env)->NewGlobalRef(env, _this);
}
