/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_pljava_h
#define __pljava_pljava_h

#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif

/*****************************************************************
 * Misc stuff to tie Java to PostgreSQL. TRY/CATCH macros, thread
 * blocking, etc. resides here.
 *
 * @author Thomas Hallgren
 *****************************************************************/

#ifdef __STRICT_ANSI__
extern int vsnprintf(char* buf, size_t count, const char* format, va_list arg);
#endif

#include <postgres.h>
#include <lib/stringinfo.h>
#include <fmgr.h>
#include <mb/pg_wchar.h>
#include <utils/syscache.h>
#include <utils/memutils.h>
#include <tcop/tcopprot.h>

/* The errorOccured will be set when a call from Java into one of the
 * backend functions results in a elog that causes a longjmp (Levels >= ERROR)
 * that was trapped using the PLJAVA_TRY/PLJAVA_CATCH macros.
 * When this happens, all further calls from Java must be blocked since the
 * state of the current transaction is unknown. Further more, once the function
 * that initially called Java finally returns, the intended longjmp (the one
 * to the original value of Warn_restart) must be made.
 */
extern jlong mainThreadId;
extern bool pljavaEntryFence(JNIEnv* env);
extern JNIEnv* currentJNIEnv;
extern MemoryContext JavaMemoryContext;

extern PGDLLIMPORT char* stack_base_ptr;

#define STACK_BASE_VARS \
	long  saveMainThreadId = 0; \
	char* saveStackBasePtr = 0;

#define STACK_BASE_PUSH(threadId) \
	if(threadId != mainThreadId) \
	{ \
		saveStackBasePtr = stack_base_ptr; \
		saveMainThreadId = mainThreadId; \
		stack_base_ptr = (char*)&saveMainThreadId; \
		mainThreadId = threadId; \
		elog(DEBUG1, "Changed stack_base_ptr from %p to %p", saveStackBasePtr, stack_base_ptr); \
	}

#define STACK_BASE_POP() \
	if(saveStackBasePtr != 0) \
	{ \
		stack_base_ptr = saveStackBasePtr; \
		mainThreadId = saveMainThreadId; \
		elog(DEBUG1, "Restored stack_base_ptr to %p", saveStackBasePtr); \
	}

/* NOTE!
 * When using the PG_TRY, PG_CATCH, PG_TRY_END family of macros,
 * it is an ABSOLUTE NECESSITY to use the PG_TRY_RETURN or
 * PG_TRY_RETURN_VOID in place of any return.
 */
#define PG_TRY_POP \
	PG_exception_stack = save_exception_stack; \
	error_context_stack = save_context_stack

#define PG_TRY_RETURN(retVal) { PG_TRY_POP; return retVal; }
#define PG_TRY_RETURN_VOID { PG_TRY_POP; return; }

/* Some error codes missing from errcodes.h
 * 
 * Class 07 - Dynamic SQL Exception
 */
#define ERRCODE_INVALID_DESCRIPTOR_INDEX		MAKE_SQLSTATE('0','7', '0','0','9')

/*
 * Union used when coercing void* to jlong and vice versa
 */
typedef union
{
	void*  ptrVal;
	jlong  longVal; /* 64 bit quantity */
	struct
	{
		/* Used when calculating pointer hash in systems where
		 * a pointer is 64 bit
		 */
		uint32 intVal_1;
		uint32 intVal_2;
	} x64;
} Ptr2Long;

struct Invocation_;
typedef struct Invocation_ Invocation;

struct Function_;
typedef struct Function_* Function;

#ifdef __cplusplus
}
#endif
#endif
