//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		pthreadwrapper.cpp
//
//	@doc:
//		Wrapper for functions in pthread library
//
//---------------------------------------------------------------------------

#include <errno.h>

#include "gpos/assert.h"
#include "gpos/utils.h"
#include "gpos/common/pthreadwrapper.h"
#include "gpos/task/IWorker.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		FValidMutexType
//
//	@doc:
//		Return the specified type is a valid mutex type or not
//
//---------------------------------------------------------------------------
BOOL
gpos::pthread::FValidMutexType
	(
	INT type
	)
{
	return
		PTHREAD_MUTEX_NORMAL == (type) ||
		PTHREAD_MUTEX_ERRORCHECK == (type) ||
		PTHREAD_MUTEX_RECURSIVE == (type) ||
		PTHREAD_MUTEX_DEFAULT == (type) ;
}


//---------------------------------------------------------------------------
//	@function:
//		FValidDetachedStat
//
//	@doc:
//		Return the specified state is a valid detached state or not
//
//---------------------------------------------------------------------------
BOOL
gpos::pthread::FValidDetachedStat
	(
	INT state
	)
{
	return
		PTHREAD_CREATE_DETACHED == (state) ||
		PTHREAD_CREATE_JOINABLE == (state);
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::IPthreadAttrInit
//
//	@doc:
//		Initialize the thread attributes object
//
//---------------------------------------------------------------------------
INT
gpos::pthread::IPthreadAttrInit
	(
	PTHREAD_ATTR_T *pthrAttr
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != pthrAttr);

	INT iRes = pthread_attr_init(pthrAttr);

	GPOS_ASSERT
	(
		0 == iRes ||
		(EINTR != iRes && "Unexpected Error")
	);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::PthreadAttrDestroy
//
//	@doc:
//		Destroy the thread attributes object
//
//---------------------------------------------------------------------------
void
gpos::pthread::PthreadAttrDestroy
	(
	PTHREAD_ATTR_T *pthrAttr
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != pthrAttr);

#ifdef GPOS_DEBUG
	INT iRes =
#endif // GPOS_DEBUG
	pthread_attr_destroy(pthrAttr);

	GPOS_ASSERT(0 == iRes && "function pthread_attr_destroy() failed");

}


//---------------------------------------------------------------------------
//	@function:
//		pthread::IPthreadAttrGetDetachState
//
//	@doc:
//		Get the detachstate attribute
//
//---------------------------------------------------------------------------
INT
gpos::pthread::IPthreadAttrGetDetachState
	(
	const PTHREAD_ATTR_T *pthrAttr,
	INT *piDetachstate
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != pthrAttr);
	GPOS_ASSERT(NULL != piDetachstate);

	INT iRes = pthread_attr_getdetachstate(pthrAttr, piDetachstate);

	GPOS_ASSERT
	(
		(0 == iRes && FValidDetachedStat(*piDetachstate)) ||
		(0 != iRes && EINTR != iRes && "Unexpected Error")
	);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::IPthreadAttrSetDetachState
//
//	@doc:
//		Set the detachstate attribute
//
//---------------------------------------------------------------------------
INT
gpos::pthread::IPthreadAttrSetDetachState
	(
	PTHREAD_ATTR_T *pthrAttr,
	INT iDetachstate
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != pthrAttr);
	GPOS_ASSERT(FValidDetachedStat(iDetachstate));

	INT iRes = pthread_attr_setdetachstate(pthrAttr, iDetachstate);

	GPOS_ASSERT
	(
		0 == iRes ||
		(EINTR != iRes && "Unexpected Error")
	);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::IPthreadMutexAttrInit
//
//	@doc:
//		Initialize the mutex attributes object
//
//---------------------------------------------------------------------------
INT
gpos::pthread::IPthreadMutexAttrInit
	(
	PTHREAD_MUTEXATTR_T *pmatAttr
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != pmatAttr);

	INT iRes = pthread_mutexattr_init(pmatAttr);

	GPOS_ASSERT(0 == iRes || ENOMEM == iRes);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::PthreadMutexAttrDestroy
//
//	@doc:
//		Destroy the mutex attributes object
//
//---------------------------------------------------------------------------
void
gpos::pthread::PthreadMutexAttrDestroy
	(
	PTHREAD_MUTEXATTR_T *pmatAttr
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != pmatAttr);

#ifdef GPOS_DEBUG
	INT iRes =
#endif // GPOS_DEBUG
	pthread_mutexattr_destroy(pmatAttr);

	GPOS_ASSERT(0 == iRes && "function pthread_mutexattr_destroy() failed");
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::IPthreadMutexAttrGettype
//
//	@doc:
//		Get the mutex type attribute
//
//---------------------------------------------------------------------------
void
gpos::pthread::PthreadMutexAttrGettype
	(
	const PTHREAD_MUTEXATTR_T *pmatAttr,
	INT *piType
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != pmatAttr);
	GPOS_ASSERT(NULL != piType);

#ifdef GPOS_DEBUG
	INT iRes =
#endif // GPOS_DEBUG
#ifdef GPOS_FreeBSD
	pthread_mutexattr_gettype(const_cast<PTHREAD_MUTEXATTR_T*>(pmatAttr), piType);
#else  // !GPOS_FreeBSD
	pthread_mutexattr_gettype(pmatAttr, piType);
#endif

	GPOS_ASSERT(0 == iRes && FValidMutexType(*piType));

}


//---------------------------------------------------------------------------
//	@function:
//		pthread::IPthreadMutexAttrSettype
//
//	@doc:
//		Set the mutex type attribute
//
//---------------------------------------------------------------------------
void
gpos::pthread::PthreadMutexAttrSettype
	(
	PTHREAD_MUTEXATTR_T *pmatAttr,
	INT iType
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != pmatAttr);
	GPOS_ASSERT(FValidMutexType(iType));

#ifdef GPOS_DEBUG
	INT iRes =
#endif // GPOS_DEBUG
	pthread_mutexattr_settype(pmatAttr, iType);

	GPOS_ASSERT(0 == iRes);
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::IPthreadMutexInit
//
//	@doc:
//		Initialize a mutex
//
//---------------------------------------------------------------------------
INT
gpos::pthread::IPthreadMutexInit
	(
	PTHREAD_MUTEX_T *ptmutex,
	const PTHREAD_MUTEXATTR_T *pmatAttr
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != pmatAttr);
	GPOS_ASSERT(NULL != ptmutex);

	INT iRes = pthread_mutex_init(ptmutex, pmatAttr);

	GPOS_ASSERT
	(
		0 == iRes ||
		(EINVAL != iRes && "Invalid mutex attr") ||
		(EBUSY != iRes && "Attempt to reinitialize") ||
		(EINTR != iRes && "Unexpected Error")
	);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::PthreadMutexDestroy
//
//	@doc:
//		Destroy a mutex
//
//---------------------------------------------------------------------------
void
gpos::pthread::PthreadMutexDestroy
	(
	PTHREAD_MUTEX_T *ptmutex
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != ptmutex);

#ifdef GPOS_DEBUG
	INT iRes =
#endif // GPOS_DEBUG
	pthread_mutex_destroy(ptmutex);

	GPOS_ASSERT(0 == iRes && "function pthread_mutex_destroy() failed");
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::IPthreadMutexLock
//
//	@doc:
//		Lock a mutex
//
//---------------------------------------------------------------------------
INT
gpos::pthread::IPthreadMutexLock
	(
	PTHREAD_MUTEX_T *ptmutex
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != ptmutex);

	INT iRes = pthread_mutex_lock(ptmutex);

	GPOS_ASSERT
	(
		iRes == 0 ||
		(EINVAL != iRes && "Uninitialized mutex structure") ||
		(EDEADLK != iRes && "The thread already owned the mutex") ||
		(EINTR != iRes && "Unexpected Error")
	);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::IPthreadMutexTryLock
//
//	@doc:
//	        Try lock a mutex
//
//---------------------------------------------------------------------------
INT
gpos::pthread::IPthreadMutexTryLock
	(
	PTHREAD_MUTEX_T *ptmutex
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != ptmutex);

	INT iRes = pthread_mutex_trylock(ptmutex);

	GPOS_ASSERT
	(
		0 == iRes ||
		(EINVAL != iRes && "Uninitialized mutex structure") ||
		(EINTR != iRes && "Unexpected Error")
	);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::IPthreadMutexUnlock
//
//	@doc:
//		Unlock a mutex
//
//---------------------------------------------------------------------------
INT
gpos::pthread::IPthreadMutexUnlock
	(
	PTHREAD_MUTEX_T *ptmutex
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != ptmutex);

	INT iRes = pthread_mutex_unlock(ptmutex);

	GPOS_ASSERT
	(
		0 == iRes ||
		(EINVAL != iRes && "Uninitialized mutex structure") ||
		(EPERM != iRes && "Mutex was not owned by thread") ||
		(EINTR != iRes && "Unexpected Error")
	);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::IPthreadMutexTimedlock
//
//	@doc:
//		Lock the mutex object referenced by ptmutex with timeout
//
//---------------------------------------------------------------------------
#ifndef GPOS_Darwin
INT
gpos::pthread::IPthreadMutexTimedlock
	(
	PTHREAD_MUTEX_T *ptmutex,
	const TIMESPEC *ptsTimeout
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != ptmutex);

	INT iRes = pthread_mutex_timedlock(ptmutex, ptsTimeout);
	GPOS_ASSERT
	(
		0 == iRes ||
		(EINVAL != iRes && "Invalid mutex structure") ||
		(EINTR != iRes && "Unexpected Error")
	);

	return iRes;
}
#endif


//---------------------------------------------------------------------------
//	@function:
//		pthread::IPthreadCondInit
//
//	@doc:
//		Initialize condition variables
//
//---------------------------------------------------------------------------
INT
gpos::pthread::IPthreadCondInit
	(
	PTHREAD_COND_T *__restrict ptcond,
	const PTHREAD_CONDATTR_T *__restrict pcatAttr
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != ptcond);

	INT iRes = pthread_cond_init(ptcond, pcatAttr);

	GPOS_ASSERT
	(
		0 == iRes ||
		(EINVAL != iRes && "Invalid condition attr") ||
		(EBUSY != iRes && "Attempt to reinitialize") ||
		(EINTR != iRes && "Unexpected Error")
	);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::PthreadCondDestroy
//
//	@doc:
//		Destroy condition variables
//
//---------------------------------------------------------------------------
void
gpos::pthread::PthreadCondDestroy
	(
	PTHREAD_COND_T *ptcond
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != ptcond);

#ifdef GPOS_DEBUG
	INT iRes =
#endif // GPOS_DEBUG
	pthread_cond_destroy(ptcond);

	GPOS_ASSERT(0 == iRes && "function pthread_attr_destroy() failed");
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::IPthreadCondBroadcast
//
//	@doc:
//		Broadcast a condition
//
//---------------------------------------------------------------------------
INT
gpos::pthread::IPthreadCondBroadcast
	(
	PTHREAD_COND_T *ptcond
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != ptcond);

	INT iRes = pthread_cond_broadcast(ptcond);

	GPOS_ASSERT
	(
		0 == iRes ||
		(EINVAL != iRes && "Uninitialized condition structure") ||
		(EINTR != iRes && "Unexpected Error")
	);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::IPthreadCondSignal
//
//	@doc:
//		Signal a condition
//
//---------------------------------------------------------------------------
INT
gpos::pthread::IPthreadCondSignal
	(
	PTHREAD_COND_T *ptcond
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != ptcond);

	INT iRes = pthread_cond_signal(ptcond);

	GPOS_ASSERT
	(
		0 == iRes ||
		(EINVAL != iRes && "Uninitialized condition structure") ||
		(EINTR != iRes && "Unexpected Error")
	);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::IPthreadCondTimedWait
//
//	@doc:
//		Wait on a condition
//
//---------------------------------------------------------------------------
INT
gpos::pthread::IPthreadCondTimedWait
	(
	PTHREAD_COND_T *__restrict ptcond,
	PTHREAD_MUTEX_T *__restrict ptmutex,
	const TIMESPEC *__restrict ptsAbsTime
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != ptcond);

	INT iRes = pthread_cond_timedwait(ptcond, ptmutex, ptsAbsTime);

	GPOS_ASSERT
	(
		0 == iRes ||
		(EINVAL != iRes && "Invalid parameters") ||
		(EPERM != iRes && "Mutex was not owned by thread") ||
		(EINTR != iRes && "Unexpected Error")
	);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::IPthreadCondWait
//
//	@doc:
//		Signal a condition
//
//---------------------------------------------------------------------------
INT
gpos::pthread::IPthreadCondWait
	(
	PTHREAD_COND_T *__restrict ptcond,
	PTHREAD_MUTEX_T *__restrict ptmutex
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != ptcond);

	INT iRes = pthread_cond_wait(ptcond, ptmutex);

	GPOS_ASSERT
	(
		0 == iRes ||
		(EINVAL != iRes && "Invalid parameters") ||
		(EPERM != iRes && "Mutex was not owned by thread") ||
		(EINTR != iRes && "Unexpected Error")
	);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::IPthreadCreate
//
//	@doc:
//		thread creation
//
//---------------------------------------------------------------------------
INT
gpos::pthread::IPthreadCreate
	(
	PTHREAD_T *__restrict pthrdt,
	const PTHREAD_ATTR_T *__restrict pthrAttr,
	PFnPthreadExec fnPthreadFunx,
	void *__restrict pvArg
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != pthrdt);

	INT iRes = pthread_create(pthrdt, pthrAttr, fnPthreadFunx, pvArg);

	GPOS_ASSERT
	(
		0 == iRes ||
		(EINVAL != iRes && "Invalid pthread attr") ||
		(EINTR != iRes && "Unexpected Error")
	);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::IPthreadJoin
//
//	@doc:
//		Wait for thread termination
//
//---------------------------------------------------------------------------
INT
gpos::pthread::IPthreadJoin
	(
	PTHREAD_T thread,
	void **ppvValue
	)
{
	GPOS_ASSERT_NO_SPINLOCK;

	INT iRes = pthread_join(thread, ppvValue);

	GPOS_ASSERT
	(
		0 == iRes ||
		(EINVAL != iRes && "Thread is not joinable") ||
		(ESRCH != iRes && "No such thread") ||
		(EINTR != iRes && "Unexpected Error")
	);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::FPthreadEqual
//
//	@doc:
//		Compare thread IDs
//
//---------------------------------------------------------------------------
BOOL
gpos::pthread::FPthreadEqual
	(
	PTHREAD_T pthrdtLhs,
	PTHREAD_T pthrdtRhs
	)
{
	INT iRes = pthread_equal(pthrdtLhs, pthrdtRhs);

	GPOS_ASSERT(EINTR != iRes && "Unexpected Error");

	return 0 != iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::PthrdtPthreadSelf
//
//	@doc:
//		Get the calling thread ID
//
//---------------------------------------------------------------------------
PTHREAD_T
gpos::pthread::PthrdtPthreadSelf
	(
	)
{
	return pthread_self();
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::IPthreadSigMask
//
//	@doc:
//		Set signal mask for thread
//
//---------------------------------------------------------------------------
void
gpos::pthread::PthreadSigMask
	(
	INT iMode,
	const SIGSET_T *pset,
	SIGSET_T *psetOld
	)
{
#ifdef GPOS_DEBUG
	INT iRes =
#endif // GPOS_DEBUG
	pthread_sigmask(iMode, pset, psetOld);

	GPOS_ASSERT(0 == iRes);
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::SigEmptySet
//
//	@doc:
//		Initialize signal set to empty
//
//---------------------------------------------------------------------------
void
gpos::pthread::SigEmptySet
	(
	SIGSET_T *pset
	)
{
#ifdef GPOS_DEBUG
	INT iRes =
#endif // GPOS_DEBUG
	sigemptyset(pset);

	GPOS_ASSERT(0 == iRes);
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::SigAddSet
//
//	@doc:
//		Add signal to set
//
//---------------------------------------------------------------------------
void
gpos::pthread::SigAddSet
	(
	SIGSET_T *pset,
	INT iSignal
	)
{
#ifdef GPOS_DEBUG
	INT iRes =
#endif // GPOS_DEBUG
	sigaddset(pset, iSignal);

	GPOS_ASSERT(0 == iRes);
}


// EOF

