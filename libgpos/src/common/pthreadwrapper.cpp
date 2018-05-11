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
//		MutexTypeIsValid
//
//	@doc:
//		Return the specified type is a valid mutex type or not
//
//---------------------------------------------------------------------------
BOOL
gpos::pthread::MutexTypeIsValid
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
//		IsValidDetachedState
//
//	@doc:
//		Return the specified state is a valid detached state or not
//
//---------------------------------------------------------------------------
BOOL
gpos::pthread::IsValidDetachedState
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
//		pthread::AttrInit
//
//	@doc:
//		Initialize the thread attributes object
//
//---------------------------------------------------------------------------
INT
gpos::pthread::AttrInit
	(
	PTHREAD_ATTR_T *attr
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != attr);

	INT res = pthread_attr_init(attr);

	GPOS_ASSERT
	(
		0 == res ||
		(EINTR != res && "Unexpected Error")
	);

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::AttrDestroy
//
//	@doc:
//		Destroy the thread attributes object
//
//---------------------------------------------------------------------------
void
gpos::pthread::AttrDestroy
	(
	PTHREAD_ATTR_T *attr
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != attr);

#ifdef GPOS_DEBUG
	INT res =
#endif // GPOS_DEBUG
	pthread_attr_destroy(attr);

	GPOS_ASSERT(0 == res && "function pthread_attr_destroy() failed");

}


//---------------------------------------------------------------------------
//	@function:
//		pthread::AttrGetDetachState
//
//	@doc:
//		Get the detachstate attribute
//
//---------------------------------------------------------------------------
INT
gpos::pthread::AttrGetDetachState
	(
	const PTHREAD_ATTR_T *attr,
	INT *state
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != attr);
	GPOS_ASSERT(NULL != state);

	INT res = pthread_attr_getdetachstate(attr, state);

	GPOS_ASSERT
	(
		(0 == res && IsValidDetachedState(*state)) ||
		(0 != res && EINTR != res && "Unexpected Error")
	);

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::AttrSetDetachState
//
//	@doc:
//		Set the detachstate attribute
//
//---------------------------------------------------------------------------
INT
gpos::pthread::AttrSetDetachState
	(
	PTHREAD_ATTR_T *attr,
	INT state
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != attr);
	GPOS_ASSERT(IsValidDetachedState(state));

	INT res = pthread_attr_setdetachstate(attr, state);

	GPOS_ASSERT
	(
		0 == res ||
		(EINTR != res && "Unexpected Error")
	);

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::MutexAttrInit
//
//	@doc:
//		Initialize the mutex attributes object
//
//---------------------------------------------------------------------------
INT
gpos::pthread::MutexAttrInit
	(
	PTHREAD_MUTEXATTR_T *attr
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != attr);

	INT res = pthread_mutexattr_init(attr);

	GPOS_ASSERT(0 == res || ENOMEM == res);

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::MutexAttrDestroy
//
//	@doc:
//		Destroy the mutex attributes object
//
//---------------------------------------------------------------------------
void
gpos::pthread::MutexAttrDestroy
	(
	PTHREAD_MUTEXATTR_T *attr
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != attr);

#ifdef GPOS_DEBUG
	INT res =
#endif // GPOS_DEBUG
	pthread_mutexattr_destroy(attr);

	GPOS_ASSERT(0 == res && "function pthread_mutexattr_destroy() failed");
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::MutexAttrGettype
//
//	@doc:
//		Get the mutex type attribute
//
//---------------------------------------------------------------------------
void
gpos::pthread::MutexAttrGettype
	(
	const PTHREAD_MUTEXATTR_T *attr,
	INT *type
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != attr);
	GPOS_ASSERT(NULL != type);

#ifdef GPOS_DEBUG
	INT res =
#endif // GPOS_DEBUG
#ifdef GPOS_FreeBSD
	pthread_mutexattr_gettype(const_cast<PTHREAD_MUTEXATTR_T*>(attr), type);
#else  // !GPOS_FreeBSD
	pthread_mutexattr_gettype(attr, type);
#endif

	GPOS_ASSERT(0 == res && MutexTypeIsValid(*type));

}


//---------------------------------------------------------------------------
//	@function:
//		pthread::MutexAttrSettype
//
//	@doc:
//		Set the mutex type attribute
//
//---------------------------------------------------------------------------
void
gpos::pthread::MutexAttrSettype
	(
	PTHREAD_MUTEXATTR_T *attr,
	INT type
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != attr);
	GPOS_ASSERT(MutexTypeIsValid(type));

#ifdef GPOS_DEBUG
	INT res =
#endif // GPOS_DEBUG
	pthread_mutexattr_settype(attr, type);

	GPOS_ASSERT(0 == res);
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::MutexInit
//
//	@doc:
//		Initialize a mutex
//
//---------------------------------------------------------------------------
INT
gpos::pthread::MutexInit
	(
	PTHREAD_MUTEX_T *mutex,
	const PTHREAD_MUTEXATTR_T *attr
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != attr);
	GPOS_ASSERT(NULL != mutex);

	INT res = pthread_mutex_init(mutex, attr);

	GPOS_ASSERT
	(
		0 == res ||
		(EINVAL != res && "Invalid mutex attr") ||
		(EBUSY != res && "Attempt to reinitialize") ||
		(EINTR != res && "Unexpected Error")
	);

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::MutexDestroy
//
//	@doc:
//		Destroy a mutex
//
//---------------------------------------------------------------------------
void
gpos::pthread::MutexDestroy
	(
	PTHREAD_MUTEX_T *mutex
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != mutex);

#ifdef GPOS_DEBUG
	INT res =
#endif // GPOS_DEBUG
	pthread_mutex_destroy(mutex);

	GPOS_ASSERT(0 == res && "function pthread_mutex_destroy() failed");
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::MutexLock
//
//	@doc:
//		Lock a mutex
//
//---------------------------------------------------------------------------
INT
gpos::pthread::MutexLock
	(
	PTHREAD_MUTEX_T *mutex
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != mutex);

	INT res = pthread_mutex_lock(mutex);

	GPOS_ASSERT
	(
		res == 0 ||
		(EINVAL != res && "Uninitialized mutex structure") ||
		(EDEADLK != res && "The thread already owned the mutex") ||
		(EINTR != res && "Unexpected Error")
	);

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::MutexTryLock
//
//	@doc:
//	        Try lock a mutex
//
//---------------------------------------------------------------------------
INT
gpos::pthread::MutexTryLock
	(
	PTHREAD_MUTEX_T *mutex
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != mutex);

	INT res = pthread_mutex_trylock(mutex);

	GPOS_ASSERT
	(
		0 == res ||
		(EINVAL != res && "Uninitialized mutex structure") ||
		(EINTR != res && "Unexpected Error")
	);

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::MutexUnlock
//
//	@doc:
//		Unlock a mutex
//
//---------------------------------------------------------------------------
INT
gpos::pthread::MutexUnlock
	(
	PTHREAD_MUTEX_T *mutex
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != mutex);

	INT res = pthread_mutex_unlock(mutex);

	GPOS_ASSERT
	(
		0 == res ||
		(EINVAL != res && "Uninitialized mutex structure") ||
		(EPERM != res && "GetMutex was not owned by thread") ||
		(EINTR != res && "Unexpected Error")
	);

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::MutexTimedlock
//
//	@doc:
//		Lock the mutex object referenced by mutex with timeout
//
//---------------------------------------------------------------------------
#ifndef GPOS_Darwin
INT
gpos::pthread::MutexTimedlock
	(
	PTHREAD_MUTEX_T *mutex,
	const TIMESPEC *timeout
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != mutex);

	INT res = pthread_mutex_timedlock(mutex, timeout);
	GPOS_ASSERT
	(
		0 == res ||
		(EINVAL != res && "Invalid mutex structure") ||
		(EINTR != res && "Unexpected Error")
	);

	return res;
}
#endif


//---------------------------------------------------------------------------
//	@function:
//		pthread::CondInit
//
//	@doc:
//		Initialize condition variables
//
//---------------------------------------------------------------------------
INT
gpos::pthread::CondInit
	(
	PTHREAD_COND_T *__restrict cond,
	const PTHREAD_CONDATTR_T *__restrict attr
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != cond);

	INT res = pthread_cond_init(cond, attr);

	GPOS_ASSERT
	(
		0 == res ||
		(EINVAL != res && "Invalid condition attr") ||
		(EBUSY != res && "Attempt to reinitialize") ||
		(EINTR != res && "Unexpected Error")
	);

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::CondDestroy
//
//	@doc:
//		Destroy condition variables
//
//---------------------------------------------------------------------------
void
gpos::pthread::CondDestroy
	(
	PTHREAD_COND_T *cond
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != cond);

#ifdef GPOS_DEBUG
	INT res =
#endif // GPOS_DEBUG
	pthread_cond_destroy(cond);

	GPOS_ASSERT(0 == res && "function pthread_attr_destroy() failed");
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::CondBroadcast
//
//	@doc:
//		Broadcast a condition
//
//---------------------------------------------------------------------------
INT
gpos::pthread::CondBroadcast
	(
	PTHREAD_COND_T *cond
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != cond);

	INT res = pthread_cond_broadcast(cond);

	GPOS_ASSERT
	(
		0 == res ||
		(EINVAL != res && "Uninitialized condition structure") ||
		(EINTR != res && "Unexpected Error")
	);

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::CondSignal
//
//	@doc:
//		Signal a condition
//
//---------------------------------------------------------------------------
INT
gpos::pthread::CondSignal
	(
	PTHREAD_COND_T *cond
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != cond);

	INT res = pthread_cond_signal(cond);

	GPOS_ASSERT
	(
		0 == res ||
		(EINVAL != res && "Uninitialized condition structure") ||
		(EINTR != res && "Unexpected Error")
	);

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::CondTimedWait
//
//	@doc:
//		Wait on a condition
//
//---------------------------------------------------------------------------
INT
gpos::pthread::CondTimedWait
	(
	PTHREAD_COND_T *__restrict cond,
	PTHREAD_MUTEX_T *__restrict mutex,
	const TIMESPEC *__restrict abstime
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != cond);

	INT res = pthread_cond_timedwait(cond, mutex, abstime);

	GPOS_ASSERT
	(
		0 == res ||
		(EINVAL != res && "Invalid parameters") ||
		(EPERM != res && "GetMutex was not owned by thread") ||
		(EINTR != res && "Unexpected Error")
	);

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::CondWait
//
//	@doc:
//		Signal a condition
//
//---------------------------------------------------------------------------
INT
gpos::pthread::CondWait
	(
	PTHREAD_COND_T *__restrict cond,
	PTHREAD_MUTEX_T *__restrict mutex
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != cond);

	INT res = pthread_cond_wait(cond, mutex);

	GPOS_ASSERT
	(
		0 == res ||
		(EINVAL != res && "Invalid parameters") ||
		(EPERM != res && "GetMutex was not owned by thread") ||
		(EINTR != res && "Unexpected Error")
	);

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::Create
//
//	@doc:
//		thread creation
//
//---------------------------------------------------------------------------
INT
gpos::pthread::Create
	(
	PTHREAD_T *__restrict thread,
	const PTHREAD_ATTR_T *__restrict attr,
	ExecFn func,
	void *__restrict arg
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != thread);

	INT res = pthread_create(thread, attr, func, arg);

	GPOS_ASSERT
	(
		0 == res ||
		(EINVAL != res && "Invalid pthread attr") ||
		(EINTR != res && "Unexpected Error")
	);

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::Join
//
//	@doc:
//		Wait for thread termination
//
//---------------------------------------------------------------------------
INT
gpos::pthread::Join
	(
	PTHREAD_T thread,
	void **retval
	)
{
	GPOS_ASSERT_NO_SPINLOCK;

	INT res = pthread_join(thread, retval);

	GPOS_ASSERT
	(
		0 == res ||
		(EINVAL != res && "Thread is not joinable") ||
		(ESRCH != res && "No such thread") ||
		(EINTR != res && "Unexpected Error")
	);

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::Equal
//
//	@doc:
//		Compare thread IDs
//
//---------------------------------------------------------------------------
BOOL
gpos::pthread::Equal
	(
	PTHREAD_T pthread1,
	PTHREAD_T pthread2
	)
{
	INT res = pthread_equal(pthread1, pthread2);

	GPOS_ASSERT(EINTR != res && "Unexpected Error");

	return 0 != res;
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::Self
//
//	@doc:
//		Get the calling thread ID
//
//---------------------------------------------------------------------------
PTHREAD_T
gpos::pthread::Self
	(
	)
{
	return pthread_self();
}


//---------------------------------------------------------------------------
//	@function:
//		pthread::SigMask
//
//	@doc:
//		Set signal mask for thread
//
//---------------------------------------------------------------------------
void
gpos::pthread::SigMask
	(
	INT mode,
	const SIGSET_T *set,
	SIGSET_T *oldset
	)
{
#ifdef GPOS_DEBUG
	INT res =
#endif // GPOS_DEBUG
	pthread_sigmask(mode, set, oldset);

	GPOS_ASSERT(0 == res);
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
	SIGSET_T *set
	)
{
#ifdef GPOS_DEBUG
	INT res =
#endif // GPOS_DEBUG
	sigemptyset(set);

	GPOS_ASSERT(0 == res);
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
	SIGSET_T *set,
	INT signum
	)
{
#ifdef GPOS_DEBUG
	INT res =
#endif // GPOS_DEBUG
	sigaddset(set, signum);

	GPOS_ASSERT(0 == res);
}


// EOF

