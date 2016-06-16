//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		pthreadwrapper.h
//
//	@doc:
//		Wrapper for functions in pthread library
//
//---------------------------------------------------------------------------

#ifndef GPOS_pthreadwrapper_H
#define GPOS_pthreadwrapper_H

#include "gpos/types.h"
#include "gpos/common/clibtypes.h"
#include "gpos/common/pthreadtypes.h"

namespace gpos
{
	namespace pthread
	{

		// thread entry function declaration
		typedef void *(*PFnPthreadExec)(void*);

		// return the specified type is a valid mutex type or not
		BOOL FValidMutexType(INT type);

		// return the specified state is a valid detached state or not
		BOOL FValidDetachedStat(INT state);

		// initialize the thread attributes object
		INT IPthreadAttrInit(PTHREAD_ATTR_T *pthrAttr);

		// destroy the thread attributes object
		void PthreadAttrDestroy(PTHREAD_ATTR_T *pthrAttr);

		// get the detachstate attribute
		INT IPthreadAttrGetDetachState(const PTHREAD_ATTR_T *pthrAttr, INT *piDetachstate);

		// set the detachstate attribute
		INT IPthreadAttrSetDetachState(PTHREAD_ATTR_T *pthrAttr, INT iDetachstate);

		// initialize the mutex attributes object
		INT IPthreadMutexAttrInit(PTHREAD_MUTEXATTR_T *pmatAttr);

		// destroy the mutex attributes object
		void PthreadMutexAttrDestroy(PTHREAD_MUTEXATTR_T * pmatAttr);

		// get the mutex type attribute
		void PthreadMutexAttrGettype(const PTHREAD_MUTEXATTR_T *pmatAttr,INT *piType);

		// set the mutex type attribute
		void PthreadMutexAttrSettype(PTHREAD_MUTEXATTR_T *pmatAttr, INT iType);

		// initialize a mutex
		INT IPthreadMutexInit(PTHREAD_MUTEX_T *ptmutex, const PTHREAD_MUTEXATTR_T *pmatAttr);

		// destroy a mutex
		void PthreadMutexDestroy(PTHREAD_MUTEX_T *ptmutex);

		// lock a mutex
		INT IPthreadMutexLock(PTHREAD_MUTEX_T *ptmutex);

		// try lock a mutex
		INT IPthreadMutexTryLock(PTHREAD_MUTEX_T *ptmutex);

		// unlock a mutex
		INT IPthreadMutexUnlock(PTHREAD_MUTEX_T *ptmutex);

#ifndef GPOS_Darwin
		// lock the mutex object referenced by ptmutex with timeout
		INT IPthreadMutexTimedlock(PTHREAD_MUTEX_T *ptmutex, const TIMESPEC *ptsTimeout);
#endif

		// initialize condition variables
		INT IPthreadCondInit(PTHREAD_COND_T *__restrict ptcond, const PTHREAD_CONDATTR_T *__restrict pcatAttr);

		// destroy condition variables
		void PthreadCondDestroy(PTHREAD_COND_T *ptcond);

		// broadcast a condition
		INT IPthreadCondBroadcast(PTHREAD_COND_T *ptcond);

		// signal a condition
		INT IPthreadCondSignal(PTHREAD_COND_T *ptcond);

		// wait on a condition
		INT IPthreadCondTimedWait(PTHREAD_COND_T *__restrict ptcond, PTHREAD_MUTEX_T *__restrict ptmutex,
			const TIMESPEC *__restrict ptsAbsTime);

		// wait on a condition
		INT IPthreadCondWait(PTHREAD_COND_T *__restrict ptcond, PTHREAD_MUTEX_T *__restrict ptmutex);

		// thread creation
		INT IPthreadCreate(PTHREAD_T *__restrict pthrdt, const PTHREAD_ATTR_T *__restrict pthrAttr,
			PFnPthreadExec fnPthreadFunx, void *__restrict pvArg);

		// wait for thread termination
		INT IPthreadJoin(PTHREAD_T thread, void **ppvValue);

		// compare thread IDs
		BOOL FPthreadEqual(PTHREAD_T pthrdtLhs, PTHREAD_T pthrdtRhs);

		// get the calling thread ID
		PTHREAD_T PthrdtPthreadSelf();

		// set signal mask for thread
		void PthreadSigMask(INT iMode, const SIGSET_T *pset, SIGSET_T *psetOld);

		// initialize signal set to empty
		void SigEmptySet(SIGSET_T *set);

		// add signal to set
		void SigAddSet(SIGSET_T *set, INT iSignal);

	} // namespace plib
}

#endif

// EOF

