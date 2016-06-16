//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CThreadManager.cpp
//
//	@doc:
//		Implementation of interface class for thread management.
//---------------------------------------------------------------------------

#include "gpos/task/CThreadManager.h"
#include "gpos/task/CWorkerPoolManager.h"
#include "gpos/common/pthreadwrapper.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CThreadManager::CThreadManager
//
//	@doc:
//		Ctor;
//
//---------------------------------------------------------------------------
CThreadManager::CThreadManager()
{
	// initialize lists
	m_tdlUnused.Init(GPOS_OFFSET(SThreadDescriptor, m_link));
	m_tdlRunning.Init(GPOS_OFFSET(SThreadDescriptor, m_link));
	m_tdlFinished.Init(GPOS_OFFSET(SThreadDescriptor, m_link));

	// initialize pthread attribute
	if (0 != pthread::IPthreadAttrInit(&m_pthrAttr))
	{
		// raise OOM exception
		GPOS_OOM_CHECK(NULL);
	}

	if (0 != pthread::IPthreadAttrSetDetachState(&m_pthrAttr, PTHREAD_CREATE_JOINABLE))
	{
		// release pthread attribute
		pthread::PthreadAttrDestroy(&m_pthrAttr);

		// raise OOM exception
		GPOS_OOM_CHECK(NULL);
	}

	// add thread descriptors to unused list
	for (ULONG i = 0; i < GPOS_THREAD_MAX; i++)
	{
		m_rgTd[i].ulId = i + 1;
		m_tdlUnused.Append(&m_rgTd[i]);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CThreadManager::~CThreadManager
//
//	@doc:
//		Dtor;
//
//---------------------------------------------------------------------------
CThreadManager::~CThreadManager()
{
	pthread::PthreadAttrDestroy(&m_pthrAttr);
}

//---------------------------------------------------------------------------
//	@function:
//		CThreadManager::PtdNextUnused
//
//	@doc:
//		Create new thread;
//
//---------------------------------------------------------------------------
GPOS_RESULT
CThreadManager::EresCreate()
{
	SThreadDescriptor *ptd = NULL;

	// scope for lock
	{
		CAutoMutex am(m_mutex);
		am.Lock();

		// if all thread descriptors are used, try to claim some back
		if (m_tdlUnused.FEmpty())
		{
			GC();
		}

		GPOS_ASSERT(!m_tdlUnused.FEmpty() && "No unused thread descriptor");

		// get thread descriptor for new thread
		ptd = m_tdlUnused.RemoveHead();

		// attempt to create thread
		INT res = pthread::IPthreadCreate(&ptd->m_pthrdt, NULL /*pthrAttr*/, RunWorker, ptd);

		// check for error
		if (0 != res)
		{
			// return thread descriptor to unused list
			m_tdlUnused.Prepend(ptd);

			return GPOS_FAILED;
		}

		SetSignalMask();

		// add thread descriptor to running list
		m_tdlRunning.Append(ptd);

		return GPOS_OK;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CThreadManager::RunWorker()
//
//	@doc:
//		Run worker;
// 		Function run by threads;
//
//---------------------------------------------------------------------------
void *
CThreadManager::RunWorker
	(
	void *pv
	)
{
	SThreadDescriptor *ptd = reinterpret_cast<SThreadDescriptor *>(pv);
	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();

	// use try/catch block instead of GPOS_TRY/GPOS_CATCH
	// to catch any possible exception (not only GPOS-defined)
	try
	{
		// assign worker to thread
		CWorker wrkr(ptd->ulId, GPOS_WORKER_STACK_SIZE, (ULONG_PTR) &ptd);

		// run task execution loop
		wrkr.Run();

		// mark thread as finished
		pwpm->m_tm.SetFinished(ptd);
	}
	catch(CException ex)
	{
		std::cerr
			<< "Unexpected exception reached top of execution stack:"
			<< " major=" << ex.UlMajor()
			<< " minor=" << ex.UlMinor()
			<< " file=" << ex.SzFilename()
			<< " line=" << ex.UlLine()
			<< std::endl;

		GPOS_ASSERT(!"Unexpected exception reached top of execution stack");
	}
	catch(...)
	{
		GPOS_ASSERT(!"Unexpected exception reached top of execution stack");
	}

	// destroy thread
	return NULL;

}


//---------------------------------------------------------------------------
//	@function:
//		CThreadManager::SetFinished
//
//	@doc:
//		Mark thread as finished
//
//---------------------------------------------------------------------------
void
CThreadManager::SetFinished
	(
	SThreadDescriptor *ptd
	)
{
	CAutoMutex am(m_mutex);
	am.Lock();

	// run garbage collection
	GC();

	// move thread descriptor from running list to completed list
	m_tdlRunning.Remove(ptd);
	m_tdlFinished.Append(ptd);
}


//---------------------------------------------------------------------------
//	@function:
//		CThreadManager::GC
//
//	@doc:
//		Garbage Collection;
// 		Join finished threads and recycle their descriptors;
//		Caller assumed to hold a lock
//
//---------------------------------------------------------------------------
void
CThreadManager::GC()
{
	while (!m_tdlFinished.FEmpty())
	{
		// get descriptor of finished thread
		SThreadDescriptor *ptd = m_tdlFinished.RemoveHead();

		// join thread
		pthread::IPthreadJoin(ptd->m_pthrdt, NULL);

		// add thread descriptor to unused list
		m_tdlUnused.Prepend(ptd);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CThreadManager::ShutDown
//
//	@doc:
//		Shutdown thread manager;
// 		Return when all threads are joined (exit);
//
//---------------------------------------------------------------------------
void
CThreadManager::ShutDown()
{
	while (true)
	{
		clib::USleep(100);

		CAutoMutex am(m_mutex);
		am.Lock();

		// run garbage collection to join any remaining thread
		GC();

		// check if all threads have exited
		if (GPOS_THREAD_MAX == m_tdlUnused.UlSize())
		{
			break;
		}
	}

	GPOS_ASSERT(m_tdlRunning.FEmpty());
	GPOS_ASSERT(m_tdlFinished.FEmpty());
}


//---------------------------------------------------------------------------
//	@function:
//		CThreadManager::SetSignalMask
//
//	@doc:
//		Set signal mask on current thread
//
//---------------------------------------------------------------------------
void
CThreadManager::SetSignalMask()
{
	SIGSET_T sigset;
	pthread::SigEmptySet(&sigset);

	// thread blocks the following signals;
	// main thread handles these;
	pthread::SigAddSet(&sigset, SIGHUP);
	pthread::SigAddSet(&sigset, SIGINT);
	pthread::SigAddSet(&sigset, SIGALRM);
	pthread::SigAddSet(&sigset, SIGPIPE);
	pthread::SigAddSet(&sigset, SIGUSR1);
	pthread::SigAddSet(&sigset, SIGUSR2);
	pthread::SigAddSet(&sigset, SIGTERM);
	pthread::SigAddSet(&sigset, SIGQUIT);

	pthread::PthreadSigMask(SIG_BLOCK, &sigset, NULL /*psetOld*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CThreadManager::FRunningThread
//
//	@doc:
//		Check if given thread is in the running threads list
//
//---------------------------------------------------------------------------
BOOL
CThreadManager::FRunningThread
	(
	PTHREAD_T pthrdt
	)
{
	SThreadDescriptor *ptd = m_tdlRunning.PtFirst();
	while (NULL != ptd)
	{
		if (pthrdt == ptd->m_pthrdt)
		{
			return true;
		}

		ptd = m_tdlRunning.PtNext(ptd);
	}

	return false;

}

// EOF

