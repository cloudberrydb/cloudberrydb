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
	m_unused_descriptors.Init(GPOS_OFFSET(ThreadDescriptor, m_link));
	m_running_descriptors.Init(GPOS_OFFSET(ThreadDescriptor, m_link));
	m_finished_descriptors.Init(GPOS_OFFSET(ThreadDescriptor, m_link));

	// initialize pthread attribute
	if (0 != pthread::AttrInit(&m_pthread_attr))
	{
		// raise OOM exception
		GPOS_OOM_CHECK(NULL);
	}

	if (0 != pthread::AttrSetDetachState(&m_pthread_attr, PTHREAD_CREATE_JOINABLE))
	{
		// release pthread attribute
		pthread::AttrDestroy(&m_pthread_attr);

		// raise OOM exception
		GPOS_OOM_CHECK(NULL);
	}

	// add thread descriptors to unused list
	for (ULONG i = 0; i < GPOS_THREAD_MAX; i++)
	{
		m_thread_descriptors[i].id = i + 1;
		m_unused_descriptors.Append(&m_thread_descriptors[i]);
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
	pthread::AttrDestroy(&m_pthread_attr);
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
CThreadManager::Create()
{
	ThreadDescriptor *descriptor = NULL;

	// scope for lock
	{
		CAutoMutex am(m_mutex);
		am.Lock();

		// if all thread descriptors are used, try to claim some back
		if (m_unused_descriptors.IsEmpty())
		{
			GC();
		}

		GPOS_ASSERT(!m_unused_descriptors.IsEmpty() && "No unused thread descriptor");

		// get thread descriptor for new thread
		descriptor = m_unused_descriptors.RemoveHead();

		// attempt to create thread
		INT res = pthread::Create(&descriptor->m_pthrdt, NULL /*pthrAttr*/, RunWorker, descriptor);

		// check for error
		if (0 != res)
		{
			// return thread descriptor to unused list
			m_unused_descriptors.Prepend(descriptor);

			return GPOS_FAILED;
		}

		SetSignalMask();

		// add thread descriptor to running list
		m_running_descriptors.Append(descriptor);

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
	void *worker
	)
{
	ThreadDescriptor *descriptor = reinterpret_cast<ThreadDescriptor *>(worker);
	CWorkerPoolManager *worker_pool_mgr = CWorkerPoolManager::WorkerPoolManager();

	// use try/catch block instead of GPOS_TRY/GPOS_CATCH
	// to catch any possible exception (not only GPOS-defined)
	try
	{
		// assign worker to thread
		CWorker new_worker(descriptor->id, GPOS_WORKER_STACK_SIZE, (ULONG_PTR) &descriptor);

		// run task execution loop
		new_worker.Run();

		// mark thread as finished
		worker_pool_mgr->m_thread_manager.SetFinished(descriptor);
	}
	catch(CException ex)
	{
		std::cerr
			<< "Unexpected exception reached top of execution stack:"
			<< " major=" << ex.Major()
			<< " minor=" << ex.Minor()
			<< " file=" << ex.Filename()
			<< " line=" << ex.Line()
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
	ThreadDescriptor *descriptor
	)
{
	CAutoMutex am(m_mutex);
	am.Lock();

	// run garbage collection
	GC();

	// move thread descriptor from running list to completed list
	m_running_descriptors.Remove(descriptor);
	m_finished_descriptors.Append(descriptor);
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
	while (!m_finished_descriptors.IsEmpty())
	{
		// get descriptor of finished thread
		ThreadDescriptor *descriptor = m_finished_descriptors.RemoveHead();

		// join thread
		pthread::Join(descriptor->m_pthrdt, NULL);

		// add thread descriptor to unused list
		m_unused_descriptors.Prepend(descriptor);
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
		if (GPOS_THREAD_MAX == m_unused_descriptors.Size())
		{
			break;
		}
	}

	GPOS_ASSERT(m_running_descriptors.IsEmpty());
	GPOS_ASSERT(m_finished_descriptors.IsEmpty());
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

	pthread::SigMask(SIG_BLOCK, &sigset, NULL /*psetOld*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CThreadManager::IsThreadRunning
//
//	@doc:
//		Check if given thread is in the running threads list
//
//---------------------------------------------------------------------------
BOOL
CThreadManager::IsThreadRunning
	(
	PTHREAD_T thread
	)
{
	ThreadDescriptor *descriptor = m_running_descriptors.First();
	while (NULL != descriptor)
	{
		if (thread == descriptor->m_pthrdt)
		{
			return true;
		}

		descriptor = m_running_descriptors.Next(descriptor);
	}

	return false;

}

// EOF

