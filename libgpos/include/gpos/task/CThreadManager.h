//--------------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CThreadManager.h
//
//	@doc:
//		Interface for thread management
//--------------------------------------------------------------------------------

#ifndef GPOS_CThreadManager_H
#define GPOS_CThreadManager_H

#include "gpos/common/CList.h"
#include "gpos/common/pthreadwrapper.h"
#include "gpos/sync/CAutoMutex.h"

#define GPOS_THREAD_MAX	(1024)	// max allowed number of threads

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CThreadManager
	//
	//	@doc:
	//		Thread management class; uses pthread library;
	//		Assigns thread descriptors to threads;
	//		Starts/joins threads;
	//
	//---------------------------------------------------------------------------
	class CThreadManager
	{
		public:

			// thread descriptor struct; encapsulates PTHREAD_T descriptor
			struct ThreadDescriptor
			{
					// pthread descriptor
					PTHREAD_T m_pthrdt;

					// thread id
					ULONG id;

					// list link
					SLink m_link;
			};

		private:

			typedef CList<ThreadDescriptor> TDL;

			// array of thread descriptors
			ThreadDescriptor m_thread_descriptors[GPOS_THREAD_MAX];

			// list of unused thread descriptors
			TDL m_unused_descriptors;

			// descriptor list of running threads
			TDL m_running_descriptors;

			// descriptor list of finished threads
			TDL m_finished_descriptors;

			// mutex
			CMutexOS m_mutex;

			// pthread attribute
			PTHREAD_ATTR_T m_pthread_attr;

			// run worker;
			// function run by threads
			// declared static to be used by pthread_create
			static void *RunWorker(void *worker);

			// mark thread as finished
			void SetFinished(ThreadDescriptor *descriptor);

			// garbage collection;
			// join finished threads and recycle their descriptors;
			void GC();

			// set signal mask on current thread
			static
			void SetSignalMask();

		public:

			// ctor
			CThreadManager();

			// no copy ctor
			CThreadManager(const CThreadManager&);

			// dtor
			~CThreadManager();

			// create new thread
			GPOS_RESULT Create();

			// check if given thread is in the running threads list
			BOOL IsThreadRunning(PTHREAD_T thread);

			// shutdown thread manager;
			// return when all threads are joined (exit);
			void ShutDown();

	}; // class CThreadManager

}

#endif // !GPOS_CThreadManager_H

// EOF

