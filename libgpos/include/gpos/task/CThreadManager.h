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
			struct SThreadDescriptor
			{
					// pthread descriptor
					PTHREAD_T m_pthrdt;

					// thread id
					ULONG ulId;

					// list link
					SLink m_link;
			};

		private:

			typedef CList<SThreadDescriptor> TDL;

			// array of thread descriptors
			SThreadDescriptor m_rgTd[GPOS_THREAD_MAX];

			// list of unused thread descriptors
			TDL m_tdlUnused;

			// descriptor list of running threads
			TDL m_tdlRunning;

			// descriptor list of finished threads
			TDL m_tdlFinished;

			// mutex
			CMutexOS m_mutex;

			// pthread attribute
			PTHREAD_ATTR_T m_pthrAttr;

			// run worker;
			// function run by threads
			// declared static to be used by pthread_create
			static void *RunWorker(void *pv);

			// mark thread as finished
			void SetFinished(SThreadDescriptor *ptd);

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
			GPOS_RESULT EresCreate();

			// check if given thread is in the running threads list
			BOOL FRunningThread(PTHREAD_T pthrdt);

			// shutdown thread manager;
			// return when all threads are joined (exit);
			void ShutDown();

	}; // class CThreadManager

}

#endif // !GPOS_CThreadManager_H

// EOF

