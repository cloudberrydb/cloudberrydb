//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		ITask.h
//
//	@doc:
//		Interface class for task abstraction
//---------------------------------------------------------------------------
#ifndef GPOS_ITask_H
#define GPOS_ITask_H

#include "gpos/types.h"
#include "gpos/task/IWorker.h"

#include "gpos/task/traceflags.h"

// trace flag macro definitions
#define GPOS_FTRACE(x)       ITask::PtskSelf()->FTrace(x)
#define GPOS_SET_TRACE(x)    (void) ITask::PtskSelf()->FTrace(x, true /*fVal*/)
#define GPOS_UNSET_TRACE(x)  (void) ITask::PtskSelf()->FTrace(x, false /*fVal*/)

namespace gpos
{
	// forward declarations
	class ILogger;
	class IMemoryPool;
	class CTaskContext;
	class CTaskLocalStorage;
	class IErrorContext;
	
	
	class ITask
	{	
		
		private:

			// private copy ctor
			ITask(const ITask&);

		public:

			// task status
			enum ETaskStatus
			{
				EtsInit,		// task initialized but not scheduled
				EtsQueued,		// task added to scheduler's queue
				EtsDequeued,	// task removed from scheduler's queue, ready to run
				EtsRunning,		// task currently executing
				EtsCompleted,	// task completed executing with no error
				EtsError		// exception encountered while task was executed
			};

			// ctor
			ITask() {}
			
			// dtor
			virtual ~ITask() {}
			
			// accessor for memory pool, e.g. used for allocating task parameters in
			virtual 
			IMemoryPool *Pmp() const = 0;
			
			// TLS
			virtual
			CTaskLocalStorage &Tls()  = 0;

			// task context accessor
			virtual
			CTaskContext *Ptskctxt() const = 0;

			// basic output streams
			virtual 
			ILogger *PlogOut() const = 0;
			virtual
			ILogger *PlogErr() const = 0;
		
			// manipulate traceflags
			virtual
			BOOL FTrace(ULONG, BOOL) = 0;
			virtual
			BOOL FTrace(ULONG) = 0;

			// current locale
			virtual
			ELocale Eloc() const = 0;

			// error context
			virtual
			IErrorContext *Perrctxt() const = 0;
			
			// any pending exceptions?
			virtual
			BOOL FPendingExc() const = 0;
		
			static
			ITask *PtskSelf();

	}; // class ITask
}

#endif // !GPOS_ITask_H

// EOF

