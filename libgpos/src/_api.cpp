//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2004-2015 Pivotal Software, Inc.
//
//	@filename:
//		_api.cpp
//
//	@doc:
//		Implementation of GPOS wrapper interface for GPDB.
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/_api.h"
#include "gpos/common/CAutoP.h"
#include "gpos/error/CFSimulator.h"
#include "gpos/error/CMessageRepository.h"
#include "gpos/error/CLoggerStream.h"
#include "gpos/io/COstreamString.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/memory/CCacheFactory.h"
#include "gpos/string/CWStringStatic.h"
#include "gpos/task/CAutoTaskProxy.h"
#include "gpos/task/CWorkerPoolManager.h"

#include "gpos/common/CMainArgs.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		gpos_init
//
//	@doc:
//		Initialize GPOS memory pool, worker pool and message repository
//
//---------------------------------------------------------------------------
void gpos_init(struct gpos_init_params* params) {

	void* (*func_ptr_alloc) (SIZE_T) = params->alloc;
	void (*func_ptr_free) (void*) = params->free;

	if (NULL == func_ptr_alloc || NULL == func_ptr_free) {
	  func_ptr_alloc = clib::Malloc;
	  func_ptr_free = clib::Free;
	}

	CWorker::abort_requested_by_system = params->abort_requested;

	if (GPOS_OK != gpos::CMemoryPoolManager::Init(func_ptr_alloc, func_ptr_free))
	{
		return;
	}

	if (GPOS_OK != gpos::CWorkerPoolManager::Init())
	{
		CMemoryPoolManager::GetMemoryPoolMgr()->Shutdown();
		return;
	}

	if (GPOS_OK != gpos::CMessageRepository::Init())
	{
		CWorkerPoolManager::WorkerPoolManager()->Shutdown();
		CMemoryPoolManager::GetMemoryPoolMgr()->Shutdown();
		return;
	}

	if (GPOS_OK != gpos::CCacheFactory::Init())
	{
		return;
	}

#ifdef GPOS_FPSIMULATOR
	if (GPOS_OK != gpos::CFSimulator::Init())
	{
		CMessageRepository::GetMessageRepository()->Shutdown();
		CWorkerPoolManager::WorkerPoolManager()->Shutdown();
		CMemoryPoolManager::GetMemoryPoolMgr()->Shutdown();
	}
#endif // GPOS_FPSIMULATOR
}

//---------------------------------------------------------------------------
//	@function:
//		gpos_exec
//
//	@doc:
//		Execute function as a GPOS task using current thread;
//		return 0 for successful completion, 1 for error;
//
//---------------------------------------------------------------------------
int gpos_exec
	(
	gpos_exec_params *params
	)
{
	// check if passed parameters are valid
	if (NULL == params || NULL == params->func)
	{
		return 1;
	}

	try
	{
		CWorkerPoolManager *pwpm = CWorkerPoolManager::WorkerPoolManager();

		// check if worker pool is initialized
		if (NULL == pwpm)
		{
			return 1;
		}

		// if no stack start address is passed, use address in current stack frame
		void *pvStackStart = params->stack_start;
		if (NULL == pvStackStart)
		{
			pvStackStart = &pwpm;
		}

		// put worker to stack - main thread has id '0'
		CWorker wrkr(GPOS_WORKER_STACK_SIZE, (ULONG_PTR) pvStackStart);

		// scope for memory pool
		{
			// setup task memory
			CAutoMemoryPool amp(CAutoMemoryPool::ElcStrict);
			CMemoryPool *mp = amp.Pmp();

			// scope for ATP
			{
				// task handler for this process
				CAutoTaskProxy atp(mp, pwpm, true /*fPropagateError*/);

				CTask *ptsk = atp.Create(params->func, params->arg, params->abort_requested);

				// init TLS
				ptsk->GetTls().Reset(mp);

				CAutoP<CWStringStatic> apwstr;
				CAutoP<COstreamString> aposs;
				CAutoP<CLoggerStream> aplogger;

				// use passed buffer for logging
				if (NULL != params->error_buffer)
				{
					GPOS_ASSERT(0 < params->error_buffer_size);

					apwstr = GPOS_NEW(mp) CWStringStatic
						(
						(WCHAR *) params->error_buffer,
						params->error_buffer_size / GPOS_SIZEOF(WCHAR)
						);
					aposs = GPOS_NEW(mp) COstreamString(apwstr.Value());
					aplogger = GPOS_NEW(mp) CLoggerStream(*aposs.Value());

					CTaskContext *ptskctxt = ptsk->GetTaskCtxt();
					ptskctxt->SetLogOut(aplogger.Value());
					ptskctxt->SetLogErr(aplogger.Value());
				}

				// execute function
				atp.Execute(ptsk);

				// export task result
				params->result = ptsk->GetRes();

				// check for errors during execution
				if (CTask::EtsError == ptsk->GetStatus())
				{
					return 1;
				}

			}
		}
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

		// unexpected failure
		throw ex;
	}
	catch (...)
	{
		// unexpected failure
		GPOS_RAISE(CException::ExmaUnhandled, CException::ExmiUnhandled);
	}

	return 0;
}


//---------------------------------------------------------------------------
//	@function:
//		gpos_terminate
//
//	@doc:
//		Shutdown GPOS memory pool, worker pool and message repository
//
//---------------------------------------------------------------------------
void gpos_terminate()
{
#ifdef GPOS_DEBUG
#ifdef GPOS_FPSIMULATOR
	CFSimulator::FSim()->Shutdown();
#endif // GPOS_FPSIMULATOR
	CMessageRepository::GetMessageRepository()->Shutdown();
	CWorkerPoolManager::WorkerPoolManager()->Shutdown();
	CCacheFactory::GetFactory()->Shutdown();
	CMemoryPoolManager::GetMemoryPoolMgr()->Shutdown();
#endif // GPOS_DEBUG
}

// EOF

