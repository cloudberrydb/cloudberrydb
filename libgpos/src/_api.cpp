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

#ifdef GPOS_DEBUG
bool fEnableExtendedAsserts = false;  // by default, extended asserts are disabled
#endif // GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		gpos_init
//
//	@doc:
//		Initialize GPOS memory pool, worker pool and message repository
//
//---------------------------------------------------------------------------
void gpos_init(struct gpos_init_params* params) {

	void* (*pfnAlloc) (SIZE_T) = params->alloc;
	void (*pfnFree) (void*) = params->free;

	if (NULL == pfnAlloc || NULL == pfnFree) {
	  pfnAlloc = clib::PvMalloc;
	  pfnFree = clib::Free;
	}

	CWorker::pfnAbortRequestedBySystem = params->abort_requested;

	if (GPOS_OK != gpos::CMemoryPoolManager::EresInit(pfnAlloc, pfnFree))
	{
		return;
	}

	if (GPOS_OK != gpos::CWorkerPoolManager::EresInit(0,0))
	{
		CMemoryPoolManager::Pmpm()->Shutdown();
		return;
	}

	if (GPOS_OK != gpos::CMessageRepository::EresInit())
	{
		CWorkerPoolManager::Pwpm()->Shutdown();
		CMemoryPoolManager::Pmpm()->Shutdown();
		return;
	}

	if (GPOS_OK != gpos::CCacheFactory::EresInit())
	{
		return;
	}

#ifdef GPOS_FPSIMULATOR
	if (GPOS_OK != gpos::CFSimulator::EresInit())
	{
		CMessageRepository::Pmr()->Shutdown();
		CWorkerPoolManager::Pwpm()->Shutdown();
		CMemoryPoolManager::Pmpm()->Shutdown();
	}
#endif // GPOS_FPSIMULATOR
}

//---------------------------------------------------------------------------
//	@function:
//		gpos_exec
//
//	@doc:
//		Set number of threads in worker pool;
//		return 0 for successful completion, 1 for error;
//		if any exception happens re-throw it.
//
//---------------------------------------------------------------------------
int gpos_set_threads(int min, int max)
{
	try
	{
		CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();

		// check if worker pool is initialized
		if (NULL == pwpm)
		{
			return 1;
		}

		pwpm->SetWorkersMin(min);
		pwpm->SetWorkersMax(max);
	}
	catch (...)
	{
		// unexpected failure
		return 1;
	}

	return 0;
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
		CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();

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
		CWorker wrkr(0, GPOS_WORKER_STACK_SIZE, (ULONG_PTR) pvStackStart);

		// scope for memory pool
		{
			// setup task memory
			CAutoMemoryPool amp(CAutoMemoryPool::ElcStrict);
			IMemoryPool *pmp = amp.Pmp();

			// scope for ATP
			{
				// task handler for this process
				CAutoTaskProxy atp(pmp, pwpm, true /*fPropagateError*/);

				CTask *ptsk = atp.PtskCreate(params->func, params->arg, params->abort_requested);

				// init TLS
				ptsk->Tls().Reset(pmp);

				CAutoP<CWStringStatic> apwstr;
				CAutoP<COstreamString> aposs;
				CAutoP<CLoggerStream> aplogger;

				// use passed buffer for logging
				if (NULL != params->error_buffer)
				{
					GPOS_ASSERT(0 < params->error_buffer_size);

					apwstr = GPOS_NEW(pmp) CWStringStatic
						(
						(WCHAR *) params->error_buffer,
						params->error_buffer_size / GPOS_SIZEOF(WCHAR)
						);
					aposs = GPOS_NEW(pmp) COstreamString(apwstr.Pt());
					aplogger = GPOS_NEW(pmp) CLoggerStream(*aposs.Pt());

					CTaskContext *ptskctxt = ptsk->Ptskctxt();
					ptskctxt->SetLogOut(aplogger.Pt());
					ptskctxt->SetLogErr(aplogger.Pt());
				}

				// execute function
				atp.Execute(ptsk);

				// export task result
				params->result = ptsk->PvRes();

				// check for errors during execution
				if (CTask::EtsError == ptsk->Ets())
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
			<< " major=" << ex.UlMajor()
			<< " minor=" << ex.UlMinor()
			<< " file=" << ex.SzFilename()
			<< " line=" << ex.UlLine()
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
	CFSimulator::Pfsim()->Shutdown();
#endif // GPOS_FPSIMULATOR
	CMessageRepository::Pmr()->Shutdown();
	CWorkerPoolManager::Pwpm()->Shutdown();
	CCacheFactory::Pcf()->Shutdown();
	CMemoryPoolManager::Pmpm()->Shutdown();
#endif // GPOS_DEBUG
}

// EOF

