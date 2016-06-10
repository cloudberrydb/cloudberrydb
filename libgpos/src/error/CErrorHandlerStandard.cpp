//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CErrorHandlerStandardStandard.cpp
//
//	@doc:
//		Implements standard error handler
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/error/CErrorContext.h"
#include "gpos/error/CErrorHandlerStandard.h"
#include "gpos/error/CLogger.h"
#include "gpos/string/CWStringStatic.h"
#include "gpos/task/CAutoSuspendAbort.h"
#include "gpos/task/CTask.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CErrorHandlerStandard::Process
//
//	@doc:
//		Process pending error context;
//
//---------------------------------------------------------------------------
void
CErrorHandlerStandard::Process
	(
	CException exc
	)
{
	CTask *ptsk = CTask::PtskSelf();

	GPOS_ASSERT(NULL != ptsk && "No task in current context");

	IErrorContext *perrctxt = ptsk->Perrctxt();
	CLogger *plog = dynamic_cast<CLogger*>(ptsk->PlogErr());
	
	GPOS_ASSERT(perrctxt->FPending() && "No error to process");
	GPOS_ASSERT(perrctxt->Exc() == exc && 
			"Exception processed different from pending");

	// print error stack trace
	if (CException::ExmaSystem == exc.UlMajor() && !perrctxt->FRethrow())
	{
		if ((CException::ExmiIOError == exc.UlMinor() ||
		    CException::ExmiNetError == exc.UlMinor() ) &&
			0 < errno)
		{
			perrctxt->AppendErrnoMsg();
		}

		if (ILogger::EeilMsgHeaderStack <= plog->Eil())
		{
			perrctxt->AppendStackTrace();
		}
	}

	// scope for suspending cancellation
	{
		// suspend cancellation
		CAutoSuspendAbort asa;

		// log error message
		plog->Log(perrctxt->WszMsg(), perrctxt->UlSev(), __FILE__, __LINE__);
	}
}

// EOF

