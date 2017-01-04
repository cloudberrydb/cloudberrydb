//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CException.cpp
//
//	@doc:
//		Implements simplified exception handling.
//---------------------------------------------------------------------------

#include "gpos/common/clibwrapper.h"
#include "gpos/error/CException.h"
#include "gpos/error/CErrorContext.h"
#include "gpos/task/CTask.h"

using namespace gpos;

const CHAR*
CException::m_rgszSeverity[] =
{
	"INVALID",
	"PANIC",
	"FATAL",
	"ERROR",
	"WARNING",
	"NOTICE",
	"TRACE"
};


// invalid exception
const CException CException::m_excInvalid
					(
					CException::ExmaInvalid,
					CException::ExmiInvalid
					);

// standard SQL error codes
const CException::SErrCodeElem
CException::m_rgerrcode[] =
{
	{ExmiSQLDefault, "XX000"},					// internal error
	{ExmiSQLNotNullViolation, "23502"},			// not null violation
	{ExmiSQLCheckConstraintViolation, "23514"},	// check constraint violation
	{ExmiSQLMaxOneRow, "P0003"},				// max one row
	{ExmiSQLTest, "XXXXX"}						// test sql state
};


//---------------------------------------------------------------------------
//	@function:
//		CException::CException
//
//	@doc:
//		Constructor for exception record; given the situation in which
//		exceptions are raised, init all elements, do not allocate any memory
//		dynamically
//
//---------------------------------------------------------------------------
CException::CException
	(
	ULONG ulMajor,
	ULONG ulMinor,
	const CHAR *szFilename,
	ULONG ulLine
	)
	:
	m_ulMajor(ulMajor),
	m_ulMinor(ulMinor),
	m_szFilename(const_cast<CHAR*>(szFilename)),
	m_ulLine(ulLine)
{
	m_szSQLState = SzSQLState(ulMajor, ulMinor);
}


//---------------------------------------------------------------------------
//	@function:
//		CException::CException
//
//	@doc:
//		Constructor for exception record; this version typically stored
//		in lookup structures etc.
//
//---------------------------------------------------------------------------
CException::CException
	(
	ULONG ulMajor,
	ULONG ulMinor
	)
	:
	m_ulMajor(ulMajor),
	m_ulMinor(ulMinor),
	m_szFilename(NULL),
	m_ulLine(0)
{
	m_szSQLState = SzSQLState(ulMajor, ulMinor);
}


//---------------------------------------------------------------------------
//	@function:
//		CException::Raise
//
//	@doc:
//		Actual point where an exception is thrown; encapsulated in a function 
//		(a) to facilitate debugging, i.e. function to set a breakpoint
//		(b) to allow for additional debugging tools such as stack dumps etc.
//			at a later point in time
//
//---------------------------------------------------------------------------
void
CException::Raise
	(
	const CHAR *szFilename,
	ULONG ulLine,
	ULONG ulMajor,
	ULONG ulMinor,
	...
	)
{
	// manufacture actual exception object
	CException exc(ulMajor, ulMinor, szFilename, ulLine);
	
	// during bootstrap there's no context object otherwise, record
	// all details in the context object
	if (NULL != ITask::PtskSelf())
	{
		CErrorContext *perrctxt = CTask::PtskSelf()->PerrctxtConvert();

		VA_LIST valist;
		VA_START(valist, ulMinor);

		perrctxt->Record(exc, valist);

		VA_END(valist);

		perrctxt->Serialize();
	}

	Raise(exc);
}



//---------------------------------------------------------------------------
//	@function:
//		CException::Reraise
//
//	@doc:
//		Throw/rethrow interface to reraise an already caught exc;
//		Wrapper that asserts there is a pending error;
//
//---------------------------------------------------------------------------
void
CException::Reraise
	(
	CException exc,
	BOOL fPropagate
	)
{
	if (NULL != ITask::PtskSelf())
	{
		CErrorContext *perrctxt = CTask::PtskSelf()->PerrctxtConvert();
		GPOS_ASSERT(perrctxt->FPending());

		perrctxt->SetRethrow();

		// serialize registered objects when current task propagates
		// an exception thrown by a child task
		if (fPropagate)
		{
			perrctxt->Psd()->BackTrace();
			perrctxt->Serialize();
		}
	}

	Raise(exc);
}


//---------------------------------------------------------------------------
//	@function:
//		CException::Raise
//
//	@doc:
//		Throw/rethrow interface
//
//---------------------------------------------------------------------------
void
CException::Raise
	(
	CException exc
	)
{
#ifdef GPOS_DEBUG
	if (NULL != ITask::PtskSelf())
	{
		IErrorContext *perrctxt = ITask::PtskSelf()->Perrctxt();
		GPOS_ASSERT_IMP(perrctxt->FPending(), 
				perrctxt->Exc() == exc &&
				"Rethrow inconsistent with current error context");
	}
#endif // GPOS_DEBUG
	
	throw exc;
}

//---------------------------------------------------------------------------
//	@function:
//		CException::SzSQLState
//
//	@doc:
//		Get sql state code for exception
//
//---------------------------------------------------------------------------
const CHAR *
CException::SzSQLState
	(
	ULONG ulMajor,
	ULONG ulMinor
	)
{
	const CHAR *szSQLState = m_rgerrcode[0].m_szSQLState;
	if (ExmaSQL == ulMajor)
	{
		ULONG ulSQLStates = GPOS_ARRAY_SIZE(m_rgerrcode);
		for (ULONG ul = 0; ul < ulSQLStates; ul++)
		{
			SErrCodeElem errcode = m_rgerrcode[ul];
			if (ulMinor == errcode.m_ul)
			{
				szSQLState = errcode.m_szSQLState;
				break;
			}
		}
	}
	
	return szSQLState;
}

// EOF

