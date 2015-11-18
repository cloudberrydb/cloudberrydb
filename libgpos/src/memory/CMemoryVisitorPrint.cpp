//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMemoryVisitorPrint.cpp
//
//	@doc:
//		Implementation of memory object visitor for printing debug information
//		for all allocated objects inside a memory pool.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpos/assert.h"
#include "gpos/types.h"
#include "gpos/utils.h"
#include "gpos/common/CStackDescriptor.h"
#include "gpos/memory/CMemoryVisitorPrint.h"
#include "gpos/string/CWStringStatic.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CMemoryVisitorPrint::CMemoryVisitorPrint
//
//	@doc:
//	  Ctor.
//
//---------------------------------------------------------------------------
CMemoryVisitorPrint::CMemoryVisitorPrint
	(
	IOstream &os
	)
	:
	m_ullVisits(0),
	m_os(os)
{}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryVisitorPrint::~CMemoryVisitorPrint
//
//	@doc:
//	  Dtor.
//
//---------------------------------------------------------------------------
CMemoryVisitorPrint::~CMemoryVisitorPrint()
{}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryVisitorPrint::FVisit
//
//	@doc:
//		Prints the live object information to the output stream.
//
//---------------------------------------------------------------------------
void
CMemoryVisitorPrint::Visit
	(
	void *pvUserAddr,
	SIZE_T ulUserSize,
	void *pvTotalAddr,
	SIZE_T ulTotalSize,
	const CHAR * szAllocFilename,
	const ULONG ulAllocLine,
	ULLONG ullAllocSeqNumber,
	CStackDescriptor *psd
	)
{
	m_os
		<< COstream::EsmDec
		<< "allocation sequence number " << ullAllocSeqNumber << ","
		<< " total size " << (ULONG)ulTotalSize << " bytes,"
		<< " base address " << pvTotalAddr << ","
		<< " user size " << (ULONG)ulUserSize << " bytes,"
		<< " user address " << pvUserAddr << ","
		<< " allocated by " << szAllocFilename << ":" << ulAllocLine
		<< std::endl;

	ITask *ptsk = ITask::PtskSelf();
	if (NULL != ptsk)
	{
		if (NULL != psd && ptsk->FTrace(EtracePrintMemoryLeakStackTrace))
		{
			m_os << "Stack trace: " << std::endl;
			psd->AppendTrace(m_os, 8 /*ulDepth*/);
		}

		if (ptsk->FTrace(EtracePrintMemoryLeakDump))
		{
			m_os << "Memory dump: " << std::endl;
			gpos::HexDump(m_os, pvTotalAddr, ulTotalSize);
		}
	}
	
	++m_ullVisits;
}


// EOF
