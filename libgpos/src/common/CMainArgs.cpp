//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CMainArgs.cpp
//
//	@doc:
//		Implementation of getopt abstraction
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/common/CMainArgs.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CMainArgs::CMainArgs
//
//	@doc:
//		ctor -- saves off all opt params
//
//---------------------------------------------------------------------------
CMainArgs::CMainArgs
	(
	ULONG ulArgs,
	const CHAR **rgszArgs,
	const CHAR *szFmt
	)
	:
	m_ulArgs(ulArgs),
	m_rgszArgs(rgszArgs),
	m_szFmt(szFmt),
	m_szOptarg(optarg),
	m_iOptind(optind),
	m_iOptopt(optopt),
	m_iOpterr(opterr)
#ifdef GPOS_Darwin
	,
	m_iOptreset(optreset)
#endif // GPOS_Darwin
{
	// initialize external opt params
	optarg = NULL;
	optind = 1;
	optopt = 1;
	opterr = 1;
#ifdef GPOS_Darwin
	optreset = 1;
#endif // GPOS_Darwin
}


//---------------------------------------------------------------------------
//	@function:
//		CMainArgs::~CMainArgs
//
//	@doc:
//		dtor -- restore previous opt params
//
//---------------------------------------------------------------------------
CMainArgs::~CMainArgs()
{
	optarg = m_szOptarg;
	optind = m_iOptind;
	optopt = m_iOptopt;
	opterr = m_iOpterr;
#ifdef GPOS_Darwin
	optreset = m_iOptreset;
#endif // GPOS_Darwin
}


//---------------------------------------------------------------------------
//	@function:
//		CMainArgs::FGetopt
//
//	@doc:
//		wraps getopt logic
//
//---------------------------------------------------------------------------
BOOL
CMainArgs::FGetopt
	(
	CHAR *pch
	)
{
	GPOS_ASSERT(NULL != pch);
	
	INT iRes = clib::IGetOpt(m_ulArgs, const_cast<CHAR**>(m_rgszArgs), m_szFmt);

	if (iRes != -1)
	{
		*pch = (CHAR)iRes;
		return true;
	}
	
	return false;
}

// EOF

