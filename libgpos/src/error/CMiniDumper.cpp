//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMiniDumper.cpp
//
//	@doc:
//		Partial implementation of interface for minidump handler
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/error/CErrorContext.h"
#include "gpos/error/CMiniDumper.h"
#include "gpos/string/CWStringConst.h"
#include "gpos/sync/atomic.h"
#include "gpos/task/CTask.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CMiniDumper::CMiniDumper
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMiniDumper::CMiniDumper
	(
	IMemoryPool *pmp
	)
	:
	m_pmp(pmp),
	m_fInit(false),
	m_fFinal(false),
	m_oos(NULL)
{
	GPOS_ASSERT(NULL != pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumper::~CMiniDumper
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMiniDumper::~CMiniDumper()
{
	if (m_fInit)
	{
		CTask *ptsk = CTask::PtskSelf();

		GPOS_ASSERT(NULL != ptsk);

		ptsk->PerrctxtConvert()->Unregister
			(
#ifdef GPOS_DEBUG
			this
#endif // GPOS_DEBUG
			);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumper::Init
//
//	@doc:
//		Initialize
//
//---------------------------------------------------------------------------
void
CMiniDumper::Init(COstream *oos)
{
	GPOS_ASSERT(!m_fInit);
	GPOS_ASSERT(!m_fFinal);

	CTask *ptsk = CTask::PtskSelf();

	GPOS_ASSERT(NULL != ptsk);

	m_oos = oos;

	ptsk->PerrctxtConvert()->Register(this);

	m_fInit = true;

	SerializeHeader();
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumper::Finalize
//
//	@doc:
//		Finalize
//
//---------------------------------------------------------------------------
void
CMiniDumper::Finalize()
{
	GPOS_ASSERT(m_fInit);
	GPOS_ASSERT(!m_fFinal);

	SerializeFooter();

	m_fFinal = true;
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumper::GetOStream
//
//	@doc:
//		Get stream to serialize to
//
//---------------------------------------------------------------------------
COstream&
CMiniDumper::GetOStream
	(
	)
{
	GPOS_ASSERT(m_fInit);

	return *m_oos;
}


// EOF

