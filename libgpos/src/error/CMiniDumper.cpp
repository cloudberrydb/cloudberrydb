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
	IMemoryPool *pmp,
	ULONG_PTR ulpCapacity
	)
	:
	m_pmp(pmp),
	m_wszBuffer(NULL),
	m_ulpUsed(0),
	m_ulpCapacity(ulpCapacity),
	m_fInit(false),
	m_fFinal(false),
	m_fConverted(false)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(0 < ulpCapacity);
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

		GPOS_DELETE_ARRAY(m_wszBuffer);
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
CMiniDumper::Init()
{
	GPOS_ASSERT(!m_fInit);
	GPOS_ASSERT(!m_fFinal);
	GPOS_ASSERT(NULL == m_wszBuffer);

	m_wszBuffer = GPOS_NEW_ARRAY(m_pmp, WCHAR, m_ulpCapacity);

	CTask *ptsk = CTask::PtskSelf();

	GPOS_ASSERT(NULL != ptsk);

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
	GPOS_ASSERT(m_ulpUsed < m_ulpCapacity);

	SerializeFooter();

	// finalize string to print
	m_wszBuffer[m_ulpUsed] = WCHAR_EOS;

	m_fFinal = true;
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumper::WszReserve
//
//	@doc:
//		Reserve space for minidump entry
//
//---------------------------------------------------------------------------
WCHAR *
CMiniDumper::WszReserve
	(
	ULONG_PTR ulpSize
	)
{
	GPOS_ASSERT(m_fInit);

	// after minidump is finalized, no more entries can be added
	if (m_fFinal)
	{
		return NULL;
	}

	while (m_ulpCapacity > m_ulpUsed + ulpSize)
	{
		ULONG_PTR ulpOld = m_ulpUsed;
		ULONG_PTR ulpNew = ulpOld + ulpSize;

		if (FCompareSwap(&m_ulpUsed, ulpOld, ulpNew))
		{
			return m_wszBuffer + ulpOld;
		}
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumper::OsPrint
//
//	@doc:
//		Write to output stream
//
//---------------------------------------------------------------------------
IOstream&
CMiniDumper::OsPrint
	(
	IOstream &os
	)
	const
{
	GPOS_ASSERT(m_fInit);
	GPOS_ASSERT(m_fFinal);
	GPOS_ASSERT(!m_fConverted);

	return os << m_wszBuffer;
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumper::ConvertToMultiByte
//
//	@doc:
//		Convert to multi-byte format
//
//---------------------------------------------------------------------------
ULONG_PTR
CMiniDumper::UlpConvertToMultiByte()
{
	GPOS_ASSERT(m_fInit);
	GPOS_ASSERT(m_fFinal);
	GPOS_ASSERT(!m_fConverted);

	LINT li = clib::LWcsToMbs
		(
		reinterpret_cast<CHAR*>(m_wszBuffer),
		m_wszBuffer,
		m_ulpCapacity * sizeof(WCHAR)
		);
	GPOS_RTL_ASSERT(0 < li);

	// ensure string is terminated
	m_wszBuffer[m_ulpCapacity - 1] = WCHAR_EOS;
	m_fConverted = true;

	return ULONG_PTR(li);
}


// EOF

