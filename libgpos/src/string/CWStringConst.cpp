//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CWStringConst.cpp
//
//	@doc:
//		Implementation of the wide character constant string class
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/string/CWStringConst.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CWStringConst::CWStringConst
//
//	@doc:
//		Initializes a constant string with a given character buffer. The string
//		does not own the memory
//
//---------------------------------------------------------------------------
CWStringConst::CWStringConst
	(
	const WCHAR *wszBuf
	)
	:
	CWStringBase
		(
		GPOS_WSZ_LENGTH(wszBuf),
		false // fOwnsMemory
		),
	m_wszBuf(wszBuf)
{
	GPOS_ASSERT(NULL != wszBuf);
	GPOS_ASSERT(FValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringConst::CWStringConst
//
//	@doc:
//		Initializes a constant string by making a copy of the given character buffer.
//		The string owns the memory.
//
//---------------------------------------------------------------------------
CWStringConst::CWStringConst
	(
	IMemoryPool *pmp,
	const WCHAR *wszBuf
	)
	:
	CWStringBase
		(
		GPOS_WSZ_LENGTH(wszBuf),
		true // fOwnsMemory
		),
	m_wszBuf(NULL)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != wszBuf);

	if (0 == m_ulLength)
	{
		// string is empty
		m_wszBuf = &m_wcEmpty;
	}
	else
	{
		// make a copy of the string
		WCHAR *wszTempBuf = GPOS_NEW_ARRAY(pmp, WCHAR, m_ulLength + 1);
		clib::WszWcsNCpy(wszTempBuf, wszBuf, m_ulLength + 1);
		m_wszBuf = wszTempBuf;
	}

	GPOS_ASSERT(FValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringConst::CWStringConst
//
//	@doc:
//		Shallow copy constructor.
//
//---------------------------------------------------------------------------
CWStringConst::CWStringConst
	(
	const CWStringConst& str
	)
	:
	CWStringBase
		(
		str.UlLength(),
		false // fOwnsMemory
		),
	m_wszBuf(str.Wsz())
{
	GPOS_ASSERT(NULL != m_wszBuf);
	GPOS_ASSERT(FValid());
}
//---------------------------------------------------------------------------
//	@function:
//		CWStringConst::~CWStringConst
//
//	@doc:
//		Destroys a constant string. This involves releasing the character buffer
//		provided the string owns it.
//
//---------------------------------------------------------------------------
CWStringConst::~CWStringConst()
{
	if (m_fOwnsMemory && m_wszBuf != &m_wcEmpty)
	{
		GPOS_DELETE_ARRAY(m_wszBuf);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringConst::Wsz
//
//	@doc:
//		Returns the wide character buffer
//
//---------------------------------------------------------------------------
const WCHAR*
CWStringConst::Wsz() const
{
	return m_wszBuf;
}

// EOF

