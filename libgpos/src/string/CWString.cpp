//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CWString.cpp
//
//	@doc:
//		Implementation of the wide character string class.
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/string/CWString.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CWString::CWString
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CWString::CWString
	(
	ULONG ulLength
	)
	:
	CWStringBase
		(
		ulLength,
		true // fOwnsMemory
		),
	m_wszBuf(NULL)
{}


//---------------------------------------------------------------------------
//	@function:
//		CWString::Wsz
//
//	@doc:
//		Returns the wide character buffer storing the string
//
//---------------------------------------------------------------------------
const WCHAR*
CWString::Wsz() const
{
	return m_wszBuf;
}


//---------------------------------------------------------------------------
//	@function:
//		CWString::Append
//
//	@doc:
//		Appends a string to the current string
//
//---------------------------------------------------------------------------
void
CWString::Append
	(
	const CWStringBase *pstr
	)
{
	GPOS_ASSERT(NULL != pstr);
	if (0 < pstr->UlLength())
	{
		AppendBuffer(pstr->Wsz());
	}
	GPOS_ASSERT(FValid());
}

// EOF

