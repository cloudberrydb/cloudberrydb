//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2010 Greenplum Inc.
//
//	@filename:
//		COstreamString.cpp
//
//	@doc:
//		Implementation of basic wide character output stream
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringConst.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		COstreamString::COstreamString
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
COstreamString::COstreamString
    (
	CWString *pws
    )
	: 
    COstream(),
    m_pws(pws)
{
	GPOS_ASSERT(m_pws && "Backing string cannot be NULL");
}

//---------------------------------------------------------------------------
//	@function:
//		COstreamString::operator<<
//
//	@doc:
//		WCHAR array write thru;
//
//---------------------------------------------------------------------------
IOstream&
COstreamString::operator << 
    (
	const WCHAR *wsz
    )
{
	m_pws->AppendWideCharArray(wsz);

	return *this;
}

//---------------------------------------------------------------------------
//	@function:
//		COstreamString::operator<<
//
//	@doc:
//		CHAR array write thru;
//
//---------------------------------------------------------------------------
IOstream&
COstreamString::operator <<
    (
	const CHAR *sz
    )
{
	m_pws->AppendCharArray(sz);

	return *this;
}


//---------------------------------------------------------------------------
//	@function:
//		COstreamString::operator<<
//
//	@doc:
//		WCHAR write thru;
//
//---------------------------------------------------------------------------
IOstream&
COstreamString::operator <<
    (
	const WCHAR wc
    )
{
	WCHAR wsz[2];
	wsz[0] = wc;
	wsz[1] = L'\0';
	m_pws->AppendWideCharArray(wsz);

	return *this;
}


//---------------------------------------------------------------------------
//	@function:
//		COstreamString::operator<<
//
//	@doc:
//		CHAR write thru;
//
//---------------------------------------------------------------------------
IOstream&
COstreamString::operator <<
    (
	const CHAR c
    )
{
	CHAR sz[2];
	sz[0] = c;
	sz[1] = '\0';
	m_pws->AppendCharArray(sz);

	return *this;
}


// EOF

