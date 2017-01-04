//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2010 Greenplum, Inc.
//
//	@filename:
//		COstreamBasic.cpp
//
//	@doc:
//		Implementation of basic wide character output stream
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/io/COstreamBasic.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		COstreamBasic::COstreamBasic
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
COstreamBasic::COstreamBasic
    (
	WOSTREAM *pos
    )
	: 
    COstream(),
	m_pos(pos)
{
	GPOS_ASSERT(NULL != m_pos && "Output stream cannot be NULL");
}

//---------------------------------------------------------------------------
//	@function:
//		COstreamBasic::operator<<
//
//	@doc:
//		WCHAR write thru;
//
//---------------------------------------------------------------------------
IOstream&
COstreamBasic::operator << 
    (
	const WCHAR *wsz
    )
{
	m_pos = &(*m_pos << wsz);
	return *this;
}

// EOF

