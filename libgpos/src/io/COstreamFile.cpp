//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		COstreamFile.cpp
//
//	@doc:
//		Implementation of output file stream class;
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/io/COstreamFile.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		COstreamFile::COstreamFile
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
COstreamFile::COstreamFile
	(
	const CHAR *szPath,
	ULONG ulPerms
	)
{
	m_fw.Open(szPath, ulPerms);
}


//---------------------------------------------------------------------------
//	@function:
//		COstreamFile::COstreamFile
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
COstreamFile::~COstreamFile()
{
	m_fw.Close();
}


//---------------------------------------------------------------------------
//	@function:
//		COstreamFile::operator<<
//
//	@doc:
//		WCHAR write thru;
//
//---------------------------------------------------------------------------
IOstream&
COstreamFile::operator <<
    (
	const WCHAR *wsz
    )
{
	ULONG_PTR ulpSize = GPOS_WSZ_LENGTH(wsz) * GPOS_SIZEOF(WCHAR);
	m_fw.Write((const BYTE*) wsz, ulpSize);

	return *this;
}


// EOF

