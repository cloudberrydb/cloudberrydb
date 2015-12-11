//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLColRef.cpp
//
//	@doc:
//		Implementation of DXL column references
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLColRef.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLColRef::CDXLColRef
//
//	@doc:
//		Constructs a column reference
//
//---------------------------------------------------------------------------
CDXLColRef::CDXLColRef
	(
	IMemoryPool *pmp,
	CMDName *pmdname,
	ULONG ulId
	)
	:
	m_pmp(pmp),
	m_pmdname(pmdname),
	m_ulId(ulId)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColRef::~CDXLColRef
//
//	@doc:
//		Desctructor
//
//---------------------------------------------------------------------------
CDXLColRef::~CDXLColRef()
{
	GPOS_DELETE(m_pmdname);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColRef::Pmdname
//
//	@doc:
//		Returns column's name
//
//---------------------------------------------------------------------------
const CMDName *
CDXLColRef::Pmdname() const
{
	return m_pmdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColRef::UlID
//
//	@doc:
//		Returns column's id
//
//---------------------------------------------------------------------------
ULONG
CDXLColRef::UlID() const
{
	return m_ulId;
}


// EOF
