//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLColRef.cpp
//
//	@doc:
//		Implementation of DXL column references
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
	ULONG ulId,
	IMDId *pmdidType,
	INT iTypeModifier,
	OID oidCollation
	)
	:
	m_pmp(pmp),
	m_pmdname(pmdname),
	m_ulId(ulId),
	m_pmdidType(pmdidType),
	m_iTypeModifer(iTypeModifier),
	m_oidCollation(oidCollation)
{
	GPOS_ASSERT(m_pmdidType->FValid());
}

// ctor for invalid collation
CDXLColRef::CDXLColRef
		(
		IMemoryPool *pmp,
		CMDName *pmdname,
		ULONG ulId,
		IMDId *pmdidType,
		INT iTypeModifier
		)
		:
		m_pmp(pmp),
		m_pmdname(pmdname),
		m_ulId(ulId),
		m_pmdidType(pmdidType),
		m_iTypeModifer(iTypeModifier)
{
	GPOS_ASSERT(m_pmdidType->FValid());
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
	m_pmdidType->Release();
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
//		CDXLColRef::PmdidType
//
//	@doc:
//		Returns column's type md id
//
//---------------------------------------------------------------------------
IMDId *
CDXLColRef::PmdidType() const
{
	return m_pmdidType;
}

INT
CDXLColRef::ITypeModifier() const
{
	return m_iTypeModifer;
}

OID
CDXLColRef::OidCollation() const
{
	return m_oidCollation;
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
