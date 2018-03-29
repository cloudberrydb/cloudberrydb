//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLColDescr.cpp
//
//	@doc:
//		Implementation of DXL column descriptors
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLColDescr.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/md/CMDIdGPDB.h"

using namespace gpos;
using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::CDXLColDescr
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLColDescr::CDXLColDescr
	(
	IMemoryPool *pmp,
	CMDName *pmdname,
	ULONG ulId,
	INT iAttno,
	IMDId *pmdidType,
	INT iTypeModifier,
	BOOL fDropped,
	ULONG ulWidth
	)
	:
	m_pmp(pmp),
	m_pmdname(pmdname),
	m_ulId(ulId),
	m_iAttno(iAttno),
	m_pmdidType(pmdidType),
	m_iTypeModifier(iTypeModifier),
	m_fDropped(fDropped),
	m_ulWidth(ulWidth)
{
	GPOS_ASSERT_IMP(m_fDropped, 0 == m_pmdname->Pstr()->UlLength());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::~CDXLColDescr
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLColDescr::~CDXLColDescr()
{
	m_pmdidType->Release();
	GPOS_DELETE(m_pmdname);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::Pmdname
//
//	@doc:
//		Returns the column name
//
//---------------------------------------------------------------------------
const CMDName *
CDXLColDescr::Pmdname() const
{
	return m_pmdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::UlID
//
//	@doc:
//		Returns the column Id
//
//---------------------------------------------------------------------------
ULONG
CDXLColDescr::UlID() const
{
	return m_ulId;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::IAttno
//
//	@doc:
//		Returns the column attribute number in GPDB
//
//---------------------------------------------------------------------------
INT
CDXLColDescr::IAttno() const
{
	return m_iAttno;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::PmdidType
//
//	@doc:
//		Returns the type id for this column
//
//---------------------------------------------------------------------------
IMDId *
CDXLColDescr::PmdidType() const
{
	return m_pmdidType;
}

INT
CDXLColDescr::ITypeModifier() const
{
	return m_iTypeModifier;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::FDropped
//
//	@doc:
//		Is the column dropped from the relation
//
//---------------------------------------------------------------------------
BOOL
CDXLColDescr::FDropped() const
{
	return m_fDropped;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::UlWidth
//
//	@doc:
//		Returns the width of the column
//
//---------------------------------------------------------------------------
ULONG
CDXLColDescr::UlWidth() const
{
	return m_ulWidth;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::SerializeToDXL
//
//	@doc:
//		Serializes the column descriptor into DXL format
//
//---------------------------------------------------------------------------
void
CDXLColDescr::SerializeToDXL
	(
	CXMLSerializer *pxmlser
	)
	const
{
	const CWStringConst *pstrTokenColDescr = CDXLTokens::PstrToken(EdxltokenColDescr);
	
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrTokenColDescr);
	
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenColId), m_ulId);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenAttno), m_iAttno);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenColName), m_pmdname->Pstr());
	m_pmdidType->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenTypeId));

	if (IDefaultTypeModifier != ITypeModifier())
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenTypeMod), ITypeModifier());
	}

	if (m_fDropped)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenColDropped), m_fDropped);
	}

	if (gpos::ulong_max != m_ulWidth)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenColWidth), m_ulWidth);
	}
	
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrTokenColDescr);

	GPOS_CHECK_ABORT;
}

// EOF
