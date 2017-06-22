//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLCtasStorageOptions.cpp
//
//	@doc:
//		Implementation of DXL CTAS storage options
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLCtasStorageOptions.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLCtasStorageOptions::CDXLCtasStorageOptions
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLCtasStorageOptions::CDXLCtasStorageOptions
	( 
	CMDName *pmdnameTablespace,
	ECtasOnCommitAction ectascommit,
	DrgPctasOpt *pdrgpctasopt
	)
	:
	m_pmdnameTablespace(pmdnameTablespace),
	m_ectascommit(ectascommit),
	m_pdrgpctasopt(pdrgpctasopt)
{
	GPOS_ASSERT(EctascommitSentinel > ectascommit);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLCtasStorageOptions::~CDXLCtasStorageOptions
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLCtasStorageOptions::~CDXLCtasStorageOptions()
{
	GPOS_DELETE(m_pmdnameTablespace);
	CRefCount::SafeRelease(m_pdrgpctasopt);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLCtasStorageOptions::PmdnameTablespace
//
//	@doc:
//		Returns the tablespace name
//
//---------------------------------------------------------------------------
CMDName *
CDXLCtasStorageOptions::PmdnameTablespace() const
{
	return m_pmdnameTablespace;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLCtasStorageOptions::Ectascommit
//
//	@doc:
//		Returns the OnCommit ctas spec
//
//---------------------------------------------------------------------------
CDXLCtasStorageOptions::ECtasOnCommitAction
CDXLCtasStorageOptions::Ectascommit() const
{
	return m_ectascommit;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLCtasStorageOptions::DrgPctasOpt
//
//	@doc:
//		Returns array of storage options
//
//---------------------------------------------------------------------------
CDXLCtasStorageOptions::DrgPctasOpt *
CDXLCtasStorageOptions::Pdrgpctasopt() const
{
	return m_pdrgpctasopt;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLCtasStorageOptions::Serialize
//
//	@doc:
//		Serialize options in DXL format
//
//---------------------------------------------------------------------------
void
CDXLCtasStorageOptions::Serialize
	(
	CXMLSerializer *pxmlser
	) 
	const
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCTASOptions));
	if (NULL != m_pmdnameTablespace)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenTablespace), m_pmdnameTablespace->Pstr());
	}
	
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenOnCommitAction), PstrOnCommitAction(m_ectascommit));
	
	const ULONG ulOptions = (m_pdrgpctasopt == NULL) ? 0 : m_pdrgpctasopt->UlLength();
	for (ULONG ul = 0; ul < ulOptions; ul++)
	{
		CDXLCtasOption *pdxlctasopt = (*m_pdrgpctasopt)[ul];
		pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCTASOption));
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenCtasOptionType), pdxlctasopt->m_ulType);
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenName), pdxlctasopt->m_pstrName);
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenValue), pdxlctasopt->m_pstrValue);
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenIsNull), pdxlctasopt->m_fNull);
		pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCTASOption));
	}
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCTASOptions));
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLCtasStorageOptions::PstrOnCommitAction
//
//	@doc:
//		String representation of OnCommit action spec
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLCtasStorageOptions::PstrOnCommitAction
	(
	CDXLCtasStorageOptions::ECtasOnCommitAction ectascommit
	) 
{
	switch (ectascommit)
	{
		case EctascommitNOOP:
			return CDXLTokens::PstrToken(EdxltokenOnCommitNOOP);
			
		case EctascommitPreserve:
			return CDXLTokens::PstrToken(EdxltokenOnCommitPreserve);
			
		case EctascommitDelete:
			return CDXLTokens::PstrToken(EdxltokenOnCommitDelete);
			
		case EctascommitDrop:
			return CDXLTokens::PstrToken(EdxltokenOnCommitDrop);
		
		default:
			GPOS_ASSERT("Invalid on commit option");
			return NULL;
	}
}

// EOF
