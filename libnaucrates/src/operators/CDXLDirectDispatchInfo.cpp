//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLDirectDispatchInfo.cpp
//
//	@doc:
//		Implementation of DXL datum of types having LINT mapping
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLDirectDispatchInfo.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLDirectDispatchInfo::CDXLDirectDispatchInfo
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLDirectDispatchInfo::CDXLDirectDispatchInfo
	(
	DrgPdrgPdxldatum *pdrgpdrgdxldatum
	)
	:
	m_pdrgpdrgpdxldatum(pdrgpdrgdxldatum)
{
	GPOS_ASSERT(NULL != pdrgpdrgdxldatum);
	
#ifdef GPOS_DEBUG
	const ULONG ulLength = pdrgpdrgdxldatum->UlLength();
	if (0 < ulLength)
	{
		ULONG ulDatums = ((*pdrgpdrgdxldatum)[0])->UlLength();
		for (ULONG ul = 1; ul < ulLength; ul++)
		{
			GPOS_ASSERT(ulDatums == ((*pdrgpdrgdxldatum)[ul])->UlLength());
		}
	}
#endif // GPOS_DEBUG
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLDirectDispatchInfo::~CDXLDirectDispatchInfo
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLDirectDispatchInfo::~CDXLDirectDispatchInfo()
{
	m_pdrgpdrgpdxldatum->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLDirectDispatchInfo::Serialize
//
//	@doc:
//		Serialize direct dispatch info in DXL format
//
//---------------------------------------------------------------------------
void
CDXLDirectDispatchInfo::Serialize
	(
	CXMLSerializer *pxmlser
	)
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenDirectDispatchInfo));
	
	const ULONG ulValueCombinations = (m_pdrgpdrgpdxldatum == NULL) ? 0 : m_pdrgpdrgpdxldatum->UlLength();
	
	for (ULONG ulA = 0; ulA < ulValueCombinations; ulA++)
	{
		pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenDirectDispatchKeyValue));

		DrgPdxldatum *pdrgpdxldatum = (*m_pdrgpdrgpdxldatum)[ulA];
		
		const ULONG ulDatums = pdrgpdxldatum->UlLength();
		for (ULONG ulB = 0; ulB < ulDatums; ulB++) 
		{
			CDXLDatum *pdxldatum = (*pdrgpdxldatum)[ulB];
			pdxldatum->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenDatum));
		}
		
		pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenDirectDispatchKeyValue));
	}
	
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenDirectDispatchInfo));
}

// EOF
