//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLWindowSpec.cpp
//
//	@doc:
//		Implementation of DXL window specification in the DXL
//		representation of the logical query tree
//
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLWindowSpec.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowSpec::CDXLWindowSpec
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLWindowSpec::CDXLWindowSpec
	(
	IMemoryPool *pmp,
	DrgPul *pdrgpulPartCol,
	CMDName *pmdname,
	CDXLNode *pdxlnSortColList,
	CDXLWindowFrame *pdxlwf
	)
	:
	m_pmp(pmp),
	m_pdrgpulPartCol(pdrgpulPartCol),
	m_pmdname(pmdname),
	m_pdxlnSortColList(pdxlnSortColList),
	m_pdxlwf(pdxlwf)
{
	GPOS_ASSERT(NULL != m_pmp);
	GPOS_ASSERT(NULL != m_pdrgpulPartCol);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowSpec::~CDXLWindowSpec
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLWindowSpec::~CDXLWindowSpec()
{
	m_pdrgpulPartCol->Release();
	CRefCount::SafeRelease(m_pdxlwf);
	CRefCount::SafeRelease(m_pdxlnSortColList);
	GPOS_DELETE(m_pmdname);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowSpec::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLWindowSpec::SerializeToDXL
	(
	CXMLSerializer *pxmlser
	)
	const
{
	const CWStringConst *pstrElemName = CDXLTokens::PstrToken(EdxltokenWindowSpec);
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	GPOS_ASSERT(NULL != m_pdrgpulPartCol);

	// serialize partition keys
	CWStringDynamic *pstrPartCols = CDXLUtils::PstrSerialize(m_pmp, m_pdrgpulPartCol);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenPartKeys), pstrPartCols);
	GPOS_DELETE(pstrPartCols);

	if (NULL != m_pmdname)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenAlias), m_pmdname->Pstr());
	}

	// serialize sorting columns
	if (NULL != m_pdxlnSortColList)
	{
		m_pdxlnSortColList->SerializeToDXL(pxmlser);
	}

	// serialize window frames
	if (NULL != m_pdxlwf)
	{
		m_pdxlwf->SerializeToDXL(pxmlser);
	}

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

// EOF
