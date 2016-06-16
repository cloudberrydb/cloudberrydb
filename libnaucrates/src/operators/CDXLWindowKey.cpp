//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLWindowKey.cpp
//
//	@doc:
//		Implementation of DXL window key
//		
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLWindowKey.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowKey::CDXLWindowKey
//
//	@doc:
//		Constructs a scalar window key node
//
//---------------------------------------------------------------------------
CDXLWindowKey::CDXLWindowKey
	(
	IMemoryPool *pmp
	)
	:
	m_pmp(pmp),
	m_pdxlwf(NULL),
	m_pdxlnSortColList(NULL)
{
	GPOS_ASSERT(NULL != m_pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowKey::~CDXLWindowKey
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLWindowKey::~CDXLWindowKey()
{
	CRefCount::SafeRelease(m_pdxlwf);
	CRefCount::SafeRelease(m_pdxlnSortColList);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowKey::SetWindowFrame
//
//	@doc:
//		Set window frame
//
//---------------------------------------------------------------------------
void
CDXLWindowKey::SetWindowFrame
	(
	CDXLWindowFrame *pdxlwf
	)
{
	// allow setting window frame only once
	GPOS_ASSERT (NULL == m_pdxlwf);
	m_pdxlwf = pdxlwf;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowKey::SetSortColList
//
//	@doc:
//		Set sort column list
//
//---------------------------------------------------------------------------
void
CDXLWindowKey::SetSortColList
	(
	CDXLNode *pdxlnSortColList
	)
{
	// allow setting window frame only once
	GPOS_ASSERT(NULL == m_pdxlnSortColList);
	m_pdxlnSortColList = pdxlnSortColList;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowKey::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLWindowKey::SerializeToDXL
	(
	CXMLSerializer *pxmlser
	)
	const
{
	const CWStringConst *pstrElemName = CDXLTokens::PstrToken(EdxltokenWindowKey);
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	if (NULL != m_pdxlnSortColList)
	{
		m_pdxlnSortColList->SerializeToDXL(pxmlser);
	}

	if (NULL != m_pdxlwf)
	{
		m_pdxlwf->SerializeToDXL(pxmlser);
	}

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

// EOF
