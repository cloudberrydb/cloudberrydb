//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalWindow.cpp
//
//	@doc:
//		Implementation of DXL physical window operators
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLPhysicalWindow.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalWindow::CDXLPhysicalWindow
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLPhysicalWindow::CDXLPhysicalWindow
	(
	IMemoryPool *pmp,
	DrgPul *pdrgpulPartCols,
	DrgPdxlwk *pdrgpdxlwk
	)
	:
	CDXLPhysical(pmp),
	m_pdrgpulPartCols(pdrgpulPartCols),
	m_pdrgpdxlwk(pdrgpdxlwk)
{
	GPOS_ASSERT(NULL != m_pdrgpulPartCols);
	GPOS_ASSERT(NULL != m_pdrgpdxlwk);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalWindow::~CDXLPhysicalWindow
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLPhysicalWindow::~CDXLPhysicalWindow()
{
	m_pdrgpulPartCols->Release();
	m_pdrgpdxlwk->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalWindow::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalWindow::Edxlop() const
{
	return EdxlopPhysicalWindow;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalWindow::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalWindow::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalWindow);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalWindow::UlPartCols
//
//	@doc:
//		Returns the number of partition columns
//
//---------------------------------------------------------------------------
ULONG
CDXLPhysicalWindow::UlPartCols() const
{
	return m_pdrgpulPartCols->UlLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalWindow::UlWindowKeys
//
//	@doc:
//		Returns the number of window keys
//
//---------------------------------------------------------------------------
ULONG
CDXLPhysicalWindow::UlWindowKeys() const
{
	return m_pdrgpdxlwk->UlLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalWindow::PdxlWindowKey
//
//	@doc:
//		Return the window key at a given position
//
//---------------------------------------------------------------------------
CDXLWindowKey *
CDXLPhysicalWindow::PdxlWindowKey
	(
	ULONG ulPos
	)
	const
{
	GPOS_ASSERT(ulPos <= m_pdrgpdxlwk->UlLength());
	return (*m_pdrgpdxlwk)[ulPos];
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalWindow::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalWindow::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	// serialize partition keys
	CWStringDynamic *pstrPartCols = CDXLUtils::PstrSerialize(m_pmp, m_pdrgpulPartCols);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenPartKeys), pstrPartCols);
	GPOS_DELETE(pstrPartCols);

	// serialize properties
	pdxln->SerializePropertiesToDXL(pxmlser);

	// serialize children
	pdxln->SerializeChildrenToDXL(pxmlser);

	// serialize the list of window keys
	const CWStringConst *pstrWindowKeyList = CDXLTokens::PstrToken(EdxltokenWindowKeyList);
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrWindowKeyList);
	const ULONG ulSize = m_pdrgpdxlwk->UlLength();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CDXLWindowKey *pdxlwk = (*m_pdrgpdxlwk)[ul];
		pdxlwk->SerializeToDXL(pxmlser);
	}
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrWindowKeyList);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalWindow::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalWindow::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	)
	const
{
	// assert proj list and filter are valid
	CDXLPhysical::AssertValid(pdxln, fValidateChildren);
	GPOS_ASSERT(NULL != m_pdrgpulPartCols);
	GPOS_ASSERT(NULL != m_pdrgpdxlwk);
	GPOS_ASSERT(EdxlwindowIndexSentinel == pdxln->UlArity());
	CDXLNode *pdxlnChild = (*pdxln)[EdxlwindowIndexChild];
	if (fValidateChildren)
	{
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
