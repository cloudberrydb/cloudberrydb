//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalWindow.cpp
//
//	@doc:
//		Implementation of DXL logical window operator
//		
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLLogicalWindow.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"


using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalWindow::CDXLLogicalWindow
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLLogicalWindow::CDXLLogicalWindow
	(
	IMemoryPool *pmp,
	DrgPdxlws *pdrgpdxlws
	)
	:
	CDXLLogical(pmp),
	m_pdrgpdxlws(pdrgpdxlws)
{
	GPOS_ASSERT(NULL != m_pdrgpdxlws);
	GPOS_ASSERT(0 < m_pdrgpdxlws->UlLength());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalWindow::~CDXLLogicalWindow
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLLogicalWindow::~CDXLLogicalWindow()
{
	m_pdrgpdxlws->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalWindow::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLLogicalWindow::Edxlop() const
{
	return EdxlopLogicalWindow;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalWindow::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLLogicalWindow::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenLogicalWindow);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalWindow::Pdxlws
//
//	@doc:
//		Return the window specification at a given position
//
//---------------------------------------------------------------------------
CDXLWindowSpec *
CDXLLogicalWindow::Pdxlws
	(
	ULONG ulPos
	)
	const
{
	GPOS_ASSERT(ulPos <= m_pdrgpdxlws->UlLength());
	return (*m_pdrgpdxlws)[ulPos];
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalWindow::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLLogicalWindow::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	// serialize the list of window specifications
	const CWStringConst *pstrWindowSpecList = CDXLTokens::PstrToken(EdxltokenWindowSpecList);
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrWindowSpecList);
	const ULONG ulSize = m_pdrgpdxlws->UlLength();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CDXLWindowSpec *pdxlwinspec = (*m_pdrgpdxlws)[ul];
		pdxlwinspec->SerializeToDXL(pxmlser);
	}
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrWindowSpecList);

	// serialize children
	pdxln->SerializeChildrenToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalWindow::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLLogicalWindow::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) const
{
	GPOS_ASSERT(2 == pdxln->UlArity());

	CDXLNode *pdxlnProjList = (*pdxln)[0];
	CDXLNode *pdxlnChild = (*pdxln)[1];

	GPOS_ASSERT(EdxlopScalarProjectList == pdxlnProjList->Pdxlop()->Edxlop());
	GPOS_ASSERT(EdxloptypeLogical == pdxlnChild->Pdxlop()->Edxloperatortype());

	if (fValidateChildren)
	{
		pdxlnProjList->Pdxlop()->AssertValid(pdxlnProjList, fValidateChildren);
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}

	const ULONG ulArity = pdxlnProjList->UlArity();
	for (ULONG ul = 0; ul < ulArity; ++ul)
	{
		CDXLNode *pdxlnPrEl = (*pdxlnProjList)[ul];
		GPOS_ASSERT(EdxlopScalarIdent != pdxlnPrEl->Pdxlop()->Edxlop());
	}

	GPOS_ASSERT(NULL != m_pdrgpdxlws);
	GPOS_ASSERT(0 < m_pdrgpdxlws->UlLength());
}

#endif // GPOS_DEBUG

// EOF
