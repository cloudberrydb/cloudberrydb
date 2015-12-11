//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalInsert.cpp
//
//	@doc:
//		Implementation of DXL logical insert operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLLogicalInsert.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLTableDescr.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalInsert::CDXLLogicalInsert
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLLogicalInsert::CDXLLogicalInsert
	(
	IMemoryPool *pmp,
	CDXLTableDescr *pdxltabdesc,
	DrgPul *pdrgpul
	)
	:
	CDXLLogical(pmp),
	m_pdxltabdesc(pdxltabdesc),
	m_pdrgpul(pdrgpul)
{
	GPOS_ASSERT(NULL != pdxltabdesc);
	GPOS_ASSERT(NULL != pdrgpul);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalInsert::~CDXLLogicalInsert
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLLogicalInsert::~CDXLLogicalInsert()
{
	m_pdxltabdesc->Release();
	m_pdrgpul->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalInsert::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLLogicalInsert::Edxlop() const
{
	return EdxlopLogicalInsert;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalInsert::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLLogicalInsert::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenLogicalInsert);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalInsert::SerializeToDXL
//
//	@doc:
//		Serialize function descriptor in DXL format
//
//---------------------------------------------------------------------------
void
CDXLLogicalInsert::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	CWStringDynamic *pstrCols = CDXLUtils::PstrSerialize(m_pmp, m_pdrgpul);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenInsertCols), pstrCols);
	GPOS_DELETE(pstrCols);

	// serialize table descriptor
	m_pdxltabdesc->SerializeToDXL(pxmlser);
	
	// serialize arguments
	pdxln->SerializeChildrenToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalInsert::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLLogicalInsert::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	GPOS_ASSERT(1 == pdxln->UlArity());

	CDXLNode *pdxlnChild = (*pdxln)[0];
	GPOS_ASSERT(EdxloptypeLogical == pdxlnChild->Pdxlop()->Edxloperatortype());

	if (fValidateChildren)
	{
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}
}

#endif // GPOS_DEBUG


// EOF
