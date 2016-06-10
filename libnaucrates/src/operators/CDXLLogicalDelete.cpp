//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalDelete.cpp
//
//	@doc:
//		Implementation of DXL logical delete operator
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLLogicalDelete.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLTableDescr.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalDelete::CDXLLogicalDelete
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLLogicalDelete::CDXLLogicalDelete
	(
	IMemoryPool *pmp,
	CDXLTableDescr *pdxltabdesc,
	ULONG ulCtid,
	ULONG ulSegmentId,
	DrgPul *pdrgpulDelete
	)
	:
	CDXLLogical(pmp),
	m_pdxltabdesc(pdxltabdesc),
	m_ulCtid(ulCtid),
	m_ulSegmentId(ulSegmentId),
	m_pdrgpulDelete(pdrgpulDelete)
{
	GPOS_ASSERT(NULL != pdxltabdesc);
	GPOS_ASSERT(NULL != pdrgpulDelete);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalDelete::~CDXLLogicalDelete
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLLogicalDelete::~CDXLLogicalDelete()
{
	m_pdxltabdesc->Release();
	m_pdrgpulDelete->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalDelete::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLLogicalDelete::Edxlop() const
{
	return EdxlopLogicalDelete;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalDelete::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLLogicalDelete::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenLogicalDelete);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalDelete::SerializeToDXL
//
//	@doc:
//		Serialize function descriptor in DXL format
//
//---------------------------------------------------------------------------
void
CDXLLogicalDelete::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	CWStringDynamic *pstrColsDel = CDXLUtils::PstrSerialize(m_pmp, m_pdrgpulDelete);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenDeleteCols), pstrColsDel);
	GPOS_DELETE(pstrColsDel);

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenCtidColId), m_ulCtid);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenGpSegmentIdColId), m_ulSegmentId);

	m_pdxltabdesc->SerializeToDXL(pxmlser);
	pdxln->SerializeChildrenToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalDelete::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLLogicalDelete::AssertValid
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
