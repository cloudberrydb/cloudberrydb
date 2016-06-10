//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalUpdate.cpp
//
//	@doc:
//		Implementation of DXL logical update operator
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLLogicalUpdate.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLTableDescr.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalUpdate::CDXLLogicalUpdate
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLLogicalUpdate::CDXLLogicalUpdate
	(
	IMemoryPool *pmp,
	CDXLTableDescr *pdxltabdesc,
	ULONG ulCtid,
	ULONG ulSegmentId,
	DrgPul *pdrgpulDelete,
	DrgPul *pdrgpulInsert,
	BOOL fPreserveOids,
	ULONG ulTupleOid
	)
	:
	CDXLLogical(pmp),
	m_pdxltabdesc(pdxltabdesc),
	m_ulCtid(ulCtid),
	m_ulSegmentId(ulSegmentId),
	m_pdrgpulDelete(pdrgpulDelete),
	m_pdrgpulInsert(pdrgpulInsert),
	m_fPreserveOids(fPreserveOids),
	m_ulTupleOid(ulTupleOid)
{
	GPOS_ASSERT(NULL != pdxltabdesc);
	GPOS_ASSERT(NULL != pdrgpulDelete);
	GPOS_ASSERT(NULL != pdrgpulInsert);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalUpdate::~CDXLLogicalUpdate
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLLogicalUpdate::~CDXLLogicalUpdate()
{
	m_pdxltabdesc->Release();
	m_pdrgpulDelete->Release();
	m_pdrgpulInsert->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalUpdate::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLLogicalUpdate::Edxlop() const
{
	return EdxlopLogicalUpdate;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalUpdate::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLLogicalUpdate::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenLogicalUpdate);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalUpdate::SerializeToDXL
//
//	@doc:
//		Serialize function descriptor in DXL format
//
//---------------------------------------------------------------------------
void
CDXLLogicalUpdate::SerializeToDXL
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

	CWStringDynamic *pstrColsIns = CDXLUtils::PstrSerialize(m_pmp, m_pdrgpulInsert);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenInsertCols), pstrColsIns);
	GPOS_DELETE(pstrColsIns);

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenCtidColId), m_ulCtid);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenGpSegmentIdColId), m_ulSegmentId);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenUpdatePreservesOids), m_fPreserveOids);
	
	if (m_fPreserveOids)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenTupleOidColId), m_ulTupleOid);
	}
	
	m_pdxltabdesc->SerializeToDXL(pxmlser);
	pdxln->SerializeChildrenToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalUpdate::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLLogicalUpdate::AssertValid
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
