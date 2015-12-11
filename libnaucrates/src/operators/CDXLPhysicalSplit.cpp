//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalSplit.cpp
//
//	@doc:
//		Implementation of DXL physical Split operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalSplit.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLTableDescr.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSplit::CDXLPhysicalSplit
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLPhysicalSplit::CDXLPhysicalSplit
	(
	IMemoryPool *pmp,
	DrgPul *pdrgpulDelete,
	DrgPul *pdrgpulInsert,
	ULONG ulAction,
	ULONG ulCtid,
	ULONG ulSegmentId,
	BOOL fPreserveOids,
	ULONG ulTupleOid
	)
	:
	CDXLPhysical(pmp),
	m_pdrgpulDelete(pdrgpulDelete),
	m_pdrgpulInsert(pdrgpulInsert),
	m_ulAction(ulAction),
	m_ulCtid(ulCtid),
	m_ulSegmentId(ulSegmentId),
	m_fPreserveOids(fPreserveOids),
	m_ulTupleOid(ulTupleOid)
{
	GPOS_ASSERT(NULL != pdrgpulDelete);
	GPOS_ASSERT(NULL != pdrgpulInsert);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSplit::~CDXLPhysicalSplit
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLPhysicalSplit::~CDXLPhysicalSplit()
{
	m_pdrgpulDelete->Release();
	m_pdrgpulInsert->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSplit::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalSplit::Edxlop() const
{
	return EdxlopPhysicalSplit;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSplit::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalSplit::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalSplit);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSplit::SerializeToDXL
//
//	@doc:
//		Serialize function descriptor in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalSplit::SerializeToDXL
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

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenActionColId), m_ulAction);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenCtidColId), m_ulCtid);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenGpSegmentIdColId), m_ulSegmentId);

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenUpdatePreservesOids), m_fPreserveOids);

	if (m_fPreserveOids)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenTupleOidColId), m_ulTupleOid);
	}
	
	pdxln->SerializePropertiesToDXL(pxmlser);

	// serialize project list
	(*pdxln)[0]->SerializeToDXL(pxmlser);

	// serialize physical child
	(*pdxln)[1]->SerializeToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSplit::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalSplit::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	)
	const
{
	GPOS_ASSERT(2 == pdxln->UlArity());
	CDXLNode *pdxlnChild = (*pdxln)[1];
	GPOS_ASSERT(EdxloptypePhysical == pdxlnChild->Pdxlop()->Edxloperatortype());

	if (fValidateChildren)
	{
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}
}

#endif // GPOS_DEBUG


// EOF
