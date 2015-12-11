//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalCTEProducer.cpp
//
//	@doc:
//		Implementation of DXL logical CTE producer operator
//		
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLLogicalCTEProducer.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEProducer::CDXLLogicalCTEProducer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLLogicalCTEProducer::CDXLLogicalCTEProducer
	(
	IMemoryPool *pmp,
	ULONG ulId,
	DrgPul *pdrgpulColIds
	)
	:
	CDXLLogical(pmp),
	m_ulId(ulId),
	m_pdrgpulColIds(pdrgpulColIds)
{
	GPOS_ASSERT(NULL != pdrgpulColIds);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEProducer::~CDXLLogicalCTEProducer
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLLogicalCTEProducer::~CDXLLogicalCTEProducer()
{
	m_pdrgpulColIds->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEProducer::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLLogicalCTEProducer::Edxlop() const
{
	return EdxlopLogicalCTEProducer;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEProducer::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLLogicalCTEProducer::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenLogicalCTEProducer);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEProducer::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLLogicalCTEProducer::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenCTEId), UlId());
	
	CWStringDynamic *pstrColIds = CDXLUtils::PstrSerialize(m_pmp, m_pdrgpulColIds);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenColumns), pstrColIds);
	GPOS_DELETE(pstrColIds);
	
	pdxln->SerializeChildrenToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEProducer::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLLogicalCTEProducer::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) const
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
