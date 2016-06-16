//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalCTEProducer.cpp
//
//	@doc:
//		Implementation of DXL physical CTE producer operator
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLPhysicalCTEProducer.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalCTEProducer::CDXLPhysicalCTEProducer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLPhysicalCTEProducer::CDXLPhysicalCTEProducer
	(
	IMemoryPool *pmp,
	ULONG ulId,
	DrgPul *pdrgpulColIds
	)
	:
	CDXLPhysical(pmp),
	m_ulId(ulId),
	m_pdrgpulColIds(pdrgpulColIds)
{
	GPOS_ASSERT(NULL != pdrgpulColIds);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalCTEProducer::~CDXLPhysicalCTEProducer
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLPhysicalCTEProducer::~CDXLPhysicalCTEProducer()
{
	m_pdrgpulColIds->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalCTEProducer::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalCTEProducer::Edxlop() const
{
	return EdxlopPhysicalCTEProducer;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalCTEProducer::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalCTEProducer::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalCTEProducer);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalCTEProducer::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalCTEProducer::SerializeToDXL
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

	// serialize properties
	pdxln->SerializePropertiesToDXL(pxmlser);

	pdxln->SerializeChildrenToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalCTEProducer::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalCTEProducer::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) const
{
	GPOS_ASSERT(2 == pdxln->UlArity());

	CDXLNode *pdxlnPrL = (*pdxln)[0];
	CDXLNode *pdxlnChild = (*pdxln)[1];

	GPOS_ASSERT(EdxlopScalarProjectList == pdxlnPrL->Pdxlop()->Edxlop());
	GPOS_ASSERT(EdxloptypePhysical == pdxlnChild->Pdxlop()->Edxloperatortype());

	if (fValidateChildren)
	{
		pdxlnPrL->Pdxlop()->AssertValid(pdxlnPrL, fValidateChildren);
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
