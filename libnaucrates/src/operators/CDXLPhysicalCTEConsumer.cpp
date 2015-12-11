//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalCTEConsumer.cpp
//
//	@doc:
//		Implementation of DXL physical CTE Consumer operator
//		
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLPhysicalCTEConsumer.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalCTEConsumer::CDXLPhysicalCTEConsumer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLPhysicalCTEConsumer::CDXLPhysicalCTEConsumer
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
//		CDXLPhysicalCTEConsumer::~CDXLPhysicalCTEConsumer
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLPhysicalCTEConsumer::~CDXLPhysicalCTEConsumer()
{
	m_pdrgpulColIds->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalCTEConsumer::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalCTEConsumer::Edxlop() const
{
	return EdxlopPhysicalCTEConsumer;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalCTEConsumer::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalCTEConsumer::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalCTEConsumer);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalCTEConsumer::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalCTEConsumer::SerializeToDXL
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
//		CDXLPhysicalCTEConsumer::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalCTEConsumer::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) const
{
	GPOS_ASSERT(1 == pdxln->UlArity());

	CDXLNode *pdxlnPrL = (*pdxln)[0];
	GPOS_ASSERT(EdxlopScalarProjectList == pdxlnPrL->Pdxlop()->Edxlop());

	if (fValidateChildren)
	{
		pdxlnPrL->Pdxlop()->AssertValid(pdxlnPrL, fValidateChildren);
	}

}
#endif // GPOS_DEBUG

// EOF
