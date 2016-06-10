//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalCTEConsumer.cpp
//
//	@doc:
//		Implementation of DXL logical CTE Consumer operator
//		
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLLogicalCTEConsumer.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEConsumer::CDXLLogicalCTEConsumer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLLogicalCTEConsumer::CDXLLogicalCTEConsumer
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
//		CDXLLogicalCTEConsumer::~CDXLLogicalCTEConsumer
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLLogicalCTEConsumer::~CDXLLogicalCTEConsumer()
{
	m_pdrgpulColIds->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEConsumer::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLLogicalCTEConsumer::Edxlop() const
{
	return EdxlopLogicalCTEConsumer;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEConsumer::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLLogicalCTEConsumer::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenLogicalCTEConsumer);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEConsumer::FDefinesColumn
//
//	@doc:
//		Check if given column is defined by operator
//
//---------------------------------------------------------------------------
BOOL
CDXLLogicalCTEConsumer::FDefinesColumn
	(
	ULONG ulColId
	)
	const
{
	const ULONG ulSize = m_pdrgpulColIds->UlLength();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		ULONG ulId = *((*m_pdrgpulColIds)[ul]);
		if (ulId == ulColId)
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEConsumer::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLLogicalCTEConsumer::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode * //pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenCTEId), UlId());
	
	CWStringDynamic *pstrColIds = CDXLUtils::PstrSerialize(m_pmp, m_pdrgpulColIds);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenColumns), pstrColIds);
	GPOS_DELETE(pstrColIds);
	
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEConsumer::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLLogicalCTEConsumer::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL // fValidateChildren
	) const
{
	GPOS_ASSERT(0 == pdxln->UlArity());

}
#endif // GPOS_DEBUG

// EOF
