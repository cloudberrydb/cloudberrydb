//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalSetOp.cpp
//
//	@doc:
//		Implementation of DXL logical set operator
//		
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLLogicalSetOp.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"


using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalSetOp::CDXLLogicalSetOp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLLogicalSetOp::CDXLLogicalSetOp
	(
	IMemoryPool *pmp,
	EdxlSetOpType edxlsetoptype,
	DrgPdxlcd *pdrgdxlcd,
	DrgPdrgPul *pdrgpdrgpul,
	BOOL fCastAcrossInputs
	)
	:
	CDXLLogical(pmp),
	m_edxlsetoptype(edxlsetoptype),
	m_pdrgpdxlcd(pdrgdxlcd),
	m_pdrgpdrgpul(pdrgpdrgpul),
	m_fCastAcrossInputs(fCastAcrossInputs)
{
	GPOS_ASSERT(NULL != m_pdrgpdxlcd);
	GPOS_ASSERT(NULL != m_pdrgpdrgpul);
	GPOS_ASSERT(EdxlsetopSentinel > edxlsetoptype);
	
#ifdef GPOS_DEBUG	
	const ULONG ulCols = m_pdrgpdxlcd->UlLength();
	const ULONG ulLen = m_pdrgpdrgpul->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		DrgPul *pdrgpulInput = (*m_pdrgpdrgpul)[ul];
		GPOS_ASSERT(ulCols == pdrgpulInput->UlLength());
	}

#endif	
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalSetOp::~CDXLLogicalSetOp
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLLogicalSetOp::~CDXLLogicalSetOp()
{
	m_pdrgpdxlcd->Release();
	m_pdrgpdrgpul->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalSetOp::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLLogicalSetOp::Edxlop() const
{
	return EdxlopLogicalSetOp;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalSetOp::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLLogicalSetOp::PstrOpName() const
{
	switch (m_edxlsetoptype)
	{
		case EdxlsetopUnion:
				return CDXLTokens::PstrToken(EdxltokenLogicalUnion);

		case EdxlsetopUnionAll:
				return CDXLTokens::PstrToken(EdxltokenLogicalUnionAll);

		case EdxlsetopIntersect:
				return CDXLTokens::PstrToken(EdxltokenLogicalIntersect);

		case EdxlsetopIntersectAll:
				return CDXLTokens::PstrToken(EdxltokenLogicalIntersectAll);

		case EdxlsetopDifference:
				return CDXLTokens::PstrToken(EdxltokenLogicalDifference);

		case EdxlsetopDifferenceAll:
				return CDXLTokens::PstrToken(EdxltokenLogicalDifferenceAll);

		default:
			GPOS_ASSERT(!"Unrecognized set operator type");
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalSetOp::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLLogicalSetOp::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	// serialize the array of input colid arrays
	CWStringDynamic *pstrInputColIds = CDXLUtils::PstrSerialize(m_pmp, m_pdrgpdrgpul);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenInputCols), pstrInputColIds);
	GPOS_DELETE(pstrInputColIds);
	
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenCastAcrossInputs), m_fCastAcrossInputs);

	// serialize output columns
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenColumns));
	GPOS_ASSERT(NULL != m_pdrgpdxlcd);

	const ULONG ulLen = m_pdrgpdxlcd->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CDXLColDescr *pdxlcd = (*m_pdrgpdxlcd)[ul];
		pdxlcd->SerializeToDXL(pxmlser);
	}
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenColumns));

	// serialize children
	pdxln->SerializeChildrenToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalSetOp::FDefinesColumn
//
//	@doc:
//		Check if given column is defined by operator
//
//---------------------------------------------------------------------------
BOOL
CDXLLogicalSetOp::FDefinesColumn
	(
	ULONG ulColId
	)
	const
{
	const ULONG ulSize = UlArity();
	for (ULONG ulDescr = 0; ulDescr < ulSize; ulDescr++)
	{
		ULONG ulId = Pdxlcd(ulDescr)->UlID();
		if (ulId == ulColId)
		{
			return true;
		}
	}

	return false;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalSetOp::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLLogicalSetOp::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) const
{
	GPOS_ASSERT(2 <= pdxln->UlArity());
	GPOS_ASSERT(NULL != m_pdrgpdxlcd);

	// validate output columns
	const ULONG ulOutputCols = m_pdrgpdxlcd->UlLength();
	GPOS_ASSERT(0 < ulOutputCols);

	// validate children
	const ULONG ulChildren = pdxln->UlArity();
	for (ULONG ul = 0; ul < ulChildren; ++ul)
	{
		CDXLNode *pdxlnChild = (*pdxln)[ul];
		GPOS_ASSERT(EdxloptypeLogical == pdxlnChild->Pdxlop()->Edxloperatortype());

		if (fValidateChildren)
		{
			pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
		}
	}
}

#endif // GPOS_DEBUG

// EOF
