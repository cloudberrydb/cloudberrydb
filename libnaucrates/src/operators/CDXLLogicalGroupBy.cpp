//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLLogicalGroupBy.cpp
//
//	@doc:
//		Implementation of DXL logical group by operator
//		
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLLogicalGroupBy.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGroupBy::CDXLLogicalGroupBy
//
//	@doc:
//		Construct a DXL Logical group by node
//
//---------------------------------------------------------------------------
CDXLLogicalGroupBy::CDXLLogicalGroupBy
	(
	IMemoryPool *pmp
	)
	:
	CDXLLogical(pmp),
	m_pdrgpulGrpColId(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGroupBy::CDXLLogicalGroupBy
//
//	@doc:
//		Construct a DXL Logical group by node
//
//---------------------------------------------------------------------------
CDXLLogicalGroupBy::CDXLLogicalGroupBy
	(
	IMemoryPool *pmp,
	DrgPul *pdrgpulGrpColIds
	)
	:
	CDXLLogical(pmp),
	m_pdrgpulGrpColId(pdrgpulGrpColIds)
{
	GPOS_ASSERT(NULL != pdrgpulGrpColIds);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGroupBy::~CDXLLogicalGroupBy
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLLogicalGroupBy::~CDXLLogicalGroupBy()
{
	CRefCount::SafeRelease(m_pdrgpulGrpColId);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGroupBy::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLLogicalGroupBy::Edxlop() const
{
	return EdxlopLogicalGrpBy;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGroupBy::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLLogicalGroupBy::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenLogicalGrpBy);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGroupBy::SetGroupingColumns
//
//	@doc:
//		Sets array of grouping columns
//
//---------------------------------------------------------------------------
void
CDXLLogicalGroupBy::SetGroupingColumns
	(
	DrgPul *pdrgpul
	)
{
	GPOS_ASSERT(NULL != pdrgpul);
	m_pdrgpulGrpColId = pdrgpul;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGroupBy::PdrgpulGroupingCols
//
//	@doc:
//		Grouping column indices
//
//---------------------------------------------------------------------------
const DrgPul *
CDXLLogicalGroupBy::PdrgpulGroupingCols() const
{
	return m_pdrgpulGrpColId;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGroupBy::SerializeGrpColsToDXL
//
//	@doc:
//		Serialize grouping column indices in DXL format
//
//---------------------------------------------------------------------------
void
CDXLLogicalGroupBy::SerializeGrpColsToDXL
	(
	CXMLSerializer *pxmlser
	)
	const
{
	if(NULL != m_pdrgpulGrpColId)
	{
		const CWStringConst *pstrTokenGroupingCols = CDXLTokens::PstrToken(EdxltokenGroupingCols);
		const CWStringConst *pstrTokenGroupingCol = CDXLTokens::PstrToken(EdxltokenGroupingCol);

		pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrTokenGroupingCols);

		for (ULONG ul = 0; ul < m_pdrgpulGrpColId->UlLength(); ul++)
		{
			GPOS_ASSERT(NULL != (*m_pdrgpulGrpColId)[ul]);
			ULONG ulGroupingCol = *((*m_pdrgpulGrpColId)[ul]);

			pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrTokenGroupingCol);
			pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenColId), ulGroupingCol);

			pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrTokenGroupingCol);
		}

		pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrTokenGroupingCols);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGroupBy::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLLogicalGroupBy::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	// serialize grouping columns
	SerializeGrpColsToDXL(pxmlser);

	// serialize children
	pdxln->SerializeChildrenToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGroupBy::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLLogicalGroupBy::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	// 1 Child node
	// 1 Group By project list

	const ULONG ulChildren = pdxln->UlArity();
	GPOS_ASSERT(2 == ulChildren);

	CDXLNode *pdxlnPrL = (*pdxln)[0];
	GPOS_ASSERT(EdxlopScalarProjectList == pdxlnPrL->Pdxlop()->Edxlop());

	CDXLNode *pdxlnOpType = (*pdxln)[1];
	GPOS_ASSERT(EdxloptypeLogical == pdxlnOpType->Pdxlop()->Edxloperatortype());

	if (fValidateChildren)
	{
		for(ULONG ul = 0; ul < ulChildren; ul++)
		{
			CDXLNode *pdxlnChild = (*pdxln)[ul];
			pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
		}
	}

	const ULONG ulArity = pdxlnPrL->UlArity();
	for (ULONG ul = 0; ul < ulArity; ++ul)
	{
		CDXLNode *pdxlnPrEl = (*pdxlnPrL)[ul];
		GPOS_ASSERT(EdxlopScalarIdent != pdxlnPrEl->Pdxlop()->Edxlop());
	}
}
#endif // GPOS_DEBUG

// EOF
