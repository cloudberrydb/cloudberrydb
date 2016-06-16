//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalAgg.cpp
//
//	@doc:
//		Implementation of DXL physical aggregate operator
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLPhysicalAgg.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAgg::CDXLPhysicalAgg
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalAgg::CDXLPhysicalAgg
	(
	IMemoryPool *pmp,
	EdxlAggStrategy edxlaggstr,
	BOOL fStreamSafe
	)
	:
	CDXLPhysical(pmp),
	m_pdrgpulGroupingCols(NULL),
	m_edxlaggstr(edxlaggstr),
	m_fStreamSafe(fStreamSafe)
{
	GPOS_ASSERT_IMP(fStreamSafe, (EdxlaggstrategyHashed == edxlaggstr));
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAgg::~CDXLPhysicalAgg
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLPhysicalAgg::~CDXLPhysicalAgg()
{
	CRefCount::SafeRelease(m_pdrgpulGroupingCols);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAgg::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalAgg::Edxlop() const
{
	return EdxlopPhysicalAgg;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAgg::Edxlaggstr
//
//	@doc:
//		Aggregation strategy
//
//---------------------------------------------------------------------------
EdxlAggStrategy
CDXLPhysicalAgg::Edxlaggstr() const
{
	return m_edxlaggstr;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAgg::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalAgg::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalAggregate);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAgg::PstrAggStrategy
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalAgg::PstrAggStrategy() const
{
	switch (m_edxlaggstr)
	{
		case EdxlaggstrategyPlain:
			return CDXLTokens::PstrToken(EdxltokenAggStrategyPlain);
		case EdxlaggstrategySorted:
			return CDXLTokens::PstrToken(EdxltokenAggStrategySorted);
		case EdxlaggstrategyHashed:
			return CDXLTokens::PstrToken(EdxltokenAggStrategyHashed);
		default:
			GPOS_ASSERT(!"Unrecognized aggregation strategy");
			return NULL;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAgg::PdrgpulGroupingCols
//
//	@doc:
//		Grouping column indices
//
//---------------------------------------------------------------------------
const DrgPul *
CDXLPhysicalAgg::PdrgpulGroupingCols() const
{
	return m_pdrgpulGroupingCols; 
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAgg::SetGroupingCols
//
//	@doc:
//		Sets array of grouping columns
//
//---------------------------------------------------------------------------
void
CDXLPhysicalAgg::SetGroupingCols(DrgPul *pdrgpul)
{
	GPOS_ASSERT(NULL != pdrgpul);
	m_pdrgpulGroupingCols = pdrgpul;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAgg::SerializeGroupingColsToDXL
//
//	@doc:
//		Serialize grouping column indices in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalAgg::SerializeGroupingColsToDXL
	(
	CXMLSerializer *pxmlser
	)
	const
{
	GPOS_ASSERT(NULL != m_pdrgpulGroupingCols);
	
	const CWStringConst *pstrTokenGroupingCols = CDXLTokens::PstrToken(EdxltokenGroupingCols);
	const CWStringConst *pstrTokenGroupingCol = CDXLTokens::PstrToken(EdxltokenGroupingCol);
		
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrTokenGroupingCols);
	
	for (ULONG ul = 0; ul < m_pdrgpulGroupingCols->UlLength(); ul++)
	{
		GPOS_ASSERT(NULL != (*m_pdrgpulGroupingCols)[ul]);
		ULONG ulGroupingCol = *((*m_pdrgpulGroupingCols)[ul]);
		
		pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrTokenGroupingCol);
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenColId), ulGroupingCol);
		pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrTokenGroupingCol);
	}
	
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrTokenGroupingCols);	
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAgg::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalAgg::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenAggStrategy), PstrAggStrategy());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenAggStreamSafe), m_fStreamSafe);
	
	// serialize properties
	pdxln->SerializePropertiesToDXL(pxmlser);
	SerializeGroupingColsToDXL(pxmlser);
	
	// serialize children
	pdxln->SerializeChildrenToDXL(pxmlser);
	
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);		
}


#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAgg::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLPhysicalAgg::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) const
{
	// assert proj list and filter are valid
	CDXLPhysical::AssertValid(pdxln, fValidateChildren);
	
	GPOS_ASSERT((EdxlaggstrategySentinel > m_edxlaggstr) && (EdxlaggstrategyPlain <= m_edxlaggstr));

	GPOS_ASSERT(EdxlaggIndexSentinel == pdxln->UlArity());
	GPOS_ASSERT(NULL != m_pdrgpulGroupingCols);
	
	CDXLNode *pdxlnChild = (*pdxln)[EdxlaggIndexChild];
	GPOS_ASSERT(EdxloptypePhysical == pdxlnChild->Pdxlop()->Edxloperatortype());
	
	if (fValidateChildren)
	{
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
