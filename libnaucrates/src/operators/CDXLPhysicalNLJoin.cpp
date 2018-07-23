//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalNLJoin.cpp
//
//	@doc:
//		Implementation of DXL physical nested loop join operator
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLPhysicalNLJoin.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalNLJoin::CDXLPhysicalNLJoin
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalNLJoin::CDXLPhysicalNLJoin
	(
	IMemoryPool *pmp,
	EdxlJoinType edxljt,
	BOOL fIndexNLJ,
	BOOL nest_params_exists
	)
	:
	CDXLPhysicalJoin(pmp, edxljt),
	m_fIndexNLJ(fIndexNLJ),
	m_nest_params_exists(nest_params_exists)
{
	m_nest_params_col_refs = NULL;
}

CDXLPhysicalNLJoin::~CDXLPhysicalNLJoin()
{
	CRefCount::SafeRelease(m_nest_params_col_refs);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalNLJoin::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalNLJoin::Edxlop() const
{
	return EdxlopPhysicalNLJoin;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalNLJoin::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalNLJoin::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalNLJoin);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalNLJoin::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalNLJoin::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);		
	
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenJoinType), PstrJoinTypeName());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenPhysicalNLJoinIndex), m_fIndexNLJ);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenNLJIndexOuterRefAsParam), m_nest_params_exists);


	// serialize properties
	pdxln->SerializePropertiesToDXL(pxmlser);
	
	// serialize children
	pdxln->SerializeChildrenToDXL(pxmlser);

	// serialize nestloop params
	SerializeNestLoopParamsToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);		
}

void
CDXLPhysicalNLJoin::SerializeNestLoopParamsToDXL
(
 CXMLSerializer *xml_serializer
 )
const
{
	if (!m_nest_params_exists)
	{
		return;
	}

	// Serialize NLJ index paramlist
	xml_serializer->OpenElement
	(
	 CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
	 CDXLTokens::PstrToken(EdxltokenNLJIndexParamList)
	 );
	
	for (ULONG ul = 0; ul < m_nest_params_col_refs->UlLength(); ul++)
	{
		xml_serializer->OpenElement
		(
		CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
		CDXLTokens::PstrToken(EdxltokenNLJIndexParam)
		);
		
		ULONG id = (*m_nest_params_col_refs)[ul]->UlID();
		xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenColId), id);
		
		const CMDName *md_name = (*m_nest_params_col_refs)[ul]->Pmdname();
		const IMDId *mdid_type = (*m_nest_params_col_refs)[ul]->PmdidType();
		xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenColName), md_name->Pstr());
		mdid_type->Serialize(xml_serializer, CDXLTokens::PstrToken(EdxltokenTypeId));
		
		xml_serializer->CloseElement
		(
		CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
		CDXLTokens::PstrToken(EdxltokenNLJIndexParam)
		);
	}
	
	xml_serializer->CloseElement
	(
	CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
	CDXLTokens::PstrToken(EdxltokenNLJIndexParamList)
	);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalNLJoin::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLPhysicalNLJoin::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	// assert proj list and filter are valid
	CDXLPhysical::AssertValid(pdxln, fValidateChildren);
	
	GPOS_ASSERT(EdxlnljIndexSentinel == pdxln->UlArity());
	GPOS_ASSERT(EdxljtSentinel > Edxltype());
	
	CDXLNode *pdxlnJoinFilter = (*pdxln)[EdxlnljIndexJoinFilter];
	CDXLNode *pdxlnLeft = (*pdxln)[EdxlnljIndexLeftChild];
	CDXLNode *pdxlnRight = (*pdxln)[EdxlnljIndexRightChild];
	
	// assert children are of right type (physical/scalar)
	GPOS_ASSERT(EdxlopScalarJoinFilter == pdxlnJoinFilter->Pdxlop()->Edxlop());
	GPOS_ASSERT(EdxloptypePhysical == pdxlnLeft->Pdxlop()->Edxloperatortype());
	GPOS_ASSERT(EdxloptypePhysical == pdxlnRight->Pdxlop()->Edxloperatortype());
	
	if (fValidateChildren)
	{
		pdxlnJoinFilter->Pdxlop()->AssertValid(pdxlnJoinFilter, fValidateChildren);
		pdxlnLeft->Pdxlop()->AssertValid(pdxlnLeft, fValidateChildren);
		pdxlnRight->Pdxlop()->AssertValid(pdxlnRight, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

void
CDXLPhysicalNLJoin::SetNestLoopParamsColRefs(DrgPdxlcr *nest_params_col_refs)
{
	m_nest_params_col_refs = nest_params_col_refs;
}

BOOL
CDXLPhysicalNLJoin::NestParamsExists() const
{
	return m_nest_params_exists;
}

DrgPdxlcr *
CDXLPhysicalNLJoin::GetNestLoopParamsColRefs() const
{
	return m_nest_params_col_refs;
}
// EOF
