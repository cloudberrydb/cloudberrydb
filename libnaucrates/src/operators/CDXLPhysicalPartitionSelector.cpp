//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CDXLPhysicalPartitionSelector.cpp
//
//	@doc:
//		Implementation of DXL physical partition selector
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalPartitionSelector.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalPartitionSelector::CDXLPhysicalPartitionSelector
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLPhysicalPartitionSelector::CDXLPhysicalPartitionSelector
	(
	IMemoryPool *pmp,
	IMDId *pmdidRel,
	ULONG ulLevels,
	ULONG ulScanId
	)
	:
	CDXLPhysical(pmp),
	m_pmdidRel(pmdidRel),
	m_ulLevels(ulLevels),
	m_ulScanId(ulScanId)
{
	GPOS_ASSERT(pmdidRel->FValid());
	GPOS_ASSERT(0 < ulLevels);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalPartitionSelector::~CDXLPhysicalPartitionSelector
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLPhysicalPartitionSelector::~CDXLPhysicalPartitionSelector()
{
	m_pmdidRel->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalPartitionSelector::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalPartitionSelector::Edxlop() const
{
	return EdxlopPhysicalPartitionSelector;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalPartitionSelector::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalPartitionSelector::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalPartitionSelector);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalPartitionSelector::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalPartitionSelector::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	m_pmdidRel->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenRelationMdid));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenPhysicalPartitionSelectorLevels), m_ulLevels);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenPhysicalPartitionSelectorScanId), m_ulScanId);
	pdxln->SerializePropertiesToDXL(pxmlser);

	// serialize project list and filter lists
	(*pdxln)[EdxlpsIndexProjList]->SerializeToDXL(pxmlser);
	(*pdxln)[EdxlpsIndexEqFilters]->SerializeToDXL(pxmlser);
	(*pdxln)[EdxlpsIndexFilters]->SerializeToDXL(pxmlser);

	// serialize residual filter
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenScalarResidualFilter));
	(*pdxln)[EdxlpsIndexResidualFilter]->SerializeToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenScalarResidualFilter));

	// serialize propagation expression
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenScalarPropagationExpr));
	(*pdxln)[EdxlpsIndexPropExpr]->SerializeToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenScalarPropagationExpr));

	// serialize printable filter
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenScalarPrintableFilter));
	(*pdxln)[EdxlpsIndexPrintableFilter]->SerializeToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenScalarPrintableFilter));

	// serialize relational child, if any
	if (7 == pdxln->UlArity())
	{
		(*pdxln)[EdxlpsIndexChild]->SerializeToDXL(pxmlser);
	}

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalPartitionSelector::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalPartitionSelector::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	)
	const
{
	const ULONG ulArity = pdxln->UlArity();
	GPOS_ASSERT(6 == ulArity || 7 == ulArity);
	for (ULONG ul = 0; ul < ulArity; ++ul)
	{
		CDXLNode *pdxlnChild = (*pdxln)[ul];
		if (fValidateChildren)
		{
			pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
		}
	}
}
#endif // GPOS_DEBUG

// EOF
