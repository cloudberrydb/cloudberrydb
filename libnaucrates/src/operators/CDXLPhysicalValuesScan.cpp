//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software, Inc.
//
//	@filename:
//		CDXLPhysicalValuesScan.cpp
//
//	@doc:
//		Implementation of DXL physical values scan operator
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalValuesScan.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

// ctor
CDXLPhysicalValuesScan::CDXLPhysicalValuesScan
	(
	IMemoryPool *pmp
	)
	:
	CDXLPhysical(pmp)
{}

// dtor
CDXLPhysicalValuesScan::~CDXLPhysicalValuesScan
	(
	)
{}

// operator type
Edxlopid
CDXLPhysicalValuesScan::Edxlop() const
{
	return EdxlopPhysicalValuesScan;
}

// operator name
const CWStringConst *
CDXLPhysicalValuesScan::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalValuesScan);
}

CDXLPhysicalValuesScan *
CDXLPhysicalValuesScan::PdxlopConvert
	(
	CDXLOperator *pdxlop
	)
{
	GPOS_ASSERT(NULL != pdxlop);
	GPOS_ASSERT(EdxlopPhysicalValuesScan ==pdxlop->Edxlop());

	return dynamic_cast<CDXLPhysicalValuesScan *>(pdxlop);
}
// serialize operator in DXL format
void
CDXLPhysicalValuesScan::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	// serialize properties
	pdxln->SerializePropertiesToDXL(pxmlser);

	// serialize children
	pdxln->SerializeChildrenToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG

// checks whether operator node is well-structured
void
CDXLPhysicalValuesScan::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	)
const
{
	GPOS_ASSERT(EdxloptypePhysical == pdxln->Pdxlop()->Edxloperatortype());

	const ULONG ulArity = pdxln->UlArity();
	GPOS_ASSERT(EdxlValIndexSentinel <= ulArity);

	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CDXLNode *pdxlnChild = (*pdxln)[ul];
		GPOS_ASSERT(EdxloptypeScalar == pdxlnChild->Pdxlop()->Edxloperatortype());
		if (fValidateChildren)
		{
			pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
		}
	}
}
#endif // GPOS_DEBUG

// EOF
