//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalSequence.cpp
//
//	@doc:
//		Implementation of DXL physical sequence operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLPhysicalSequence.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSequence::CDXLPhysicalSequence
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalSequence::CDXLPhysicalSequence
	(
	IMemoryPool *pmp
	)
	:
	CDXLPhysical(pmp)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSequence::~CDXLPhysicalSequence
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLPhysicalSequence::~CDXLPhysicalSequence()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSequence::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalSequence::Edxlop() const
{
	return EdxlopPhysicalSequence;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSequence::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalSequence::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalSequence);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSequence::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalSequence::SerializeToDXL
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
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSequence::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalSequence::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{

	const ULONG ulArity = pdxln->UlArity();  
	GPOS_ASSERT(1 < ulArity);

	for (ULONG ul = 1; ul < ulArity; ul++)
	{
		CDXLNode *pdxlnChild = (*pdxln)[ul];
		GPOS_ASSERT(EdxloptypePhysical == pdxlnChild->Pdxlop()->Edxloperatortype());

		if (fValidateChildren)
		{
			pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
		}
	}

}
#endif // GPOS_DEBUG

// EOF
