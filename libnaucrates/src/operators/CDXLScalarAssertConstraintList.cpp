//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Inc.
//
//	@filename:
//		CDXLScalarAssertConstraintList.cpp
//
//	@doc:
//		Implementation of DXL scalar assert constraint lists
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarAssertConstraintList.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAssertConstraintList::CDXLScalarAssertConstraintList
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarAssertConstraintList::CDXLScalarAssertConstraintList
	(
	IMemoryPool *pmp
	)
	:
	CDXLScalar(pmp)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAssertConstraintList::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarAssertConstraintList::Edxlop() const
{
	return EdxlopScalarAssertConstraintList;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAssertConstraintList::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarAssertConstraintList::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarAssertConstraintList);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAssertConstraintList::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarAssertConstraintList::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	pdxln->SerializeChildrenToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}


#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAssertConstraintList::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarAssertConstraintList::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	)
	const
{
	const ULONG ulArity = pdxln->UlArity();
	GPOS_ASSERT(0 < ulArity);
	
	for (ULONG ul = 0; ul < ulArity; ++ul)
	{
		CDXLNode *pdxlnChild = (*pdxln)[ul];
		GPOS_ASSERT(EdxlopScalarAssertConstraint == pdxlnChild->Pdxlop()->Edxlop());

		if (fValidateChildren)
		{
			pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
		}
	}
}
#endif // GPOS_DEBUG

// EOF
