//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarSortColList.cpp
//
//	@doc:
//		Implementation of DXL sorting column lists for sort and motion operator nodes.
//---------------------------------------------------------------------------
#include "naucrates/dxl/operators/CDXLScalarSortColList.h"

#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSortColList::CDXLScalarSortColList
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarSortColList::CDXLScalarSortColList
	(
	IMemoryPool *pmp
	)
	:
	CDXLScalar(pmp)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSortColList::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarSortColList::Edxlop() const
{
	return EdxlopScalarSortColList;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSortColList::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarSortColList::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarSortColList);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSortColList::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarSortColList::SerializeToDXL
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
//		CDXLScalarSortColList::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLScalarSortColList::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	const ULONG ulArity = pdxln->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CDXLNode *pdxlnChild = (*pdxln)[ul];
		GPOS_ASSERT(EdxlopScalarSortCol == pdxlnChild->Pdxlop()->Edxlop());
		
		if (fValidateChildren)
		{
			pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
		}
	}
}
#endif // GPOS_DEBUG

// EOF
