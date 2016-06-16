//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarMergeCondList.cpp
//
//	@doc:
//		Implementation of DXL merge condition lists for merge join operators
//---------------------------------------------------------------------------
#include "naucrates/dxl/operators/CDXLScalarMergeCondList.h"

#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarMergeCondList::CDXLScalarMergeCondList
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarMergeCondList::CDXLScalarMergeCondList
	(
	IMemoryPool *pmp
	)
	:
	CDXLScalar(pmp)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarMergeCondList::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarMergeCondList::Edxlop() const
{
	return EdxlopScalarMergeCondList;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarMergeCondList::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarMergeCondList::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarMergeCondList);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarMergeCondList::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarMergeCondList::SerializeToDXL
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
//		CDXLScalarMergeCondList::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLScalarMergeCondList::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	GPOS_ASSERT(NULL != pdxln);
	
	const ULONG ulArity = pdxln->UlArity();
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
