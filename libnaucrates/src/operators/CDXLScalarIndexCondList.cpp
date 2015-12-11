//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarIndexCondList.cpp
//
//	@doc:
//		Implementation of DXL index condition lists for DXL index scan operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#include "naucrates/dxl/operators/CDXLScalarIndexCondList.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarIndexCondList::CDXLScalarIndexCondList
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CDXLScalarIndexCondList::CDXLScalarIndexCondList
	(
	IMemoryPool *pmp
	)
	:
	CDXLScalar(pmp)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarIndexCondList::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarIndexCondList::Edxlop() const
{
	return EdxlopScalarIndexCondList;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarIndexCondList::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarIndexCondList::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarIndexCondList);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarIndexCondList::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarIndexCondList::SerializeToDXL
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
//		CDXLScalarIndexCondList::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarIndexCondList::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	)
	const
{
	GPOS_ASSERT(NULL != pdxln);

	if (fValidateChildren)
	{
		const ULONG ulArity = pdxln->UlArity();
		for (ULONG ul = 0; ul < ulArity; ul++)
		{
			CDXLNode *pdxlnChild = (*pdxln)[ul];
			GPOS_ASSERT(EdxloptypeScalar == pdxlnChild->Pdxlop()->Edxloperatortype());
			pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
		}
	}
}
#endif // GPOS_DEBUG

// EOF
