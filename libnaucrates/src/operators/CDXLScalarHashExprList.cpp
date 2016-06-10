//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarHashExprList.cpp
//
//	@doc:
//		Implementation of DXL hash expression lists for redistribute operators
//---------------------------------------------------------------------------
#include "naucrates/dxl/operators/CDXLScalarHashExprList.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarHashExprList::CDXLScalarHashExprList
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarHashExprList::CDXLScalarHashExprList
	(
	IMemoryPool *pmp
	)
	:
	CDXLScalar(pmp)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarHashExprList::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarHashExprList::Edxlop() const
{
	return EdxlopScalarHashExprList;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarHashExprList::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarHashExprList::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarHashExprList);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarHashExprList::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarHashExprList::SerializeToDXL
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
//		CDXLScalarHashExprList::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarHashExprList::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren 
	) 
	const
{
	const ULONG ulArity = pdxln->UlArity();
	GPOS_ASSERT(1 <= ulArity);

	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CDXLNode *pdxlnChild = (*pdxln)[ul];
		GPOS_ASSERT(EdxlopScalarHashExpr == pdxlnChild->Pdxlop()->Edxlop());
		
		if (fValidateChildren)
		{
			pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
		}
	}
}

#endif // GPOS_DEBUG



// EOF
