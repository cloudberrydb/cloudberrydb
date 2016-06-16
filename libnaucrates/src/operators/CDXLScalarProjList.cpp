//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarProjList.cpp
//
//	@doc:
//		Implementation of DXL projection list operators
//---------------------------------------------------------------------------
#include "naucrates/dxl/operators/CDXLScalarProjList.h"

#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarProjList::CDXLScalarProjList
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarProjList::CDXLScalarProjList
	(
	IMemoryPool *pmp
	)
	:
	CDXLScalar(pmp)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarProjList::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarProjList::Edxlop() const
{
	return EdxlopScalarProjectList;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarProjList::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarProjList::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarProjList);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarProjList::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarProjList::SerializeToDXL
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
//		CDXLScalarProjList::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarProjList::AssertValid
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
		GPOS_ASSERT(EdxlopScalarProjectElem == pdxlnChild->Pdxlop()->Edxlop());
		
		if (fValidateChildren)
		{
			pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
		}
	}
}

#endif // GPOS_DEBUG


// EOF
