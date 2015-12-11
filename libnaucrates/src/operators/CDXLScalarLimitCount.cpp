//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarLimitCount.cpp
//
//	@doc:
//		Implementation of DXL Scalar Limit Count
//		
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarLimitCount.h"

#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarLimitCount::CDXLScalarLimitCount
//
//	@doc:
//		Constructs a scalar Limit Count node
//
//---------------------------------------------------------------------------
CDXLScalarLimitCount::CDXLScalarLimitCount
	(
	IMemoryPool *pmp
	)
	:
	CDXLScalar(pmp)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarLimitCount::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarLimitCount::Edxlop() const
{
	return EdxlopScalarLimitCount;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarLimitCount::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarLimitCount::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarLimitCount);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarLimitCount::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarLimitCount::SerializeToDXL
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
//		CDXLScalarLimitCount::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarLimitCount::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	const ULONG ulArity = pdxln->UlArity();
	GPOS_ASSERT(1 >= ulArity);

	for (ULONG ul = 0; ul < ulArity; ++ul)
	{
		CDXLNode *pdxlnArg = (*pdxln)[ul];
		GPOS_ASSERT(EdxloptypeScalar == pdxlnArg->Pdxlop()->Edxloperatortype());
		
		if (fValidateChildren)
		{
			pdxlnArg->Pdxlop()->AssertValid(pdxlnArg, fValidateChildren);
		}
	}
}
#endif // GPOS_DEBUG

// EOF
