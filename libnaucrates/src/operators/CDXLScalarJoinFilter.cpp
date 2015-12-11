//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarJoinFilter.cpp
//
//	@doc:
//		Implementation of DXL join filter operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLScalarJoinFilter.h"

#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarJoinFilter::CDXLScalarJoinFilter
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarJoinFilter::CDXLScalarJoinFilter
	(
	IMemoryPool *pmp
	)
	:
	CDXLScalarFilter(pmp)
{
}



//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarJoinFilter::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarJoinFilter::Edxlop() const
{
	return EdxlopScalarJoinFilter;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarJoinFilter::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarJoinFilter::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarJoinFilter);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarJoinFilter::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarJoinFilter::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode * pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	
	// serilize children
	pdxln->SerializeChildrenToDXL(pxmlser);
	
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);	
}


#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarJoinFilter::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLScalarJoinFilter::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	GPOS_ASSERT(1 >= pdxln->UlArity());
	
	if (1 == pdxln->UlArity())
	{
		CDXLNode *pdxlnCond = (*pdxln)[0];
		GPOS_ASSERT(EdxloptypeScalar == pdxlnCond->Pdxlop()->Edxloperatortype());
		
		if (fValidateChildren)
		{
			pdxlnCond->Pdxlop()->AssertValid(pdxlnCond, fValidateChildren);
		}
	}
	
}
#endif // GPOS_DEBUG


// EOF
