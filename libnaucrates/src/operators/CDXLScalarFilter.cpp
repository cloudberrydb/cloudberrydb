//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarFilter.cpp
//
//	@doc:
//		Implementation of DXL physical filter operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLScalarFilter.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFilter::CDXLScalarFilter
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarFilter::CDXLScalarFilter
	(
	IMemoryPool *pmp
	)
	:
	CDXLScalar(pmp)
{
}



//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFilter::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarFilter::Edxlop() const
{
	return EdxlopScalarFilter;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFilter::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarFilter::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarFilter);;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarFilter::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarFilter::SerializeToDXL
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
//		CDXLScalarFilter::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarFilter::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren 
	) 
	const
{
	GPOS_ASSERT(1 >= pdxln->UlArity());
	
	if (1 == pdxln->UlArity())
	{
		CDXLNode *pdxlnChild = (*pdxln)[0];
		
		GPOS_ASSERT(EdxloptypeScalar == pdxlnChild->Pdxlop()->Edxloperatortype());
	
		if (fValidateChildren)
		{
			pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
		}
	}
}

#endif // GPOS_DEBUG


// EOF
