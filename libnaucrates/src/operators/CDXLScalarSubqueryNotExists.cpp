//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC, Corp.
//
//	@filename:
//		CDXLScalarSubqueryNotExists.cpp
//
//	@doc:
//		Implementation of NOT EXISTS subqueries
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLScalarSubqueryNotExists.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryNotExists::CDXLScalarSubqueryNotExists
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarSubqueryNotExists::CDXLScalarSubqueryNotExists
	(
	IMemoryPool *pmp
	)
	:
	CDXLScalar(pmp)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryNotExists::~CDXLScalarSubqueryNotExists
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLScalarSubqueryNotExists::~CDXLScalarSubqueryNotExists()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryNotExists::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarSubqueryNotExists::Edxlop() const
{
	return EdxlopScalarSubqueryNotExists;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryNotExists::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarSubqueryNotExists::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarSubqueryNotExists);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryNotExists::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarSubqueryNotExists::SerializeToDXL
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
//		CDXLScalarSubqueryNotExists::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLScalarSubqueryNotExists::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	GPOS_ASSERT(1 == pdxln->UlArity());
	
	CDXLNode *pdxlnChild = (*pdxln)[0];
	GPOS_ASSERT(EdxloptypeLogical == pdxlnChild->Pdxlop()->Edxloperatortype());

	pdxln->AssertValid(fValidateChildren);
}
#endif // GPOS_DEBUG

// EOF
