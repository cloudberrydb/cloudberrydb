//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC, Corp.
//
//	@filename:
//		CDXLScalarSubqueryExists.cpp
//
//	@doc:
//		Implementation of EXISTS subqueries
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLScalarSubqueryExists.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryExists::CDXLScalarSubqueryExists
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarSubqueryExists::CDXLScalarSubqueryExists
	(
	IMemoryPool *pmp
	)
	:
	CDXLScalar(pmp)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryExists::~CDXLScalarSubqueryExists
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLScalarSubqueryExists::~CDXLScalarSubqueryExists()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryExists::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarSubqueryExists::Edxlop() const
{
	return EdxlopScalarSubqueryExists;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryExists::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarSubqueryExists::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarSubqueryExists);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryExists::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarSubqueryExists::SerializeToDXL
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
//		CDXLScalarSubqueryExists::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLScalarSubqueryExists::AssertValid
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
