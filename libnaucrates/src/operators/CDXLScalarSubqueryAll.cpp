//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC, Corp.
//
//	@filename:
//		CDXLScalarSubqueryAll.cpp
//
//	@doc:
//		Implementation of subquery ALL
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLScalarSubqueryAll.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryAll::CDXLScalarSubqueryAll
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarSubqueryAll::CDXLScalarSubqueryAll
	(
	IMemoryPool *pmp,
	IMDId *pmdidScalarOp,
	CMDName *pmdnameScalarOp,
	ULONG ulColId
	)
	:
	CDXLScalarSubqueryQuantified(pmp, pmdidScalarOp, pmdnameScalarOp, ulColId)
{}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryAll::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarSubqueryAll::Edxlop() const
{
	return EdxlopScalarSubqueryAll;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryAll::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarSubqueryAll::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarSubqueryAll);
}

// EOF
