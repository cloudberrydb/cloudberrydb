//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC, Corp.
//
//	@filename:
//		CDXLScalarSubqueryAny.cpp
//
//	@doc:
//		Implementation of subquery ANY
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLScalarSubqueryAny.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryAny::CDXLScalarSubqueryAny
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarSubqueryAny::CDXLScalarSubqueryAny
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
//		CDXLScalarSubqueryAny::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarSubqueryAny::Edxlop() const
{
	return EdxlopScalarSubqueryAny;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryAny::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarSubqueryAny::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarSubqueryAny);
}

// EOF
