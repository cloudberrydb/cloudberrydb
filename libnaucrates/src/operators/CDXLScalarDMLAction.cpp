//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarDMLAction.cpp
//
//	@doc:
//		Implementation of DXL DML action expression
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarDMLAction.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpopt;
using namespace gpmd;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarDMLAction::CDXLScalarDMLAction
//
//	@doc:
//		Constructs an action expression
//
//---------------------------------------------------------------------------
CDXLScalarDMLAction::CDXLScalarDMLAction
	(
	IMemoryPool *pmp
	)
	:
	CDXLScalar(pmp)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarDMLAction::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarDMLAction::Edxlop() const
{
	return EdxlopScalarDMLAction;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarDMLAction::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarDMLAction::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarDMLAction);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarDMLAction::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarDMLAction::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode * // pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarDMLAction::FBoolean
//
//	@doc:
//		Does the operator return a boolean result
//
//---------------------------------------------------------------------------
BOOL
CDXLScalarDMLAction::FBoolean
	(
	CMDAccessor * // pmda
	)
	const
{
	return false;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarDMLAction::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarDMLAction::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL // fValidateChildren
	) 
	const
{
	GPOS_ASSERT(0 == pdxln->UlArity());
}
#endif // GPOS_DEBUG

// EOF
