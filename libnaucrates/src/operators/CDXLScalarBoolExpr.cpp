//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarBoolExpr.cpp
//
//	@doc:
//		Implementation of DXL BoolExpr
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarBoolExpr.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBoolExpr::CDXLScalarBoolExpr
//
//	@doc:
//		Constructs a BoolExpr node
//
//---------------------------------------------------------------------------
CDXLScalarBoolExpr::CDXLScalarBoolExpr
	(
	IMemoryPool *pmp,
	const EdxlBoolExprType boolexptype
	)
	:
	CDXLScalar(pmp),
	m_boolexptype(boolexptype)
{

}



//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBoolExpr::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarBoolExpr::Edxlop() const
{
	return EdxlopScalarBoolExpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBoolExpr::EdxlBoolType
//
//	@doc:
//		Boolean expression type
//
//---------------------------------------------------------------------------
EdxlBoolExprType
CDXLScalarBoolExpr::EdxlBoolType() const
{
	return m_boolexptype;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBoolExpr::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarBoolExpr::PstrOpName() const
{
	switch (m_boolexptype)
	{
		case Edxland:
				return CDXLTokens::PstrToken(EdxltokenScalarBoolAnd);
		case Edxlor:
				return CDXLTokens::PstrToken(EdxltokenScalarBoolOr);
		case Edxlnot:
				return CDXLTokens::PstrToken(EdxltokenScalarBoolNot);
		default:
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBoolExpr::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarBoolExpr::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	GPOS_CHECK_ABORT;

	const CWStringConst *pstrElemName = PstrOpName();

	GPOS_ASSERT(NULL != pstrElemName);
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	pdxln->SerializeChildrenToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	GPOS_CHECK_ABORT;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBoolExpr::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarBoolExpr::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	EdxlBoolExprType edxlbooltype = ((CDXLScalarBoolExpr *) pdxln->Pdxlop())->EdxlBoolType();

	GPOS_ASSERT( (edxlbooltype == Edxlnot) || (edxlbooltype == Edxlor) || (edxlbooltype == Edxland));

	const ULONG ulArity = pdxln->UlArity();
	if(edxlbooltype == Edxlnot)
	{
		GPOS_ASSERT(1 == ulArity);
	}
	else
	{
		GPOS_ASSERT(2 <= ulArity);
	}

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
