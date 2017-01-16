//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Inc.
//
//	@filename:
//		CDXLScalarArrayCoerceExpr.cpp
//
//	@doc:
//		Implementation of DXL scalar array coerce expr
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarArrayCoerceExpr.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpopt;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayCoerceExpr::CDXLScalarArrayCoerceExpr
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarArrayCoerceExpr::CDXLScalarArrayCoerceExpr
	(
	IMemoryPool *pmp,
	IMDId *pmdidElementFunc,
	IMDId *pmdidResultType,
	INT iMod,
	BOOL fIsExplicit,
	EdxlCoercionForm edxlcf,
	INT iLoc
	)
	:
	CDXLScalarCoerceBase(pmp, pmdidResultType, iMod, edxlcf, iLoc),
	m_pmdidElementFunc(pmdidElementFunc),
	m_fIsExplicit(fIsExplicit)
{
	GPOS_ASSERT(NULL != pmdidElementFunc);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayCoerceExpr::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarArrayCoerceExpr::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarArrayCoerceExpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayCoerceExpr::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarArrayCoerceExpr::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	m_pmdidElementFunc->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenElementFunc));
	PmdidResultType()->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenTypeId));

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenTypeMod), IMod());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenIsExplicit), m_fIsExplicit);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenCoercionForm), (ULONG) Edxlcf());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenLocation), ILoc());

	pdxln->SerializeChildrenToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

// EOF
