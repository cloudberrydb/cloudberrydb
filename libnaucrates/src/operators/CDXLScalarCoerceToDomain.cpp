//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLScalarCoerceToDomain.cpp
//
//	@doc:
//		Implementation of DXL scalar coerce
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarCoerceToDomain.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpopt;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCoerceToDomain::CDXLScalarCoerceToDomain
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarCoerceToDomain::CDXLScalarCoerceToDomain
	(
	IMemoryPool *pmp,
	IMDId *pmdidType,
	INT iMod,
	EdxlCoercionForm edxlcf,
	INT iLoc
	)
	:
	CDXLScalar(pmp),
	m_pmdidResultType(pmdidType),
	m_iMod(iMod),
	m_edxlcf(edxlcf),
	m_iLoc(iLoc)
{
	GPOS_ASSERT(NULL != pmdidType);
	GPOS_ASSERT(pmdidType->FValid());
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCoerceToDomain::~CDXLScalarCoerceToDomain
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarCoerceToDomain::~CDXLScalarCoerceToDomain()
{
	m_pmdidResultType->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCoerceToDomain::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarCoerceToDomain::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarCoerceToDomain);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCoerceToDomain::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarCoerceToDomain::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	m_pmdidResultType->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenTypeId));

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenTypeMod), m_iMod);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenCoercionForm), (ULONG) m_edxlcf);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenLocation), m_iLoc);

	pdxln->SerializeChildrenToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCoerceToDomain::FBoolean
//
//	@doc:
//		Does the operator return a boolean result
//
//---------------------------------------------------------------------------
BOOL
CDXLScalarCoerceToDomain::FBoolean
	(
	CMDAccessor *pmda
	)
	const
{
	return (IMDType::EtiBool == pmda->Pmdtype(m_pmdidResultType)->Eti());
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCoerceToDomain::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarCoerceToDomain::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) const
{
	GPOS_ASSERT(1 == pdxln->UlArity());

	CDXLNode *pdxlnChild = (*pdxln)[0];
	GPOS_ASSERT(EdxloptypeScalar == pdxlnChild->Pdxlop()->Edxloperatortype());

	if (fValidateChildren)
	{
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
