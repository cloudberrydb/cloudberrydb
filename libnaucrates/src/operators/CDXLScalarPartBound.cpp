//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CDXLScalarPartBound.cpp
//
//	@doc:
//		Implementation of DXL Part Bound expression
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarPartBound.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpopt;
using namespace gpmd;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartBound::CDXLScalarPartBound
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarPartBound::CDXLScalarPartBound
	(
	IMemoryPool *pmp,
	ULONG ulLevel,
	IMDId *pmdidType,
	BOOL fLower
	)
	:
	CDXLScalar(pmp),
	m_ulLevel(ulLevel),
	m_pmdidType(pmdidType),
	m_fLower(fLower)
{
	GPOS_ASSERT(pmdidType->FValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartBound::~CDXLScalarPartBound
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarPartBound::~CDXLScalarPartBound()
{
	m_pmdidType->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartBound::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarPartBound::Edxlop() const
{
	return EdxlopScalarPartBound;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartBound::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarPartBound::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarPartBound);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartBound::FBoolean
//
//	@doc:
//		Does the operator return a boolean result
//
//---------------------------------------------------------------------------
BOOL
CDXLScalarPartBound::FBoolean(CMDAccessor *pmda) const
{
	return (IMDType::EtiBool == pmda->Pmdtype(m_pmdidType)->Eti());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartBound::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarPartBound::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode * // pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenPartLevel), m_ulLevel);
	m_pmdidType->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenMDType));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenScalarPartBoundLower), m_fLower);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartBound::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarPartBound::AssertValid
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
