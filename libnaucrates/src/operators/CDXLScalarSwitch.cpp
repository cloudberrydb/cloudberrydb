//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarSwitch.cpp
//
//	@doc:
//		Implementation of DXL Switch
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarSwitch.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpopt;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSwitch::CDXLScalarSwitch
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarSwitch::CDXLScalarSwitch
	(
	IMemoryPool *pmp,
	IMDId *pmdidType
	)
	:
	CDXLScalar(pmp),
	m_pmdidType(pmdidType)
{
	GPOS_ASSERT(m_pmdidType->FValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSwitch::~CDXLScalarSwitch
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarSwitch::~CDXLScalarSwitch()
{
	m_pmdidType->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSwitch::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarSwitch::Edxlop() const
{
	return EdxlopScalarSwitch;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSwitch::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarSwitch::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarSwitch);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSwitch::PmdidType
//
//	@doc:
//		Return type id
//
//---------------------------------------------------------------------------
IMDId *
CDXLScalarSwitch::PmdidType() const
{
	return m_pmdidType;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSwitch::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarSwitch::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	m_pmdidType->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenTypeId));
	pdxln->SerializeChildrenToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSwitch::FBoolean
//
//	@doc:
//		Does the operator return a boolean result
//
//---------------------------------------------------------------------------
BOOL
CDXLScalarSwitch::FBoolean
	(
	CMDAccessor *pmda
	)
	const
{
	return (IMDType::EtiBool == pmda->Pmdtype(m_pmdidType)->Eti());
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSwitch::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarSwitch::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	)
	const
{
	const ULONG ulArity = pdxln->UlArity();
	GPOS_ASSERT(1 < ulArity);

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
