//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarNullIf.cpp
//
//	@doc:
//		Implementation of DXL NullIf operator
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarNullIf.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullIf::CDXLScalarNullIf
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarNullIf::CDXLScalarNullIf
	(
	IMemoryPool *pmp,
	IMDId *pmdidOp,
	IMDId *pmdidType
	)
	:
	CDXLScalar(pmp),
	m_pmdidOp(pmdidOp),
	m_pmdidType(pmdidType)
{
	GPOS_ASSERT(pmdidOp->FValid());
	GPOS_ASSERT(pmdidType->FValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullIf::~CDXLScalarNullIf
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarNullIf::~CDXLScalarNullIf()
{
	m_pmdidOp->Release();
	m_pmdidType->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullIf::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarNullIf::Edxlop() const
{
	return EdxlopScalarNullIf;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullIf::PmdidOp
//
//	@doc:
//		Operator id
//
//---------------------------------------------------------------------------
IMDId *
CDXLScalarNullIf::PmdidOp() const
{
	return m_pmdidOp;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullIf::PmdidType
//
//	@doc:
//		Return type
//
//---------------------------------------------------------------------------
IMDId *
CDXLScalarNullIf::PmdidType() const
{
	return m_pmdidType;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullIf::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarNullIf::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarNullIf);;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullIf::FBoolean
//
//	@doc:
//		Does the operator return boolean result
//
//---------------------------------------------------------------------------
BOOL
CDXLScalarNullIf::FBoolean
	(
	CMDAccessor *pmda
	)
	const
{
	return (IMDType::EtiBool == pmda->Pmdtype(m_pmdidType)->Eti());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullIf::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarNullIf::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	m_pmdidOp->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenOpNo));
	m_pmdidType->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenTypeId));

	pdxln->SerializeChildrenToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullIf::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarNullIf::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	)
	const
{
	const ULONG ulArity = pdxln->UlArity();
	GPOS_ASSERT(2 == ulArity);

	for (ULONG ul = 0; ul < ulArity; ++ul)
	{
		CDXLNode *pdxlnChild = (*pdxln)[ul];
		GPOS_ASSERT(EdxloptypeScalar == pdxlnChild->Pdxlop()->Edxloperatortype());

		if (fValidateChildren)
		{
			pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
		}
	}
}
#endif // GPOS_DEBUG

// EOF
