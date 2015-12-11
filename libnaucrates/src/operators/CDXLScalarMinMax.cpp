//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Inc.
//
//	@filename:
//		CDXLScalarMinMax.cpp
//
//	@doc:
//		Implementation of DXL MinMax
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarMinMax.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpopt;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarMinMax::CDXLScalarMinMax
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarMinMax::CDXLScalarMinMax
	(
	IMemoryPool *pmp,
	IMDId *pmdidType,
	EdxlMinMaxType emmt
	)
	:
	CDXLScalar(pmp),
	m_pmdidType(pmdidType),
	m_emmt(emmt)
{
	GPOS_ASSERT(m_pmdidType->FValid());
	GPOS_ASSERT(EmmtSentinel > emmt);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarMinMax::~CDXLScalarMinMax
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarMinMax::~CDXLScalarMinMax()
{
	m_pmdidType->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarMinMax::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarMinMax::Edxlop() const
{
	return EdxlopScalarMinMax;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarMinMax::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarMinMax::PstrOpName() const
{
	switch (m_emmt)
	{
		case EmmtMin:
				return CDXLTokens::PstrToken(EdxltokenScalarMin);
		case EmmtMax:
				return CDXLTokens::PstrToken(EdxltokenScalarMax);
		default:
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarMinMax::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarMinMax::SerializeToDXL
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
//		CDXLScalarMinMax::FBoolean
//
//	@doc:
//		Does the operator return a boolean result
//
//---------------------------------------------------------------------------
BOOL
CDXLScalarMinMax::FBoolean
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
//		CDXLScalarMinMax::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarMinMax::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	)
	const
{
	GPOS_ASSERT(0 < pdxln->UlArity());

	const ULONG ulArity = pdxln->UlArity();
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
