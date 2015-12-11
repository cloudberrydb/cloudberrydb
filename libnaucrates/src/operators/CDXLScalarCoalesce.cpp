//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarCoalesce.cpp
//
//	@doc:
//		Implementation of DXL Coalesce
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarCoalesce.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpopt;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCoalesce::CDXLScalarCoalesce
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarCoalesce::CDXLScalarCoalesce
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
//		CDXLScalarCoalesce::~CDXLScalarCoalesce
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarCoalesce::~CDXLScalarCoalesce()
{
	m_pmdidType->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCoalesce::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarCoalesce::Edxlop() const
{
	return EdxlopScalarCoalesce;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCoalesce::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarCoalesce::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarCoalesce);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCoalesce::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarCoalesce::SerializeToDXL
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
//		CDXLScalarCoalesce::FBoolean
//
//	@doc:
//		Does the operator return a boolean result
//
//---------------------------------------------------------------------------
BOOL
CDXLScalarCoalesce::FBoolean
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
//		CDXLScalarCoalesce::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarCoalesce::AssertValid
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
