//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarIfStmt.cpp
//
//	@doc:
//		Implementation of DXL If Statement
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarIfStmt.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpopt;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarIfStmt::CDXLScalarIfStmt
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarIfStmt::CDXLScalarIfStmt
	(
	IMemoryPool *pmp,
	IMDId *pmdidResultType
	)
	:
	CDXLScalar(pmp),
	m_pmdidResultType(pmdidResultType)
{
	GPOS_ASSERT(m_pmdidResultType->FValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarIfStmt::~CDXLScalarIfStmt
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLScalarIfStmt::~CDXLScalarIfStmt()
{
	m_pmdidResultType->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarIfStmt::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarIfStmt::Edxlop() const
{
	return EdxlopScalarIfStmt;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarIfStmt::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarIfStmt::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarIfStmt);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarIfStmt::PmdidResultType
//
//	@doc:
//		Return type id
//
//---------------------------------------------------------------------------
IMDId *
CDXLScalarIfStmt::PmdidResultType() const
{
	return m_pmdidResultType;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarIfStmt::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarIfStmt::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	m_pmdidResultType->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenTypeId));
	pdxln->SerializeChildrenToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarIfStmt::FBoolean
//
//	@doc:
//		Does the operator return a boolean result
//
//---------------------------------------------------------------------------
BOOL
CDXLScalarIfStmt::FBoolean
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
//		CDXLScalarIfStmt::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarIfStmt::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	const ULONG ulArity = pdxln->UlArity();
	GPOS_ASSERT(3 == ulArity);

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
