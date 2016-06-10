//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarCaseTest.cpp
//
//	@doc:
//		Implementation of DXL case test
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarCaseTest.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpopt;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCaseTest::CDXLScalarCaseTest
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarCaseTest::CDXLScalarCaseTest
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
//		CDXLScalarCaseTest::~CDXLScalarCaseTest
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarCaseTest::~CDXLScalarCaseTest()
{
	m_pmdidType->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCaseTest::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarCaseTest::Edxlop() const
{
	return EdxlopScalarCaseTest;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCaseTest::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarCaseTest::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarCaseTest);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCaseTest::PmdidType
//
//	@doc:
//		Return type id
//
//---------------------------------------------------------------------------
IMDId *
CDXLScalarCaseTest::PmdidType() const
{
	return m_pmdidType;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCaseTest::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarCaseTest::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode * //pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	m_pmdidType->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenTypeId));
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCaseTest::FBoolean
//
//	@doc:
//		Does the operator return a boolean result
//
//---------------------------------------------------------------------------
BOOL
CDXLScalarCaseTest::FBoolean
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
//		CDXLScalarCaseTest::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarCaseTest::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL // fValidateChildren
	)
	const
{
	GPOS_ASSERT(0 == pdxln->UlArity());
	GPOS_ASSERT(m_pmdidType->FValid());
}
#endif // GPOS_DEBUG

// EOF
