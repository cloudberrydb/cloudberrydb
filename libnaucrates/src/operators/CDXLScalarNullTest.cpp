//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarNullTest.cpp
//
//	@doc:
//		Implementation of DXL NullTest
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarNullTest.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/dxltokens.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullTest::CDXLScalarNullTest
//
//	@doc:
//		Constructs a NullTest node
//
//---------------------------------------------------------------------------
CDXLScalarNullTest::CDXLScalarNullTest
	(
	IMemoryPool *pmp,
	BOOL fIsNull
	)
	:
	CDXLScalar(pmp),
	m_fIsNull(fIsNull)
{

}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullTest::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarNullTest::Edxlop() const
{
	return EdxlopScalarNullTest;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullTest::FIsNullTest
//
//	@doc:
//		Null Test type (is null or is not null)
//
//---------------------------------------------------------------------------
BOOL
CDXLScalarNullTest::FIsNullTest() const
{
	return m_fIsNull;
}



//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullTest::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarNullTest::PstrOpName() const
{
	if(m_fIsNull)
	{
		return CDXLTokens::PstrToken(EdxltokenScalarIsNull);
	}
	return CDXLTokens::PstrToken(EdxltokenScalarIsNotNull);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullTest::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarNullTest::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	pdxln->SerializeChildrenToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarNullTest::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarNullTest::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	)
	const
{
	GPOS_ASSERT(1 == pdxln->UlArity());

	CDXLNode *pdxlnArg = (*pdxln)[0];
	GPOS_ASSERT(EdxloptypeScalar == pdxlnArg->Pdxlop()->Edxloperatortype());
	
	if (fValidateChildren)
	{
		pdxlnArg->Pdxlop()->AssertValid(pdxlnArg, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
