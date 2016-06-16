//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarBooleanTest.cpp
//
//	@doc:
//		Implementation of DXL BooleanTest
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarBooleanTest.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBooleanTest::CDXLScalarBooleanTest
//
//	@doc:
//		Constructs a BooleanTest node
//
//---------------------------------------------------------------------------
CDXLScalarBooleanTest::CDXLScalarBooleanTest
	(
	IMemoryPool *pmp,
	const EdxlBooleanTestType edxlbooleanTestType
	)
	:
	CDXLScalar(pmp),
	m_edxlbooleantesttype(edxlbooleanTestType)
{

}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBooleanTest::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarBooleanTest::Edxlop() const
{
	return EdxlopScalarBooleanTest;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBooleanTest::EdxlBoolType
//
//	@doc:
//		Boolean Test type
//
//---------------------------------------------------------------------------
EdxlBooleanTestType
CDXLScalarBooleanTest::EdxlBoolType() const
{
	return m_edxlbooleantesttype;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBooleanTest::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarBooleanTest::PstrOpName() const
{
	switch (m_edxlbooleantesttype)
	{
		case EdxlbooleantestIsTrue:
				return CDXLTokens::PstrToken(EdxltokenScalarBoolTestIsTrue);
		case EdxlbooleantestIsNotTrue:
				return CDXLTokens::PstrToken(EdxltokenScalarBoolTestIsNotTrue);
		case EdxlbooleantestIsFalse:
				return CDXLTokens::PstrToken(EdxltokenScalarBoolTestIsFalse);
		case EdxlbooleantestIsNotFalse:
				return CDXLTokens::PstrToken(EdxltokenScalarBoolTestIsNotFalse);
		case EdxlbooleantestIsUnknown:
				return CDXLTokens::PstrToken(EdxltokenScalarBoolTestIsUnknown);
		case EdxlbooleantestIsNotUnknown:
				return CDXLTokens::PstrToken(EdxltokenScalarBoolTestIsNotUnknown);
		default:
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBooleanTest::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarBooleanTest::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	GPOS_ASSERT(NULL != pstrElemName);
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	pdxln->SerializeChildrenToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBooleanTest::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarBooleanTest::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{

	EdxlBooleanTestType edxlbooltype = ((CDXLScalarBooleanTest *) pdxln->Pdxlop())->EdxlBoolType();

	GPOS_ASSERT( (EdxlbooleantestIsTrue == edxlbooltype) || (EdxlbooleantestIsNotTrue == edxlbooltype) || (EdxlbooleantestIsFalse == edxlbooltype)
			|| (EdxlbooleantestIsNotFalse == edxlbooltype)|| (EdxlbooleantestIsUnknown == edxlbooltype)|| (EdxlbooleantestIsNotUnknown == edxlbooltype));

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
