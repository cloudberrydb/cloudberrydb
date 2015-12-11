//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Inc.
//
//	@filename:
//		CDXLScalarAssertConstraint.cpp
//
//	@doc:
//		Implementation of DXL scalar assert predicate
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarAssertConstraint.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAssertConstraint::CDXLScalarAssertConstraint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarAssertConstraint::CDXLScalarAssertConstraint
	(
	IMemoryPool *pmp,
	CWStringBase *pstrErrorMsg
	)
	:
	CDXLScalar(pmp),
	m_pstrErrorMsg(pstrErrorMsg)
{
	GPOS_ASSERT(NULL != pstrErrorMsg);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAssertConstraint::~CDXLScalarAssertConstraint
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarAssertConstraint::~CDXLScalarAssertConstraint()
{
	GPOS_DELETE(m_pstrErrorMsg);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAssertConstraint::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarAssertConstraint::Edxlop() const
{
	return EdxlopScalarAssertConstraint;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAssertConstraint::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarAssertConstraint::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarAssertConstraint);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAssertConstraint::PstrErrorMsg
//
//	@doc:
//		Error message
//
//---------------------------------------------------------------------------
CWStringBase *
CDXLScalarAssertConstraint::PstrErrorMsg() const
{
	return m_pstrErrorMsg;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAssertConstraint::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarAssertConstraint::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenErrorMessage), m_pstrErrorMsg);
		
	pdxln->SerializeChildrenToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}


#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAssertConstraint::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarAssertConstraint::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	)
	const
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
