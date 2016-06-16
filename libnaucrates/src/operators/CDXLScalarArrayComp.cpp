//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarArrayComp.cpp
//
//	@doc:
//		Implementation of DXL scalar array comparison
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarArrayComp.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayComp::CDXLScalarArrayComp
//
//	@doc:
//		Constructs a ScalarArrayComp node
//
//---------------------------------------------------------------------------
CDXLScalarArrayComp::CDXLScalarArrayComp
	(
	IMemoryPool *pmp,
	IMDId *pmdidOp,
	const CWStringConst *pstrOpName,
	EdxlArrayCompType edxlcomptype
	)
	:
	CDXLScalarComp(pmp, pmdidOp, pstrOpName),
	m_edxlcomptype(edxlcomptype)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayComp::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarArrayComp::Edxlop() const
{
	return EdxlopScalarArrayComp;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayComp::Edxlarraycomptype
//
//	@doc:
//	 	Returns the array comparison operation type (ALL/ANY)
//
//---------------------------------------------------------------------------
EdxlArrayCompType
CDXLScalarArrayComp::Edxlarraycomptype() const
{
	return m_edxlcomptype;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayComp::PstrArrayCompType
//
//	@doc:
//		AggRef AggStage
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarArrayComp::PstrArrayCompType() const
{
	switch (m_edxlcomptype)
	{
		case Edxlarraycomptypeany:
			return CDXLTokens::PstrToken(EdxltokenOpTypeAny);
		case Edxlarraycomptypeall:
			return CDXLTokens::PstrToken(EdxltokenOpTypeAll);
		default:
			GPOS_ASSERT(!"Unrecognized array operation type");
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayComp::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarArrayComp::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarArrayComp);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayComp::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarArrayComp::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenOpName), m_pstrCompOpName);
	m_pmdid->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenOpNo));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenOpType), PstrArrayCompType());

	pdxln->SerializeChildrenToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayComp::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarArrayComp::AssertValid
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
