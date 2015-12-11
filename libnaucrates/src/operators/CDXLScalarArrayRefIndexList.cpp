//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLScalarArrayRefIndexList.cpp
//
//	@doc:
//		Implementation of DXL arrayref index list
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarArrayRefIndexList.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRefIndexList::CDXLScalarArrayRefIndexList
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarArrayRefIndexList::CDXLScalarArrayRefIndexList
	(
	IMemoryPool *pmp,
	EIndexListBound eilb
	)
	:
	CDXLScalar(pmp),
	m_eilb(eilb)
{
	GPOS_ASSERT(EilbSentinel > eilb);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRefIndexList::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarArrayRefIndexList::Edxlop() const
{
	return EdxlopScalarArrayRefIndexList;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRefIndexList::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarArrayRefIndexList::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarArrayRefIndexList);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRefIndexList::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarArrayRefIndexList::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenScalarArrayRefIndexListBound), PstrIndexListBound(m_eilb));

	pdxln->SerializeChildrenToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRefIndexList::PstrIndexListBound
//
//	@doc:
//		String representation of index list bound
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarArrayRefIndexList::PstrIndexListBound
	(
	EIndexListBound eilb
	)
{
	switch (eilb)
	{
		case EilbLower:
			return CDXLTokens::PstrToken(EdxltokenScalarArrayRefIndexListLower);

		case EilbUpper:
			return CDXLTokens::PstrToken(EdxltokenScalarArrayRefIndexListUpper);

		default:
			GPOS_ASSERT("Invalid array bound");
			return NULL;
	}
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRefIndexList::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarArrayRefIndexList::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	)
	const
{
	const ULONG ulArity = pdxln->UlArity();
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
