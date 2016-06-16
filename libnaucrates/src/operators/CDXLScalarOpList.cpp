//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLScalarOpList.cpp
//
//	@doc:
//		Implementation of DXL list of scalar expressions
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarOpList.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarOpList::CDXLScalarOpList
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarOpList::CDXLScalarOpList
	(
	IMemoryPool *pmp,
	EdxlOpListType edxloplisttype
	)
	:
	CDXLScalar(pmp),
	m_edxloplisttype(edxloplisttype)
{
	GPOS_ASSERT(EdxloplistSentinel > edxloplisttype);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarOpList::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarOpList::Edxlop() const
{
	return EdxlopScalarOpList;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarOpList::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarOpList::PstrOpName() const
{
	Edxltoken edxlt = EdxltokenSentinel;
	switch (m_edxloplisttype)
	{
		case EdxloplistEqFilterList:
			edxlt = EdxltokenPartLevelEqFilterList;
			break;

		case EdxloplistFilterList:
			edxlt = EdxltokenPartLevelFilterList;
			break;

		case EdxloplistGeneral:
			edxlt = EdxltokenScalarOpList;
			break;

		default:
			GPOS_ASSERT(!"Invalid op list type");
			break;
	}

	return CDXLTokens::PstrToken(edxlt);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarOpList::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarOpList::SerializeToDXL
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
//		CDXLScalarOpList::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarOpList::AssertValid
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
