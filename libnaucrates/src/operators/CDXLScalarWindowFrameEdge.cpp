//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarWindowFrameEdge.cpp
//
//	@doc:
//		Implementation of DXL scalar window frame edge
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarWindowFrameEdge.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarWindowFrameEdge::CDXLScalarWindowFrameEdge
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarWindowFrameEdge::CDXLScalarWindowFrameEdge
	(
	IMemoryPool *pmp,
	BOOL fLeading,
	EdxlFrameBoundary edxlfb
	)
	:
	CDXLScalar(pmp),
	m_fLeading(fLeading),
	m_edxlfb(edxlfb)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarWindowFrameEdge::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarWindowFrameEdge::Edxlop() const
{
	return EdxlopScalarWindowFrameEdge;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarWindowFrameEdge::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarWindowFrameEdge::PstrOpName() const
{
	if (m_fLeading)
	{
		return CDXLTokens::PstrToken(EdxltokenScalarWindowFrameLeadingEdge);
	}

	return CDXLTokens::PstrToken(EdxltokenScalarWindowFrameTrailingEdge);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarWindowFrameEdge::PstrFrameBoundary
//
//	@doc:
//		Return the string representation of the window frame boundary
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarWindowFrameEdge::PstrFrameBoundary
	(
	EdxlFrameBoundary edxlfb
	)
	const
{
	GPOS_ASSERT(EdxlfbSentinel > edxlfb);

	ULONG rgrgulMapping[][2] =
					{
					{EdxlfbUnboundedPreceding, EdxltokenWindowBoundaryUnboundedPreceding},
					{EdxlfbBoundedPreceding, EdxltokenWindowBoundaryBoundedPreceding},
					{EdxlfbCurrentRow, EdxltokenWindowBoundaryCurrentRow},
					{EdxlfbUnboundedFollowing, EdxltokenWindowBoundaryUnboundedFollowing},
					{EdxlfbBoundedFollowing, EdxltokenWindowBoundaryBoundedFollowing},
					{EdxlfbDelayedBoundedPreceding, EdxltokenWindowBoundaryDelayedBoundedPreceding},
					{EdxlfbDelayedBoundedFollowing, EdxltokenWindowBoundaryDelayedBoundedFollowing}
					};

	const ULONG ulArity = GPOS_ARRAY_SIZE(rgrgulMapping);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		ULONG *pulElem = rgrgulMapping[ul];
		if ((ULONG) edxlfb == pulElem[0])
		{
			Edxltoken edxltk = (Edxltoken) pulElem[1];
			return CDXLTokens::PstrToken(edxltk);
		}
	}

	GPOS_ASSERT(!"Unrecognized window frame boundary");
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarWindowFrameEdge::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarWindowFrameEdge::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{

	const CWStringConst *pstrElemName = PstrOpName();
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	if (m_fLeading)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenWindowLeadingBoundary), PstrFrameBoundary(m_edxlfb));
	}
	else
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenWindowTrailingBoundary), PstrFrameBoundary(m_edxlfb));
	}

	pdxln->SerializeChildrenToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarWindowFrameEdge::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarWindowFrameEdge::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	)
	const
{
	const ULONG ulArity = pdxln->UlArity();
	GPOS_ASSERT(1 >= ulArity);

	GPOS_ASSERT_IMP((m_edxlfb == EdxlfbBoundedPreceding || m_edxlfb == EdxlfbBoundedFollowing
					|| m_edxlfb == EdxlfbDelayedBoundedPreceding || m_edxlfb == EdxlfbDelayedBoundedFollowing), 1 == ulArity);
	GPOS_ASSERT_IMP((m_edxlfb == EdxlfbUnboundedPreceding || m_edxlfb == EdxlfbUnboundedFollowing || m_edxlfb == EdxlfbCurrentRow), 0 == ulArity);

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
