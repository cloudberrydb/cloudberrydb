//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLWindowFrame.cpp
//
//	@doc:
//		Implementation of DXL Window Frame
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLWindowFrame.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "naucrates/md/IMDAggregate.h"

using namespace gpopt;
using namespace gpmd;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowFrame::CDXLWindowFrame
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLWindowFrame::CDXLWindowFrame
	(
	IMemoryPool *pmp,
	EdxlFrameSpec edxlfs,
	EdxlFrameExclusionStrategy edxlfes,
	CDXLNode *pdxlnLeading,
	CDXLNode *pdxlnTrailing
	)
	:
	m_pmp(pmp),
	m_edxlfs(edxlfs),
	m_edxlfes(edxlfes),
	m_pdxlnLeading(pdxlnLeading),
	m_pdxlnTrailing(pdxlnTrailing)
{
	GPOS_ASSERT(NULL != m_pmp);
	GPOS_ASSERT(EdxlfsSentinel > m_edxlfs);
	GPOS_ASSERT(EdxlfesSentinel > m_edxlfes);
	GPOS_ASSERT(NULL != pdxlnLeading);
	GPOS_ASSERT(NULL != pdxlnTrailing);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowFrame::~CDXLWindowFrame
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLWindowFrame::~CDXLWindowFrame()
{
	m_pdxlnLeading->Release();
	m_pdxlnTrailing->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowFrame::PstrES
//
//	@doc:
//		Return the string representation of the window frame exclusion strategy
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLWindowFrame::PstrES
	(
	EdxlFrameExclusionStrategy edxles
	)
	const
{
	GPOS_ASSERT(EdxlfesSentinel > edxles);
	ULONG rgrgulMapping[][2] =
					{
					{EdxlfesNone, EdxltokenWindowESNone},
					{EdxlfesNulls, EdxltokenWindowESNulls},
					{EdxlfesCurrentRow, EdxltokenWindowESCurrentRow},
					{EdxlfesGroup, EdxltokenWindowESGroup},
					{EdxlfesTies, EdxltokenWindowESTies}
					};

	const ULONG ulArity = GPOS_ARRAY_SIZE(rgrgulMapping);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		ULONG *pulElem = rgrgulMapping[ul];
		if ((ULONG) edxles == pulElem[0])
		{
			Edxltoken edxltk = (Edxltoken) pulElem[1];
			return CDXLTokens::PstrToken(edxltk);
			break;
		}
	}

	GPOS_ASSERT(!"Unrecognized window frame exclusion strategy");
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowFrame::PstrFS
//
//	@doc:
//		Return the string representation of the window frame specification
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLWindowFrame::PstrFS
	(
	EdxlFrameSpec edxlfs
	)
	const
{
	GPOS_ASSERT(EdxlfsSentinel > edxlfs && "Unrecognized window frame specification");

	if (EdxlfsRow == edxlfs)
	{
		return CDXLTokens::PstrToken(EdxltokenWindowFSRow);
	}

	return CDXLTokens::PstrToken(EdxltokenWindowFSRange);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowFrame::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLWindowFrame::SerializeToDXL
	(
	CXMLSerializer *pxmlser
	)
	const
{
	const CWStringConst *pstrElemName = CDXLTokens::PstrToken(EdxltokenWindowFrame);
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	// add attributes
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenWindowFrameSpec), PstrFS(m_edxlfs));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenWindowExclusionStrategy), PstrES(m_edxlfes));

	// add the values representing the window boundary
	m_pdxlnTrailing->SerializeToDXL(pxmlser);
	m_pdxlnLeading->SerializeToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

// EOF
