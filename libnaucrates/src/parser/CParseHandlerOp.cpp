//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerOp.cpp
//
//	@doc:
//		Implementation of the base SAX parse handler class for parsing DXL operators.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerOp::CParseHandlerOp
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerOp::CParseHandlerOp
	(
	IMemoryPool *pmp, 
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pdxln(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerOp::~CParseHandlerOp
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CParseHandlerOp::~CParseHandlerOp()
{
	CRefCount::SafeRelease(m_pdxln);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerOp::Pdxln
//
//	@doc:
//		Returns the constructed DXL node and passes ownership over it.
//
//---------------------------------------------------------------------------
CDXLNode *
CParseHandlerOp::Pdxln() const
{
	return m_pdxln;
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerOp::AddChildFromParseHandler
//
//	@doc:
//		Extracts the node constructed from the given parse handler and adds it
//		to children array of the current node. Child nodes are ref-counted before
//		being added to the array.
//
//---------------------------------------------------------------------------
void
CParseHandlerOp::AddChildFromParseHandler
	(
	const CParseHandlerOp *pph
	)
{
	GPOS_ASSERT(NULL != m_pdxln);
	GPOS_ASSERT(NULL != pph);
	
	// extract constructed element
	CDXLNode *pdxlnChild = pph->Pdxln();
	GPOS_ASSERT(NULL != pdxlnChild);
	
	pdxlnChild->AddRef();
	m_pdxln->AddChild(pdxlnChild);
}


// EOF

