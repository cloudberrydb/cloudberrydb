//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerStackTrace.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for stack traces.
//		This is a pass-through parse handler, since we do not do anything with
//		the stack traces
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerStacktrace.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStacktrace::CParseHandlerStacktrace
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerStacktrace::CParseHandlerStacktrace
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStacktrace::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerStacktrace::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const, // xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes&  // attrs
	)
{
	// passthrough
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStacktrace::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerStacktrace::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const, // xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
