//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CParseHandlerExternalScan.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing external scan
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerExternalScan.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerExternalScan::CParseHandlerExternalScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerExternalScan::CParseHandlerExternalScan
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerTableScan(pmp, pphm, pphRoot)
{}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerExternalScan::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerExternalScan::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& // attrs
	)
{
	CParseHandlerTableScan::StartElement(xmlszLocalname, EdxltokenPhysicalExternalScan);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerExternalScan::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerExternalScan::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	CParseHandlerTableScan::EndElement(xmlszLocalname, EdxltokenPhysicalExternalScan);
}

// EOF

