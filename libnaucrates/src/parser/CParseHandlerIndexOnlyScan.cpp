//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerIndexOnlyScan.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for the index only scan operator
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerIndexOnlyScan.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerIndexOnlyScan::CParseHandlerIndexOnlyScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerIndexOnlyScan::CParseHandlerIndexOnlyScan
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerIndexScan(pmp, pphm, pphRoot)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerIndexOnlyScan::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerIndexOnlyScan::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	StartElementHelper(xmlszLocalname, attrs, EdxltokenPhysicalIndexOnlyScan);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerIndexOnlyScan::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerIndexOnlyScan::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	EndElementHelper(xmlszLocalname, EdxltokenPhysicalIndexOnlyScan);
}

// EOF
