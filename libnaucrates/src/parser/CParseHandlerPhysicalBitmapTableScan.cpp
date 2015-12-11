//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CParseHandlerPhysicalBitmapTableScan.cpp
//
//	@doc:
//		SAX parse handler class for parsing bitmap table scan operator nodes
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerPhysicalBitmapTableScan.h"

#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalBitmapTableScan::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalBitmapTableScan::StartElement
	(
	const XMLCh* const,  // xmlszUri
 	const XMLCh* const xmlszLocalname,
	const XMLCh* const,  // xmlszQname
	const Attributes&  // attrs
	)
{
	StartElementHelper(xmlszLocalname, EdxltokenPhysicalBitmapTableScan);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalBitmapTableScan::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalBitmapTableScan::EndElement
	(
	const XMLCh* const,  // xmlszUri
	const XMLCh* const xmlszLocalname,
	const XMLCh* const  // xmlszQname
	)
{
	EndElementHelper(xmlszLocalname, EdxltokenPhysicalBitmapTableScan);
}

