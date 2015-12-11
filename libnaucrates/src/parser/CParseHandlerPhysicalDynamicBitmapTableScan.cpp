//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CParseHandlerPhysicalDynamicBitmapTableScan.cpp
//
//	@doc:
//		SAX parse handler class for parsing dynamic bitmap table scan operator nodes
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerPhysicalDynamicBitmapTableScan.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalDynamicBitmapTableScan::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalDynamicBitmapTableScan::StartElement
	(
	const XMLCh* const,  // xmlszUri
 	const XMLCh* const xmlszLocalname,
	const XMLCh* const,  // xmlszQname
	const Attributes& attrs
	)
{
	StartElementHelper(xmlszLocalname, EdxltokenPhysicalDynamicBitmapTableScan);
	m_ulPartIndexId = CDXLOperatorFactory::UlValueFromAttrs
						(
						m_pphm->Pmm(),
						attrs,
						EdxltokenPartIndexId,
						EdxltokenPhysicalDynamicBitmapTableScan
						);

	m_ulPartIndexIdPrintable = CDXLOperatorFactory::UlValueFromAttrs
						(
						m_pphm->Pmm(),
						attrs,
						EdxltokenPartIndexIdPrintable,
						EdxltokenPhysicalDynamicBitmapTableScan,
						true, //fOptional
						m_ulPartIndexId
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalDynamicBitmapTableScan::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalDynamicBitmapTableScan::EndElement
	(
	const XMLCh* const,  // xmlszUri
	const XMLCh* const xmlszLocalname,
	const XMLCh* const  // xmlszQname
	)
{
	EndElementHelper(xmlszLocalname, EdxltokenPhysicalDynamicBitmapTableScan, m_ulPartIndexId, m_ulPartIndexIdPrintable);
}

// EOF
