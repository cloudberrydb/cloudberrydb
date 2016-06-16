//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarOp.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing scalar operators.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarOp::CParseHandlerScalarOp
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerScalarOp::CParseHandlerScalarOp
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerOp(pmp, pphm, pphRoot)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarOp::~CParseHandlerScalarOp
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CParseHandlerScalarOp::~CParseHandlerScalarOp()
{
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarOp::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag for a scalar operator.
//		The function serves as a dispatcher for invoking the correct processing
//		function of the actual operator type.
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarOp::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	// instantiate the parse handler
	CParseHandlerBase *pph = CParseHandlerFactory::Pph(m_pmp, xmlszLocalname, m_pphm, this);
	
	GPOS_ASSERT(NULL != pph);
	
	// activate the specialized parse handler
	m_pphm->ReplaceHandler(pph, m_pphRoot);
	
	// pass the startElement message for the specialized parse handler to process
	pph->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarOp::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag.
//		This function should never be called. Instead, the endElement function
//		of the parse handler for the actual physical operator type is called.
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarOp::EndElement
	(
	const XMLCh* const, //= xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname,
	)
{
	CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
	GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
}


// EOF

