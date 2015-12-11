//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerBase.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing the list of
//		column descriptors in a table descriptor node.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerBase.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/xml/CDXLMemoryManager.h"


using namespace gpdxl;
using namespace gpos;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerBase::CParseHandlerBase
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerBase::CParseHandlerBase
	(
	IMemoryPool *pmp, 
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	m_pmp(pmp),
	m_pphm(pphm),
	m_pphRoot(pphRoot)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pphm);
	
	m_pdrgpph = GPOS_NEW(m_pmp) DrgPph(m_pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerBase::~CParseHandlerBase
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------

CParseHandlerBase::~CParseHandlerBase()
{
	m_pdrgpph->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerBase::Edxlphtype
//
//	@doc:
//		Return the type of the parse handler. Currently we overload this method to 
//		return a specific type for the plan, query, metadata and traceflags parse handlers.
//
//---------------------------------------------------------------------------
EDxlParseHandlerType
CParseHandlerBase::Edxlphtype() const
{
	return EdxlphOther;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerBase::ReplaceParseHandler
//
//	@doc:
//		Replaces a parse handler in the parse handler array with a new one
//
//---------------------------------------------------------------------------
void
CParseHandlerBase::ReplaceParseHandler
	(
	CParseHandlerBase *pphOld,
	CParseHandlerBase *pphNew
	)
{
	ULONG ulPos = 0;
	
	GPOS_ASSERT(NULL != m_pdrgpph);
	
	for (ulPos = 0; ulPos < m_pdrgpph->UlLength(); ulPos++)
	{
		if ((*m_pdrgpph)[ulPos] == pphOld)
		{
			break;
		}
	}
	
	// assert old parse handler was found in array
	GPOS_ASSERT(ulPos < m_pdrgpph->UlLength());
	
	m_pdrgpph->Replace(ulPos, pphNew);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerBase::startElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerBase::startElement
	(
		const XMLCh* const xmlszUri,
		const XMLCh* const xmlszLocalname,
		const XMLCh* const xmlszQname,
		const Attributes& attrs
	)
{
	StartElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerBase::endElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerBase::endElement
	(
		const XMLCh* const xmlszUri,
		const XMLCh* const xmlszLocalname,
		const XMLCh* const xmlszQname
	)
{
	EndElement(xmlszUri, xmlszLocalname, xmlszQname);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerBase::error
//
//	@doc:
//		Invoked by Xerces to process an error
//
//---------------------------------------------------------------------------
void
CParseHandlerBase::error
	(
	const SAXParseException& toCatch
	)
{
	CHAR* szMessage = XMLString::transcode(toCatch.getMessage(), m_pphm->Pmm());
	GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLValidationError, szMessage);
}

// EOF

