//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerFilter.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing filter operators.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerFilter.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLScalarFilter.h"
#include "naucrates/dxl/operators/CDXLScalarOneTimeFilter.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFilter::CParseHandlerFilter
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerFilter::CParseHandlerFilter
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerScalarOp(pmp, pphm, pphRoot)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFilter::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerFilter::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{	
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarFilter), xmlszLocalname))
	{
		// start the filter
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode (m_pmp, GPOS_NEW(m_pmp) CDXLScalarFilter(m_pmp));
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarJoinFilter), xmlszLocalname))
	{
		// start the filter
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode (m_pmp, GPOS_NEW(m_pmp) CDXLScalarJoinFilter(m_pmp));
	} 
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarOneTimeFilter), xmlszLocalname))
	{
		// start the filter
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode (m_pmp, GPOS_NEW(m_pmp) CDXLScalarOneTimeFilter(m_pmp));
	}
	else if (0 == XMLString::compareString(
			CDXLTokens::XmlstrToken(EdxltokenScalarRecheckCondFilter), xmlszLocalname))
	{
		// start the filter
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode (m_pmp, GPOS_NEW(m_pmp) CDXLScalarRecheckCondFilter(m_pmp));
	}
	else
	{
		GPOS_ASSERT(NULL != m_pdxln);
		
		// install a scalar element parser for parsing the condition element
		CParseHandlerBase *pphOp = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);

		m_pphm->ActivateParseHandler(pphOp);
		
		// store parse handler
		this->Append(pphOp);
		
		pphOp->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
		
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFilter::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerFilter::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarFilter), xmlszLocalname) &&
		0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarJoinFilter), xmlszLocalname) &&
		0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarOneTimeFilter), xmlszLocalname) &&
		0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarRecheckCondFilter), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	if (0 < this->UlLength())
	{
		// filter node was not empty 
		CParseHandlerScalarOp *pphOp = dynamic_cast<CParseHandlerScalarOp *>((*this)[0]);
		
		AddChildFromParseHandler(pphOp);	
	}
	
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
