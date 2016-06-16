//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerProjList.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing projection lists.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerProjList.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerProjElem.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/operators/CDXLScalarProjList.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerProjList::CParseHandlerProjList
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerProjList::CParseHandlerProjList
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
//		CParseHandlerProjList::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerProjList::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarProjList), xmlszLocalname))
	{
		// start the proj list
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode (m_pmp, GPOS_NEW(m_pmp) CDXLScalarProjList(m_pmp));
	}
	else if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarProjElem), xmlszLocalname))
	{
		// we must have seen a proj list already and initialized the proj list node
		GPOS_ASSERT(NULL != m_pdxln);

		// start new project element
		CParseHandlerBase *pphPrEl = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarProjElem), m_pphm, this);
		m_pphm->ActivateParseHandler(pphPrEl);
		
		// store parse handler
		this->Append(pphPrEl);
		
		pphPrEl->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerProjList::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerProjList::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarProjList), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	const ULONG ulSize = this->UlLength();
	// add constructed children from child parse handlers
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CParseHandlerProjElem *pphPrEl = dynamic_cast<CParseHandlerProjElem*>((*this)[ul]);
		AddChildFromParseHandler(pphPrEl);
	}		
		
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
