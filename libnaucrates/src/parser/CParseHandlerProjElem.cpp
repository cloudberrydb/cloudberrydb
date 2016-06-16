//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerProjElem.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing proj elem operators.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerProjElem.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerProjElem::CParseHandlerProjElem
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerProjElem::CParseHandlerProjElem
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
//		CParseHandlerProjElem::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerProjElem::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarProjElem), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// parse and create proj elem operator
	m_pdxlop = (CDXLScalarProjElem *) CDXLOperatorFactory::PdxlopProjElem(m_pphm->Pmm(), attrs);
	
	// create and activate the parse handler for the child scalar expression node
	
	CParseHandlerBase *pph = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
	m_pphm->ActivateParseHandler(pph);
	
	// store parse handler
	this->Append(pph);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerProjElem::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerProjElem::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarProjElem), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// construct node from the parsed expression node
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, m_pdxlop);	
		
	CParseHandlerScalarOp *pph = dynamic_cast<CParseHandlerScalarOp*>((*this)[0]);
	
	// store constructed child
	AddChildFromParseHandler(pph);
	
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
