//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarArrayComp.cpp
//
//	@doc:
//		
//		Implementation of the SAX parse handler class for parsing scalar array comparison.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerScalarArrayComp.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarArrayComp::CParseHandlerScalarArrayComp
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerScalarArrayComp::CParseHandlerScalarArrayComp
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
//		CParseHandlerScalarArrayComp::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarArrayComp::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarArrayComp), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// parse and create scalar ArrayComp
	CDXLScalarArrayComp *pdxlop = (CDXLScalarArrayComp*) CDXLOperatorFactory::PdxlopArrayComp(m_pphm->Pmm(), attrs);

	// construct node from the created child nodes
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);

	// create and activate the parse handler for the children nodes in reverse
	// order of their expected appearance

	// parse handler for right scalar node
	CParseHandlerBase *pphRight = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
	m_pphm->ActivateParseHandler(pphRight);

	// parse handler for left scalar node
	CParseHandlerBase *pphLeft = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
	m_pphm->ActivateParseHandler(pphLeft);

	// store parse handlers
	this->Append(pphLeft);
	this->Append(pphRight);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarArrayComp::EndElement
//
//	@doc:
//		Processes a Xerces end element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarArrayComp::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarArrayComp), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	CParseHandlerScalarOp *pphLeft = dynamic_cast<CParseHandlerScalarOp *>((*this)[0]);
	CParseHandlerScalarOp *pphRight = dynamic_cast<CParseHandlerScalarOp *>((*this)[1]);

	// add constructed children
	AddChildFromParseHandler(pphLeft);
	AddChildFromParseHandler(pphRight);

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
