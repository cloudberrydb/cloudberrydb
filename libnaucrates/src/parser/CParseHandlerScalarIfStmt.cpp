//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarIfStmt.cpp
//
//	@doc:
//		
//		Implementation of the SAX parse handler class for an if statement.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------


#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerScalarIfStmt.h"


using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarIfStmt::CParseHandlerScalarIfStmt
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerScalarIfStmt::CParseHandlerScalarIfStmt
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
//		CParseHandlerScalarIfStmt::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarIfStmt::StartElement
	(
	const XMLCh* const,// xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const,// xmlszQname,
	const Attributes& attrs
	)
{
	if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarIfStmt), xmlszLocalname))
	{
		// parse and create scalar if statment
		CDXLScalarIfStmt *pdxlop = (CDXLScalarIfStmt*) CDXLOperatorFactory::PdxlopIfStmt(m_pphm->Pmm(), attrs);

		// construct node
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);

		// create and activate the parse handler for the children nodes in reverse
		// order of their expected appearance

		// parse handler for handling else result expression scalar node
		CParseHandlerBase *pphElse = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
		m_pphm->ActivateParseHandler(pphElse);

		// parse handler for handling result expression scalar node
		CParseHandlerBase *pphResult = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
		m_pphm->ActivateParseHandler(pphResult);

		// parse handler for the when condition clause
		CParseHandlerBase *pphWhenCond = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
		m_pphm->ActivateParseHandler(pphWhenCond);

		// store parse handlers
		this->Append(pphWhenCond);
		this->Append(pphResult);
		this->Append(pphElse);

	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarIfStmt::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarIfStmt::EndElement
	(
	const XMLCh* const ,// xmlszUri
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarIfStmt), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	CParseHandlerScalarOp *pphWhenCond = dynamic_cast<CParseHandlerScalarOp *>((*this)[0]);
	CParseHandlerScalarOp *pphResult = dynamic_cast<CParseHandlerScalarOp *>((*this)[1]);
	CParseHandlerScalarOp *pphElse = dynamic_cast<CParseHandlerScalarOp *>((*this)[2]);

	// add constructed children
	AddChildFromParseHandler(pphWhenCond);
	AddChildFromParseHandler(pphResult);
	AddChildFromParseHandler(pphElse);

	// deactivate handler
	m_pphm->DeactivateHandler();
}


// EOF
