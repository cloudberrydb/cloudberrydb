//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CParseHandlerScalarSubqueryExists.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing EXISTS and NOT EXISTS 
//		 subquery operators.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarSubqueryExists.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerLogicalOp.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSubqueryExists::CParseHandlerScalarSubqueryExists
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerScalarSubqueryExists::CParseHandlerScalarSubqueryExists
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerScalarOp(pmp, pphm, pphRoot),
	m_pdxlop(NULL)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSubqueryExists::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarSubqueryExists::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& // attrs
	)
{
	
	GPOS_ASSERT(NULL == m_pdxlop);
		
	if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubqueryExists), xmlszLocalname))
	{
		m_pdxlop = GPOS_NEW(m_pmp) CDXLScalarSubqueryExists(m_pmp);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubqueryNotExists), xmlszLocalname))
	{
		m_pdxlop = GPOS_NEW(m_pmp) CDXLScalarSubqueryNotExists(m_pmp);	
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// parse handler for the child node
	CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenLogical), m_pphm, this);
	m_pphm->ActivateParseHandler(pphChild);

	// store child parse handler in array
	this->Append(pphChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSubqueryExists::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarSubqueryExists::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubqueryExists), xmlszLocalname) &&
		0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubqueryNotExists), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// construct node from parsed components
	GPOS_ASSERT(NULL != m_pdxlop);
	GPOS_ASSERT(1 == this->UlLength());
	
	CParseHandlerLogicalOp *pphChild = dynamic_cast<CParseHandlerLogicalOp *>((*this)[0]);
		
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, m_pdxlop);

	// add constructed child
	AddChildFromParseHandler(pphChild);

#ifdef GPOS_DEBUG
	m_pdxlop->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG
	
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF

