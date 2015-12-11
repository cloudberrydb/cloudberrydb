//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerNLJoin.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing nested loop join operators.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerNLJoin.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFilter.h"
#include "naucrates/dxl/parser/CParseHandlerProjList.h"
#include "naucrates/dxl/parser/CParseHandlerProperties.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerUtils.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerNLJoin::CParseHandlerNLJoin
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerNLJoin::CParseHandlerNLJoin
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerPhysicalOp(pmp, pphm, pphRoot),
	m_pdxlop(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerNLJoin::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerNLJoin::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalNLJoin), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// parse and create Hash join operator
	m_pdxlop = (CDXLPhysicalNLJoin *) CDXLOperatorFactory::PdxlopNLJoin(m_pphm->Pmm(), attrs);
	
	// create and activate the parse handler for the children nodes in reverse
	// order of their expected appearance
	
	// parse handler for right child
	CParseHandlerBase *pphRight = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenPhysical), m_pphm, this);
	m_pphm->ActivateParseHandler(pphRight);
	
	// parse handler for left child
	CParseHandlerBase *pphLeft = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenPhysical), m_pphm, this);
	m_pphm->ActivateParseHandler(pphLeft);

	// parse handler for the join filter
	CParseHandlerBase *pphJoinFilter = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarFilter), m_pphm, this);
	m_pphm->ActivateParseHandler(pphJoinFilter);

	// parse handler for the filter
	CParseHandlerBase *pphFilter = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarFilter), m_pphm, this);
	m_pphm->ActivateParseHandler(pphFilter);
	
	// parse handler for the proj list
	CParseHandlerBase *pphPrL = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarProjList), m_pphm, this);
	m_pphm->ActivateParseHandler(pphPrL);
	
	//parse handler for the properties of the operator
	CParseHandlerBase *pphProp = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenProperties), m_pphm, this);
	m_pphm->ActivateParseHandler(pphProp);
	
	// store parse handlers
	this->Append(pphProp);
	this->Append(pphPrL);
	this->Append(pphFilter);
	this->Append(pphJoinFilter);
	this->Append(pphLeft);
	this->Append(pphRight);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerNLJoin::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerNLJoin::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalNLJoin), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLUnexpectedTag,
			pstr->Wsz()
			);
	}
	
	// construct node from the created child nodes
	CParseHandlerProperties *pphProp = dynamic_cast<CParseHandlerProperties *>((*this)[0]);
	CParseHandlerProjList *pphPrL = dynamic_cast<CParseHandlerProjList*>((*this)[1]);
	CParseHandlerFilter *pphFilter = dynamic_cast<CParseHandlerFilter *>((*this)[2]);
	CParseHandlerFilter *pphJoinFilter = dynamic_cast<CParseHandlerFilter *>((*this)[3]);
	CParseHandlerPhysicalOp *pphLeft = dynamic_cast<CParseHandlerPhysicalOp *>((*this)[4]);
	CParseHandlerPhysicalOp *pphRight = dynamic_cast<CParseHandlerPhysicalOp *>((*this)[5]);

	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, m_pdxlop);
	// set statictics and physical properties
	CParseHandlerUtils::SetProperties(m_pdxln, pphProp);
	
	// add constructed children
	AddChildFromParseHandler(pphPrL);
	AddChildFromParseHandler(pphFilter);
	AddChildFromParseHandler(pphJoinFilter);
	AddChildFromParseHandler(pphLeft);
	AddChildFromParseHandler(pphRight);
	
#ifdef GPOS_DEBUG
	m_pdxlop->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
