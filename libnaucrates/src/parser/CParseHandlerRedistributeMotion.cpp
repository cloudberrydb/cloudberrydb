//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerRedistributeMotion.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing gather motion operators.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerRedistributeMotion.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFilter.h"
#include "naucrates/dxl/parser/CParseHandlerHashExprList.h"
#include "naucrates/dxl/parser/CParseHandlerProjList.h"
#include "naucrates/dxl/parser/CParseHandlerProperties.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerSortColList.h"
#include "naucrates/dxl/parser/CParseHandlerUtils.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerRedistributeMotion::CParseHandlerRedistributeMotion
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerRedistributeMotion::CParseHandlerRedistributeMotion
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
//		CParseHandlerRedistributeMotion::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerRedistributeMotion::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{	
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalRedistributeMotion), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// parse and create Redistribute motion operator
	m_pdxlop = (CDXLPhysicalRedistributeMotion *) CDXLOperatorFactory::PdxlopRedistributeMotion(m_pphm->Pmm(), attrs);
	
	// create and activate the parse handler for the children nodes in reverse
	// order of their expected appearance
	
	// parse handler for child node
	CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenPhysical), m_pphm, this);
	m_pphm->ActivateParseHandler(pphChild);
	
	// parse handler for hash expr list
	CParseHandlerBase *pphHExprList = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarHashExprList), m_pphm, this);
	m_pphm->ActivateParseHandler(pphHExprList);
	
	// parse handler for the sorting column list
	CParseHandlerBase *pphSortColList = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarSortColList), m_pphm, this);
	m_pphm->ActivateParseHandler(pphSortColList);

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
	this->Append(pphSortColList);
	this->Append(pphHExprList);
	this->Append(pphChild);

}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerRedistributeMotion::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerRedistributeMotion::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalRedistributeMotion), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// construct node from the created child nodes
	
	CParseHandlerProperties *pphProp = dynamic_cast<CParseHandlerProperties *>((*this)[0]);
	CParseHandlerProjList *pphPrL = dynamic_cast<CParseHandlerProjList*>((*this)[1]);
	CParseHandlerFilter *pphFilter = dynamic_cast<CParseHandlerFilter *>((*this)[2]);
	CParseHandlerSortColList *pphSortColList = dynamic_cast<CParseHandlerSortColList *>((*this)[3]);
	CParseHandlerHashExprList *pphHExprList = dynamic_cast<CParseHandlerHashExprList *>((*this)[4]);
	CParseHandlerPhysicalOp *pphChild = dynamic_cast<CParseHandlerPhysicalOp *>((*this)[5]);

	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, m_pdxlop);
	// set statictics and physical properties
	CParseHandlerUtils::SetProperties(m_pdxln, pphProp);
	
	// add children
	AddChildFromParseHandler(pphPrL);
	AddChildFromParseHandler(pphFilter);
	AddChildFromParseHandler(pphSortColList);
	AddChildFromParseHandler(pphHExprList);
	AddChildFromParseHandler(pphChild);
	
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
