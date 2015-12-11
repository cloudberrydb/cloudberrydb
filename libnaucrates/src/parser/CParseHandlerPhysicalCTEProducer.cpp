//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerPhysicalCTEProducer.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing CTE producer
//		operators
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/operators/CDXLPhysicalCTEProducer.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalCTEProducer.h"
#include "naucrates/dxl/parser/CParseHandlerProperties.h"
#include "naucrates/dxl/parser/CParseHandlerUtils.h"
#include "naucrates/dxl/parser/CParseHandlerProjList.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalCTEProducer::CParseHandlerPhysicalCTEProducer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerPhysicalCTEProducer::CParseHandlerPhysicalCTEProducer
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerPhysicalOp(pmp, pphm, pphRoot)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalCTEProducer::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalCTEProducer::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalCTEProducer), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// parse cteid and create cte operator
	ULONG ulId = CDXLOperatorFactory::UlValueFromAttrs
											(
											m_pphm->Pmm(),
											attrs,
											EdxltokenCTEId,
											EdxltokenPhysicalCTEProducer
											);

	DrgPul *pdrgpulColIds = CDXLOperatorFactory::PdrgpulFromAttrs(m_pphm->Pmm(), attrs, EdxltokenColumns, EdxltokenPhysicalCTEProducer);

	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLPhysicalCTEProducer(m_pmp, ulId, pdrgpulColIds));

	// create and activate the parse handler for the child expression node
	CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenPhysical), m_pphm, this);
	m_pphm->ActivateParseHandler(pphChild);

	// parse handler for the proj list
	CParseHandlerBase *pphPrL = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarProjList), m_pphm, this);
	m_pphm->ActivateParseHandler(pphPrL);

	//parse handler for the properties of the operator
	CParseHandlerBase *pphProp = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenProperties), m_pphm, this);
	m_pphm->ActivateParseHandler(pphProp);

	// store parse handler
	this->Append(pphProp);
	this->Append(pphPrL);
	this->Append(pphChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalCTEProducer::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalCTEProducer::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalCTEProducer), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(NULL != m_pdxln);


	CParseHandlerProperties *pphProp = dynamic_cast<CParseHandlerProperties *>((*this)[0]);
	CParseHandlerProjList *pphPrL = dynamic_cast<CParseHandlerProjList*>((*this)[1]);
	CParseHandlerPhysicalOp *pphChild = dynamic_cast<CParseHandlerPhysicalOp *>((*this)[2]);

	// set physical properties
	CParseHandlerUtils::SetProperties(m_pdxln, pphProp);

	// add constructed children
	AddChildFromParseHandler(pphPrL);
	AddChildFromParseHandler(pphChild);

#ifdef GPOS_DEBUG
		m_pdxln->Pdxlop()->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG

	m_pphm->DeactivateHandler();
}

// EOF
