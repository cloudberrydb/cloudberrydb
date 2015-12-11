//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerPhysicalCTEConsumer.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing CTE Consumer
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
#include "naucrates/dxl/operators/CDXLPhysicalCTEConsumer.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalCTEConsumer.h"
#include "naucrates/dxl/parser/CParseHandlerProperties.h"
#include "naucrates/dxl/parser/CParseHandlerProjList.h"
#include "naucrates/dxl/parser/CParseHandlerUtils.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalCTEConsumer::CParseHandlerPhysicalCTEConsumer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerPhysicalCTEConsumer::CParseHandlerPhysicalCTEConsumer
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
//		CParseHandlerPhysicalCTEConsumer::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalCTEConsumer::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalCTEConsumer), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

		ULONG ulId = CDXLOperatorFactory::UlValueFromAttrs
											(
											m_pphm->Pmm(),
											attrs,
											EdxltokenCTEId,
											EdxltokenPhysicalCTEConsumer
											);

	DrgPul *pdrgpulColIds = CDXLOperatorFactory::PdrgpulFromAttrs(m_pphm->Pmm(), attrs, EdxltokenColumns, EdxltokenPhysicalCTEConsumer);

	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLPhysicalCTEConsumer(m_pmp, ulId, pdrgpulColIds));

	// parse handler for the proj list
	CParseHandlerBase *pphPrL = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarProjList), m_pphm, this);
	m_pphm->ActivateParseHandler(pphPrL);

	//parse handler for the properties of the operator
	CParseHandlerBase *pphProp = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenProperties), m_pphm, this);
	m_pphm->ActivateParseHandler(pphProp);

	// store parse handlers
	this->Append(pphProp);
	this->Append(pphPrL);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalCTEConsumer::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalCTEConsumer::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalCTEConsumer), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(NULL != m_pdxln);

	CParseHandlerProperties *pphProp = dynamic_cast<CParseHandlerProperties *>((*this)[0]);
	CParseHandlerProjList *pphPrL = dynamic_cast<CParseHandlerProjList*>((*this)[1]);

	// set physical properties
	CParseHandlerUtils::SetProperties(m_pdxln, pphProp);

	// add children
	AddChildFromParseHandler(pphPrL);

#ifdef GPOS_DEBUG
		m_pdxln->Pdxlop()->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG

	m_pphm->DeactivateHandler();
}

// EOF
