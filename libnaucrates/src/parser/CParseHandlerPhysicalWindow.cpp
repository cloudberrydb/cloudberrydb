//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerPhysicalWindow.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing
//		a physical window operators
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerPhysicalWindow.h"
#include "naucrates/dxl/parser/CParseHandlerWindowKeyList.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFilter.h"
#include "naucrates/dxl/parser/CParseHandlerProjList.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"
#include "naucrates/dxl/parser/CParseHandlerProperties.h"
#include "naucrates/dxl/parser/CParseHandlerUtils.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalWindow::CParseHandlerPhysicalWindow
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerPhysicalWindow::CParseHandlerPhysicalWindow
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerPhysicalOp(pmp, pphm, pphRoot),
	m_pdrgpulPartCols(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalWindow::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalWindow::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalWindow), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	const XMLCh *xmlszPartCols= CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenPartKeys, EdxltokenPhysicalWindow);
	m_pdrgpulPartCols = CDXLOperatorFactory::PdrgpulFromXMLCh(m_pphm->Pmm(), xmlszPartCols, EdxltokenPartKeys, EdxltokenPhysicalWindow);
	GPOS_ASSERT(NULL != m_pdrgpulPartCols);

	// parse handler for window key list
	CParseHandlerBase *pphWkL = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenWindowKeyList), m_pphm, this);
	m_pphm->ActivateParseHandler(pphWkL);

	// parse handler for child node
	CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenPhysical), m_pphm, this);
	m_pphm->ActivateParseHandler(pphChild);

	// parse handler for the filter
	CParseHandlerBase *pphFilter = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarFilter), m_pphm, this);
	m_pphm->ActivateParseHandler(pphFilter);

	// parse handler for the proj list
	CParseHandlerBase *pphPrL = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarProjList), m_pphm, this);
	m_pphm->ActivateParseHandler(pphPrL);

	//parse handler for the properties of the operator
	CParseHandlerBase *pphProp = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenProperties), m_pphm, this);
	m_pphm->ActivateParseHandler(pphProp);

	// store child parse handlers in array
	this->Append(pphProp);
	this->Append(pphPrL);
	this->Append(pphFilter);
	this->Append(pphChild);
	this->Append(pphWkL);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalWindow::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalWindow::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalWindow), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(5 == this->UlLength());
	CParseHandlerProperties *pphProp = dynamic_cast<CParseHandlerProperties *>((*this)[0]);
	CParseHandlerProjList *pphPrL = dynamic_cast<CParseHandlerProjList*>((*this)[1]);
	CParseHandlerFilter *pphF = dynamic_cast<CParseHandlerFilter *>((*this)[2]);
	CParseHandlerPhysicalOp *pphChild = dynamic_cast<CParseHandlerPhysicalOp *>((*this)[3]);

	CParseHandlerWindowKeyList *pphWkL = dynamic_cast<CParseHandlerWindowKeyList *>((*this)[4]);
	DrgPdxlwk *pdrgpdxlwk = pphWkL->Pdrgpdxlwk();
	CDXLPhysicalWindow *pdxlopWindow = GPOS_NEW(m_pmp) CDXLPhysicalWindow(m_pmp, m_pdrgpulPartCols, pdrgpdxlwk);
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopWindow);

	// set statistics and physical properties
	CParseHandlerUtils::SetProperties(m_pdxln, pphProp);

	// add children
	AddChildFromParseHandler(pphPrL);
	AddChildFromParseHandler(pphF);
	AddChildFromParseHandler(pphChild);

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF

