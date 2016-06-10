//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerLogicalWindow.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing logical
//		window operators
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerLogicalWindow.h"
#include "naucrates/dxl/parser/CParseHandlerWindowSpecList.h"
#include "naucrates/dxl/parser/CParseHandlerProjList.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalWindow::CParseHandlerLogicalWindow
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerLogicalWindow::CParseHandlerLogicalWindow
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerLogicalOp(pmp, pphm, pphRoot),
	m_pdrgpdxlws(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalWindow::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerLogicalWindow::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& //attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalWindow), xmlszLocalname))
	{
		// create child node parsers
		// parse handler for logical operator
		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenLogical), m_pphm, this);
		m_pphm->ActivateParseHandler(pphChild);

		// parse handler for the proj list
		CParseHandlerBase *pphPrL = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarProjList), m_pphm, this);
		m_pphm->ActivateParseHandler(pphPrL);

		// parse handler for window specification list
		CParseHandlerBase *pphWsL = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenWindowSpecList), m_pphm, this);
		m_pphm->ActivateParseHandler(pphWsL);

		// store child parse handler in array
		this->Append(pphWsL);
		this->Append(pphPrL);
		this->Append(pphChild);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalWindow::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerLogicalWindow::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalWindow), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	CParseHandlerWindowSpecList *pphWsL = dynamic_cast<CParseHandlerWindowSpecList*>((*this)[0]);
	CParseHandlerProjList *pphPrL = dynamic_cast<CParseHandlerProjList*>((*this)[1]);
	CParseHandlerLogicalOp *pphLgOp = dynamic_cast<CParseHandlerLogicalOp*>((*this)[2]);

	DrgPdxlws *pdrgpdxlws = pphWsL->Pdrgpdxlws();
	GPOS_ASSERT(NULL != pdrgpdxlws);

	CDXLLogicalWindow *pdxlopWin = GPOS_NEW(m_pmp) CDXLLogicalWindow(m_pmp, pdrgpdxlws);
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopWin);
	GPOS_ASSERT(NULL != pphPrL->Pdxln());
	GPOS_ASSERT(NULL != pphLgOp->Pdxln());

	AddChildFromParseHandler(pphPrL);
	AddChildFromParseHandler(pphLgOp);

#ifdef GPOS_DEBUG
	m_pdxln->Pdxlop()->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG

	// deactivate handler
	m_pphm->DeactivateHandler();
}
// EOF
