//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerAppend.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing Append operator.
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerAppend.h"

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
//		CParseHandlerAppend::CParseHandlerAppend
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerAppend::CParseHandlerAppend
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
//		CParseHandlerAppend::SetupInitialHandlers
//
//	@doc:
//		Setup initial set of handlers for append node
//
//---------------------------------------------------------------------------
void
CParseHandlerAppend::SetupInitialHandlers
	(
	const Attributes &attrs
	)
{
	// seeing a result tag
	GPOS_ASSERT(m_pdxlop == NULL && "Append dxl node should not have been created yet");
	GPOS_ASSERT(this->UlLength() == 0 && "No handlers should have been added yet");

	m_pdxlop = (CDXLPhysicalAppend *) CDXLOperatorFactory::PdxlopAppend(m_pphm->Pmm(), attrs);

	// parse handler for the filter
	CParseHandlerBase *pphFilter = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarFilter), m_pphm, this);
	m_pphm->ActivateParseHandler(pphFilter);

	// parse handler for the proj list
	CParseHandlerBase *pphPrL = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarProjList), m_pphm, this);
	m_pphm->ActivateParseHandler(pphPrL);

	//parse handler for the properties of the operator
	CParseHandlerBase *pphProp = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenProperties), m_pphm, this);
	m_pphm->ActivateParseHandler(pphProp);

	this->Append(pphProp);
	this->Append(pphPrL);
	this->Append(pphFilter);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerAppend::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerAppend::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalAppend), xmlszLocalname)
		&& NULL == m_pdxlop)
	{
		// open a root Append element
		SetupInitialHandlers(attrs);
	}
	else if (NULL != m_pdxlop)
	{
		// install a parse handler for a child node
		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenPhysical), m_pphm, this);
		m_pphm->ActivateParseHandler(pphChild);

		this->Append(pphChild);

		pphChild->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerAppend::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerAppend::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalAppend), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// construct node from the created child nodes
	CParseHandlerProperties *pphProp = dynamic_cast<CParseHandlerProperties *>((*this)[0]);
	CParseHandlerProjList *pphPrL = dynamic_cast<CParseHandlerProjList*>((*this)[1]);
	CParseHandlerFilter *pphFilter = dynamic_cast<CParseHandlerFilter *>((*this)[2]);

	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, m_pdxlop);
	CParseHandlerUtils::SetProperties(m_pdxln, pphProp);

	// add constructed children
	AddChildFromParseHandler(pphPrL);
	AddChildFromParseHandler(pphFilter);

	GPOS_ASSERT(3 <= this->UlLength());
	
	const ULONG ulLen = this->UlLength();
	// an append node can have variable number of children: add them one by one from the respective parse handlers
	for (ULONG ul = 3; ul < ulLen; ul++)
	{
		CParseHandlerPhysicalOp *pphChild = dynamic_cast<CParseHandlerPhysicalOp*>((*this)[ul]);
		AddChildFromParseHandler(pphChild);
	}

#ifdef GPOS_DEBUG
	m_pdxlop->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF

