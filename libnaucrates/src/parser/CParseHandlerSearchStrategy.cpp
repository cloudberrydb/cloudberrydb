//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerSearchStrategy.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing search strategy.
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerSearchStrategy.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerSearchStage.h"

using namespace gpdxl;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerSearchStrategy::CParseHandlerSearchStrategy
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerSearchStrategy::CParseHandlerSearchStrategy
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pdrgpss(NULL)
{}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerSearchStrategy::~CParseHandlerSearchStrategy
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerSearchStrategy::~CParseHandlerSearchStrategy()
{
	CRefCount::SafeRelease(m_pdrgpss);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerSearchStrategy::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerSearchStrategy::StartElement
	(
	const XMLCh* const xmlstrUri,
	const XMLCh* const xmlstrLocalname,
	const XMLCh* const xmlstrQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenSearchStrategy), xmlstrLocalname))
	{
		m_pdrgpss = GPOS_NEW(m_pmp) DrgPss(m_pmp);
	}
	else if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenSearchStage), xmlstrLocalname))
	{
		GPOS_ASSERT(NULL != m_pdrgpss);

		// start new search stage
		CParseHandlerBase *pphSearchStage = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenSearchStage), m_pphm, this);
		m_pphm->ActivateParseHandler(pphSearchStage);

		// store parse handler
		this->Append(pphSearchStage);

		pphSearchStage->startElement(xmlstrUri, xmlstrLocalname, xmlstrQname, attrs);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlstrLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerSearchStrategy::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerSearchStrategy::EndElement
	(
	const XMLCh* const, // xmlstrUri,
	const XMLCh* const xmlstrLocalname,
	const XMLCh* const // xmlstrQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenSearchStrategy), xmlstrLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlstrLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	const ULONG ulSize = this->UlLength();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CParseHandlerSearchStage *pphSearchStage = dynamic_cast<CParseHandlerSearchStage*>((*this)[ul]);
		CXformSet *pxfs = pphSearchStage->Pxfs();
		pxfs->AddRef();
		CSearchStage *pss = GPOS_NEW(m_pmp) CSearchStage(pxfs, pphSearchStage->UlTimeThreshold(), pphSearchStage->CostThreshold());
		m_pdrgpss->Append(pss);
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF

