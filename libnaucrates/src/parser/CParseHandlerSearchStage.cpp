//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerSearchStage.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing search strategy.
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerSearchStage.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerXform.h"


using namespace gpopt;
using namespace gpdxl;


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerSearchStage::CParseHandlerSearchStage
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerSearchStage::CParseHandlerSearchStage
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pxfs(NULL),
	m_costThreshold(GPOPT_INVALID_COST)
{}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerSearchStage::~CParseHandlerSearchStage
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerSearchStage::~CParseHandlerSearchStage()
{
	CRefCount::SafeRelease(m_pxfs);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerSearchStage::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerSearchStage::StartElement
	(
	const XMLCh* const xmlstrUri,
	const XMLCh* const xmlstrLocalname,
	const XMLCh* const xmlstrQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenSearchStage), xmlstrLocalname))
	{
		// start search stage section in the DXL document
		GPOS_ASSERT(NULL == m_pxfs);

		m_pxfs = GPOS_NEW(m_pmp) CXformSet(m_pmp);

		const XMLCh *xmlszCost =
			CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenCostThreshold, EdxltokenSearchStage);

		m_costThreshold =
			CCost(CDXLOperatorFactory::DValueFromXmlstr(m_pphm->Pmm(), xmlszCost, EdxltokenCostThreshold, EdxltokenSearchStage));

		const XMLCh *xmlszTime =
			CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenTimeThreshold, EdxltokenSearchStage);

		m_ulTimeThreshold =
			CDXLOperatorFactory::UlValueFromXmlstr(m_pphm->Pmm(), xmlszTime, EdxltokenTimeThreshold, EdxltokenSearchStage);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenXform), xmlstrLocalname))
	{
		GPOS_ASSERT(NULL != m_pxfs);

		// start new xform
		CParseHandlerBase *pphXform = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenXform), m_pphm, this);
		m_pphm->ActivateParseHandler(pphXform);

		// store parse handler
		this->Append(pphXform);

		pphXform->startElement(xmlstrUri, xmlstrLocalname, xmlstrQname, attrs);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlstrLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerSearchStage::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerSearchStage::EndElement
	(
	const XMLCh* const, // xmlstrUri,
	const XMLCh* const xmlstrLocalname,
	const XMLCh* const // xmlstrQname
	)
{

	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenSearchStage), xmlstrLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlstrLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	const ULONG ulSize = this->UlLength();
	// add constructed children from child parse handlers
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CParseHandlerXform *pphXform = dynamic_cast<CParseHandlerXform*>((*this)[ul]);
#ifdef GPOS_DEBUG
		BOOL fSet =
#endif // GPOS_DEBUG
			m_pxfs->FExchangeSet(pphXform->Pxform()->Exfid());
		GPOS_ASSERT(!fSet);
	}

	// deactivate handler
	m_pphm->DeactivateHandler();

}

// EOF

