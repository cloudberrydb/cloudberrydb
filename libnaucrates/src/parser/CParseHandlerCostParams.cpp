//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CParseHandlerCostParams.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing cost parameters.
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerCostParam.h"
#include "naucrates/dxl/parser/CParseHandlerCostParams.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"

#include "gpdbcost/CCostModelParamsGPDB.h"

using namespace gpdxl;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCostParams::CParseHandlerCostParams
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerCostParams::CParseHandlerCostParams
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pcp(NULL)
{}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCostParams::~CParseHandlerCostParams
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerCostParams::~CParseHandlerCostParams()
{
	CRefCount::SafeRelease(m_pcp);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCostParams::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerCostParams::StartElement
	(
	const XMLCh* const xmlstrUri,
	const XMLCh* const xmlstrLocalname,
	const XMLCh* const xmlstrQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCostParams), xmlstrLocalname))
	{
		// as of now, we only parse params of GPDB cost model
		m_pcp = GPOS_NEW(m_pmp) CCostModelParamsGPDB(m_pmp);
	}
	else if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCostParam), xmlstrLocalname))
	{
		GPOS_ASSERT(NULL != m_pcp);

		// start new search stage
		CParseHandlerBase *pphCostParam = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenCostParam), m_pphm, this);
		m_pphm->ActivateParseHandler(pphCostParam);

		// store parse handler
		this->Append(pphCostParam);

		pphCostParam->startElement(xmlstrUri, xmlstrLocalname, xmlstrQname, attrs);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlstrLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCostParams::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerCostParams::EndElement
	(
	const XMLCh* const, // xmlstrUri,
	const XMLCh* const xmlstrLocalname,
	const XMLCh* const // xmlstrQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCostParams), xmlstrLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlstrLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	const ULONG ulSize = this->UlLength();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CParseHandlerCostParam *pphCostParam = dynamic_cast<CParseHandlerCostParam*>((*this)[ul]);
		m_pcp->SetParam(pphCostParam->SzName(), pphCostParam->DVal(), pphCostParam->DLowerBound(), pphCostParam->DUpperBound());
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF

