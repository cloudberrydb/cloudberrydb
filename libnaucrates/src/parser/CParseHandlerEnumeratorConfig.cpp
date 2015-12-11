//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CParseHandlerEnumeratorConfig.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing enumerator
//		configuratiom params
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerEnumeratorConfig.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/xml/dxltokens.h"

#include "gpopt/engine/CEnumeratorConfig.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerEnumeratorConfig::CParseHandlerEnumeratorConfig
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerEnumeratorConfig::CParseHandlerEnumeratorConfig
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pec(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerEnumeratorConfig::~CParseHandlerEnumeratorConfig
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerEnumeratorConfig::~CParseHandlerEnumeratorConfig()
{
	CRefCount::SafeRelease(m_pec);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerEnumeratorConfig::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerEnumeratorConfig::StartElement
	(
	const XMLCh* const , //xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const , //xmlszQname,
	const Attributes& attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenEnumeratorConfig), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// parse enumerator config options
	ULLONG ullPlanId = CDXLOperatorFactory::UllValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenPlanId, EdxltokenOptimizerConfig);
	ULLONG ullPlanSamples = CDXLOperatorFactory::UllValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenPlanSamples, EdxltokenOptimizerConfig);
	CDouble dCostThreshold = CDXLOperatorFactory::DValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenCostThreshold, EdxltokenOptimizerConfig);

	m_pec = GPOS_NEW(m_pmp) CEnumeratorConfig(m_pmp, ullPlanId, ullPlanSamples, dCostThreshold);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerEnumeratorConfig::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerEnumeratorConfig::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenEnumeratorConfig), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE( gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(NULL != m_pec);
	GPOS_ASSERT(0 == this->UlLength());

	// deactivate handler
	m_pphm->DeactivateHandler();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerEnumeratorConfig::Edxlphtype
//
//	@doc:
//		Return the type of the parse handler.
//
//---------------------------------------------------------------------------
EDxlParseHandlerType
CParseHandlerEnumeratorConfig::Edxlphtype() const
{
	return EdxlphEnumeratorConfig;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerEnumeratorConfig::Pec
//
//	@doc:
//		Returns the enumerator configuration
//
//---------------------------------------------------------------------------
CEnumeratorConfig *
CParseHandlerEnumeratorConfig::Pec() const
{
	return m_pec;
}

// EOF
