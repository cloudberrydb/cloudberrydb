//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CParseHandlerStatisticsConfig.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing statistics
//		configuration
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerStatisticsConfig.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

#include "naucrates/dxl/xml/dxltokens.h"

#include "gpopt/engine/CStatisticsConfig.h"

using namespace gpdxl;
using namespace gpopt;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatisticsConfig::CParseHandlerStatisticsConfig
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerStatisticsConfig::CParseHandlerStatisticsConfig
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pstatsconf(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatisticsConfig::~CParseHandlerStatisticsConfig
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerStatisticsConfig::~CParseHandlerStatisticsConfig()
{
	CRefCount::SafeRelease(m_pstatsconf);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatisticsConfig::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerStatisticsConfig::StartElement
	(
	const XMLCh* const , //xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const , //xmlszQname,
	const Attributes& attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenStatisticsConfig), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// parse statistics configuration options
	CDouble dDampingFactorFilter = CDXLOperatorFactory::DValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenDampingFactorFilter, EdxltokenStatisticsConfig);
	CDouble dDampingFactorJoin = CDXLOperatorFactory::DValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenDampingFactorJoin, EdxltokenStatisticsConfig);
	CDouble dDampingFactorGroupBy = CDXLOperatorFactory::DValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenDampingFactorGroupBy, EdxltokenStatisticsConfig);

	m_pstatsconf = GPOS_NEW(m_pmp) CStatisticsConfig(m_pmp, dDampingFactorFilter, dDampingFactorJoin, dDampingFactorGroupBy);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatisticsConfig::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerStatisticsConfig::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenStatisticsConfig), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE( gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(NULL != m_pstatsconf);
	GPOS_ASSERT(0 == this->UlLength());

	// deactivate handler
	m_pphm->DeactivateHandler();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatisticsConfig::Edxlphtype
//
//	@doc:
//		Return the type of the parse handler.
//
//---------------------------------------------------------------------------
EDxlParseHandlerType
CParseHandlerStatisticsConfig::Edxlphtype() const
{
	return EdxlphStatisticsConfig;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatisticsConfig::Pstatsconf
//
//	@doc:
//		Returns the statistics configuration
//
//---------------------------------------------------------------------------
CStatisticsConfig *
CParseHandlerStatisticsConfig::Pstatsconf() const
{
	return m_pstatsconf;
}

// EOF
