//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CParseHandlerCTEConfig.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing CTE
//		configuration
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerCTEConfig.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

#include "naucrates/dxl/xml/dxltokens.h"

#include "gpopt/engine/CCTEConfig.h"

using namespace gpdxl;
using namespace gpopt;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCTEConfig::CParseHandlerCTEConfig
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerCTEConfig::CParseHandlerCTEConfig
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pcteconf(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCTEConfig::~CParseHandlerCTEConfig
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerCTEConfig::~CParseHandlerCTEConfig()
{
	CRefCount::SafeRelease(m_pcteconf);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCTEConfig::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerCTEConfig::StartElement
	(
	const XMLCh* const , //xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const , //xmlszQname,
	const Attributes& attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCTEConfig), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// parse CTE configuration options
	ULONG ulCTEInliningCutoff = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenCTEInliningCutoff, EdxltokenCTEConfig);

	m_pcteconf = GPOS_NEW(m_pmp) CCTEConfig(ulCTEInliningCutoff);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCTEConfig::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerCTEConfig::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCTEConfig), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE( gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(NULL != m_pcteconf);
	GPOS_ASSERT(0 == this->UlLength());

	// deactivate handler
	m_pphm->DeactivateHandler();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCTEConfig::Edxlphtype
//
//	@doc:
//		Return the type of the parse handler.
//
//---------------------------------------------------------------------------
EDxlParseHandlerType
CParseHandlerCTEConfig::Edxlphtype() const
{
	return EdxlphCTEConfig;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCTEConfig::Pcteconf
//
//	@doc:
//		Returns the CTE configuration
//
//---------------------------------------------------------------------------
CCTEConfig *
CParseHandlerCTEConfig::Pcteconf() const
{
	return m_pcteconf;
}

// EOF
