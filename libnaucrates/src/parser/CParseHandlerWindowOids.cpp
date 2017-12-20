//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software, Inc.
//
//	@filename:
//		CParseHandlerWindowOids.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing window oids
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerWindowOids.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpdxl;
using namespace gpopt;

XERCES_CPP_NAMESPACE_USE

CParseHandlerWindowOids::CParseHandlerWindowOids
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pwindowoids(NULL)
{
}

CParseHandlerWindowOids::~CParseHandlerWindowOids()
{
	CRefCount::SafeRelease(m_pwindowoids);
}

void
CParseHandlerWindowOids::StartElement
	(
	const XMLCh* const , //xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const , //xmlszQname,
	const Attributes& attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenWindowOids), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// parse window function oids
	OID oidRowNumber = CDXLOperatorFactory::OidValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenOidRowNumber, EdxltokenWindowOids);
	OID oidRank = CDXLOperatorFactory::OidValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenOidRank, EdxltokenWindowOids);

	m_pwindowoids = GPOS_NEW(m_pmp) CWindowOids(oidRowNumber, oidRank);
}

// invoked by Xerces to process a closing tag
void
CParseHandlerWindowOids::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenWindowOids), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE( gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(NULL != m_pwindowoids);
	GPOS_ASSERT(0 == this->UlLength());

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// return the type of the parse handler.
EDxlParseHandlerType
CParseHandlerWindowOids::Edxlphtype() const
{
	return EdxlphWindowOids;
}

CWindowOids *
CParseHandlerWindowOids::Pwindowoids() const
{
	return m_pwindowoids;
}

// EOF
