//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CParseHandlerCostParam
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing xform
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerCostParam.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"

using namespace gpdxl;
using namespace gpopt;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCostParam::CParseHandlerCostParam
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerCostParam::CParseHandlerCostParam
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_szName(NULL),
	m_dVal(0),
	m_dLowerBound(0),
	m_dUpperBound(0)
{}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCostParam::~CParseHandlerCostParam
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerCostParam::~CParseHandlerCostParam()
{
	GPOS_DELETE_ARRAY(m_szName);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCostParam::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerCostParam::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlstrLocalname,
	const XMLCh* const, // xmlstrQname
	const Attributes& attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCostParam), xmlstrLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlstrLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	const XMLCh *xmlstrName = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenName, EdxltokenCostParam);
	CWStringDynamic *pstrName = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlstrName);
	m_szName = CDXLUtils::SzFromWsz(m_pmp, pstrName->Wsz());
	GPOS_DELETE(pstrName);

	m_dVal = CDXLOperatorFactory::DValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenValue, EdxltokenCostParam);
	m_dLowerBound = CDXLOperatorFactory::DValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenCostParamLowerBound, EdxltokenCostParam);
	m_dUpperBound = CDXLOperatorFactory::DValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenCostParamUpperBound, EdxltokenCostParam);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCostParam::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerCostParam::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlstrLocalname,
	const XMLCh* const // xmlstrQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCostParam), xmlstrLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlstrLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}


// EOF

