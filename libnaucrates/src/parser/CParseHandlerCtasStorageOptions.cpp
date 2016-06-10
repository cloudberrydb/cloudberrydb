//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CParseHandlerCtasStorageOptions.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing CTAS storage
//		options
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerCtasStorageOptions.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCtasStorageOptions::CParseHandlerCtasStorageOptions
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerCtasStorageOptions::CParseHandlerCtasStorageOptions
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pmdnameTablespace(NULL),
	m_pdxlctasopt(NULL),
	m_pdrgpctasopt(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCtasStorageOptions::~CParseHandlerCtasStorageOptions
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerCtasStorageOptions::~CParseHandlerCtasStorageOptions()
{
	CRefCount::SafeRelease(m_pdxlctasopt);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCtasStorageOptions::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerCtasStorageOptions::StartElement
	(
	const XMLCh* const , // xmlszUri
	const XMLCh* const xmlszLocalname,
	const XMLCh* const , // xmlszQname
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCTASOptions), xmlszLocalname))
	{
		const XMLCh *xmlszTablespace = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenTablespace));
		if (NULL != xmlszTablespace)
		{
			m_pmdnameTablespace = CDXLUtils::PmdnameFromXmlsz(m_pphm->Pmm(), xmlszTablespace);
		}
		
		m_ectascommit = CDXLOperatorFactory::EctascommitFromAttr(attrs);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCTASOption), xmlszLocalname))
	{
		// parse option name and value
		ULONG ulType = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenCtasOptionType, EdxltokenCTASOption);
		CWStringBase *pstrName = CDXLOperatorFactory::PstrValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenName, EdxltokenCTASOption);
		CWStringBase *pstrValue = CDXLOperatorFactory::PstrValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenValue, EdxltokenCTASOption);
		BOOL fNull = CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenIsNull, EdxltokenCTASOption);
		
		if (NULL == m_pdrgpctasopt)
		{
			m_pdrgpctasopt = GPOS_NEW(m_pmp) CDXLCtasStorageOptions::DrgPctasOpt(m_pmp);
		}
		m_pdrgpctasopt->Append(
				GPOS_NEW(m_pmp) CDXLCtasStorageOptions::CDXLCtasOption(ulType, pstrName, pstrValue, fNull));
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCtasStorageOptions::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerCtasStorageOptions::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCTASOptions), xmlszLocalname))
	{
		m_pdxlctasopt = GPOS_NEW(m_pmp) CDXLCtasStorageOptions(m_pmdnameTablespace, m_ectascommit, m_pdrgpctasopt);
		// deactivate handler
		m_pphm->DeactivateHandler();
	}
	else if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCTASOption), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCtasStorageOptions::Pdxlctasopt
//
//	@doc:
//		Return parsed storage options
//
//---------------------------------------------------------------------------
CDXLCtasStorageOptions *
CParseHandlerCtasStorageOptions::Pdxlctasopt() const
{
	return m_pdxlctasopt;
}

// EOF
