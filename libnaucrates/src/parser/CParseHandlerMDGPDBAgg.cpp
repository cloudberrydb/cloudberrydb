//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CParseHandlerMDGPDBAgg.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing metadata for
//		GPDB aggregates.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerMDGPDBAgg.h"

#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBAgg::CParseHandlerMDGPDBAgg
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerMDGPDBAgg::CParseHandlerMDGPDBAgg
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerMetadataObject(pmp, pphm, pphRoot),
	m_pmdid(NULL),
	m_pmdname(NULL),
	m_pmdidTypeResult(NULL),
	m_pmdidTypeIntermediate(NULL),
	m_fOrdered(false),
	m_fSplittable(true),
	m_fHashAggCapable(true)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBAgg::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDGPDBAgg::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBAgg), xmlszLocalname))
	{
		// parse agg name
		const XMLCh *xmlszAggName = CDXLOperatorFactory::XmlstrFromAttrs
										(
										attrs,
										EdxltokenName,
										EdxltokenGPDBAgg
										);

		CWStringDynamic *pstrAggName = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszAggName);
		
		// create a copy of the string in the CMDName constructor
		m_pmdname = GPOS_NEW(m_pmp) CMDName(m_pmp, pstrAggName);
		
		GPOS_DELETE(pstrAggName);

		// parse metadata id info
		m_pmdid = CDXLOperatorFactory::PmdidFromAttrs
											(
											m_pphm->Pmm(),
											attrs,
											EdxltokenMdid,
											EdxltokenGPDBAgg
											);
					
		// parse ordered aggregate info
		const XMLCh *xmlszOrderedAgg = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenGPDBIsAggOrdered));
		if (NULL != xmlszOrderedAgg)
		{
			m_fOrdered = CDXLOperatorFactory::FValueFromXmlstr
												(
												m_pphm->Pmm(),
												xmlszOrderedAgg,
												EdxltokenGPDBIsAggOrdered,
												EdxltokenGPDBAgg
												);
		}
		
		// parse splittable aggregate info
		const XMLCh *xmlszSplittableAgg = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenGPDBAggSplittable));
		if (NULL != xmlszSplittableAgg)
		{
			m_fSplittable = CDXLOperatorFactory::FValueFromXmlstr
												(
												m_pphm->Pmm(),
												xmlszSplittableAgg,
												EdxltokenGPDBAggSplittable,
												EdxltokenGPDBAgg
												);
		}

		// parse hash capable aggragate info
		const XMLCh *xmlszHashAggCapableAgg = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenGPDBAggHashAggCapable));
		if (NULL != xmlszHashAggCapableAgg)
		{
			m_fHashAggCapable = CDXLOperatorFactory::FValueFromXmlstr
												(
												m_pphm->Pmm(),
												xmlszHashAggCapableAgg,
												EdxltokenGPDBAggHashAggCapable,
												EdxltokenGPDBAgg
												);
		}
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBAggResultTypeId), xmlszLocalname))
	{
		// parse result type
		GPOS_ASSERT(NULL != m_pmdname);

		m_pmdidTypeResult = CDXLOperatorFactory::PmdidFromAttrs
													(
													m_pphm->Pmm(),
													attrs,
													EdxltokenMdid,
													EdxltokenGPDBAggResultTypeId
													);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBAggIntermediateResultTypeId), xmlszLocalname))
	{
		// parse intermediate result type
		GPOS_ASSERT(NULL != m_pmdname);

		m_pmdidTypeIntermediate = CDXLOperatorFactory::PmdidFromAttrs
														(
														m_pphm->Pmm(),
														attrs,
														EdxltokenMdid,
														EdxltokenGPDBAggIntermediateResultTypeId
														);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBAgg::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDGPDBAgg::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBAgg), xmlszLocalname))
	{
		// construct the MD agg object from its part
		GPOS_ASSERT(m_pmdid->FValid() && NULL != m_pmdname);
		
		m_pimdobj = GPOS_NEW(m_pmp) CMDAggregateGPDB(m_pmp,
												m_pmdid,
												m_pmdname,
												m_pmdidTypeResult,
												m_pmdidTypeIntermediate,
												m_fOrdered,
												m_fSplittable,
												m_fHashAggCapable
												);
		
		// deactivate handler
		m_pphm->DeactivateHandler();

	}
	else if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBAggResultTypeId), xmlszLocalname) && 
			 0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBAggIntermediateResultTypeId), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

// EOF
