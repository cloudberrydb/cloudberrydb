//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerColStats.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing column
//		statistics.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/md/CDXLColStats.h"

#include "naucrates/dxl/parser/CParseHandlerColStats.h"
#include "naucrates/dxl/parser/CParseHandlerColStatsBucket.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;
using namespace gpmd;
using namespace gpnaucrates;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerColStats::CParseHandlerColStats
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerColStats::CParseHandlerColStats
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerMetadataObject(pmp, pphm, pphRoot),
	m_pmdidColStats(NULL),
	m_pmdname(NULL),
	m_dWidth(0.0),
	m_dNullFreq(0.0),
	m_dDistinctRemain(0.0),
	m_dFreqRemain(0.0),
	m_fColStatsMissing(false)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerColStats::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerColStats::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenColumnStats), xmlszLocalname))
	{
		// new column stats object 
		GPOS_ASSERT(NULL == m_pmdidColStats);

		// parse mdid and name
		IMDId *pmdid = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenMdid, EdxltokenColumnStats);
		m_pmdidColStats = CMDIdColStats::PmdidConvert(pmdid);
		
		// parse column name
		const XMLCh *xmlszColName = CDXLOperatorFactory::XmlstrFromAttrs
																(
																attrs,
																EdxltokenName,
																EdxltokenColumnStats
																);

		CWStringDynamic *pstrColName = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszColName);
		
		// create a copy of the string in the CMDName constructor
		m_pmdname = GPOS_NEW(m_pmp) CMDName(m_pmp, pstrColName);
		GPOS_DELETE(pstrColName);
		
		m_dWidth = CDXLOperatorFactory::DValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenWidth, EdxltokenColumnStats);

		const XMLCh *xmlszNullFreq = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenColNullFreq));
		if (NULL != xmlszNullFreq)
		{
			m_dNullFreq = CDXLOperatorFactory::DValueFromXmlstr(m_pphm->Pmm(), xmlszNullFreq, EdxltokenColNullFreq, EdxltokenColumnStats);
		}

		const XMLCh *xmlszDistinctRemain = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenColNdvRemain));
		if (NULL != xmlszDistinctRemain)
		{
			m_dDistinctRemain = CDXLOperatorFactory::DValueFromXmlstr(m_pphm->Pmm(), xmlszDistinctRemain, EdxltokenColNdvRemain, EdxltokenColumnStats);
		}

		const XMLCh *xmlszFreqRemain = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenColFreqRemain));
		if (NULL != xmlszFreqRemain)
		{
			m_dFreqRemain = CDXLOperatorFactory::DValueFromXmlstr(m_pphm->Pmm(), xmlszFreqRemain, EdxltokenColFreqRemain, EdxltokenColumnStats);
		}

		const XMLCh *xmlszColStatsMissing = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenColStatsMissing));
		if (NULL != xmlszColStatsMissing)
		{
			m_fColStatsMissing = CDXLOperatorFactory::FValueFromXmlstr(m_pphm->Pmm(), xmlszColStatsMissing, EdxltokenColStatsMissing, EdxltokenColumnStats);
		}

	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenColumnStatsBucket), xmlszLocalname))
	{
		// new bucket
		CParseHandlerBase *pphStatsBucket = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenColumnStatsBucket), m_pphm, this);
		this->Append(pphStatsBucket);
		
		m_pphm->ActivateParseHandler(pphStatsBucket);	
		pphStatsBucket->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerColStats::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerColStats::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenColumnStats), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// get histogram buckets from child parse handlers
	
	DrgPdxlbucket *pdrgpdxlbucket = GPOS_NEW(m_pmp) DrgPdxlbucket(m_pmp);
	
	for (ULONG ul = 0; ul < this->UlLength(); ul++)
	{
		CParseHandlerColStatsBucket *pphBucket = dynamic_cast<CParseHandlerColStatsBucket *>((*this)[ul]);
				
		CDXLBucket *pdxlbucket = pphBucket->Pdxlbucket();
		pdxlbucket->AddRef();
		
		pdrgpdxlbucket->Append(pdxlbucket);
	}
	
	m_pimdobj = GPOS_NEW(m_pmp) CDXLColStats
							(
							m_pmp,
							m_pmdidColStats,
							m_pmdname,
							m_dWidth,
							m_dNullFreq,
							m_dDistinctRemain,
							m_dFreqRemain,
							pdrgpdxlbucket,
							m_fColStatsMissing
							);
	
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
