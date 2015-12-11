//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerStatsDerivedColumn.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing statistics of
//		derived column.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerStatsDerivedColumn.h"
#include "naucrates/dxl/parser/CParseHandlerColStatsBucket.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;
using namespace gpnaucrates;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatsDerivedColumn::CParseHandlerStatsDerivedColumn
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerStatsDerivedColumn::CParseHandlerStatsDerivedColumn
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_ulColId(0),
	m_dWidth(CStatistics::DDefaultColumnWidth),
	m_dNullFreq(0.0),
	m_dDistinctRemain(0.0),
	m_dFreqRemain(0.0),
	m_pstatsdercol(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatsDerivedColumn::~CParseHandlerStatsDerivedColumn
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerStatsDerivedColumn::~CParseHandlerStatsDerivedColumn()
{
	m_pstatsdercol->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatsDerivedColumn::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerStatsDerivedColumn::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenStatsDerivedColumn), xmlszLocalname))
	{
		// must have not seen a bucket yet
		GPOS_ASSERT(0 == this->UlLength());

		// parse column id
		m_ulColId = CDXLOperatorFactory::UlValueFromAttrs
											(
											m_pphm->Pmm(),
											attrs,
											EdxltokenColId,
											EdxltokenStatsDerivedColumn
											);

		// parse column width
		m_dWidth = CDXLOperatorFactory::DValueFromAttrs
											(
											m_pphm->Pmm(),
											attrs,
											EdxltokenWidth,
											EdxltokenStatsDerivedColumn
											);

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
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenColumnStatsBucket), xmlszLocalname))
	{
		// install a parse handler for the given element
		CParseHandlerBase *pph = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenColumnStatsBucket), m_pphm, this);
		m_pphm->ActivateParseHandler(pph);

		// store parse handler
		this->Append(pph);

		pph->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatsDerivedColumn::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerStatsDerivedColumn::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenStatsDerivedColumn), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	DrgPdxlbucket *pdrgpdxlbucket = GPOS_NEW(m_pmp) DrgPdxlbucket(m_pmp);

	const ULONG ulBuckets = this->UlLength();
	// add constructed children from child parse handlers
	for (ULONG ul = 0; ul < ulBuckets; ul++)
	{
		CParseHandlerColStatsBucket *pph = dynamic_cast<CParseHandlerColStatsBucket*>((*this)[ul]);
		CDXLBucket *pdxlbucket = pph->Pdxlbucket();
		pdxlbucket->AddRef();
		pdrgpdxlbucket->Append(pdxlbucket);
	}

	m_pstatsdercol = GPOS_NEW(m_pmp) CDXLStatsDerivedColumn(m_ulColId, m_dWidth, m_dNullFreq, m_dDistinctRemain, m_dFreqRemain, pdrgpdxlbucket);

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
