//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerStatistics.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing a DXL document
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerStatistics.h"

#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerStatsDerivedRelation.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatistics::CParseHandlerStatistics
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerStatistics::CParseHandlerStatistics
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pdrgpdxlstatsderrel(NULL)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatistics::~CParseHandlerStatistics
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerStatistics::~CParseHandlerStatistics()
{
	CRefCount::SafeRelease(m_pdrgpdxlstatsderrel);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatistics::Edxlphtype
//
//	@doc:
//		Return the type of the parse handler. Currently we overload this method to
//		return a specific type for the plann, query and metadata parse handlers.
//
//---------------------------------------------------------------------------
EDxlParseHandlerType
CParseHandlerStatistics::Edxlphtype() const
{
	return EdxlphStatistics;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatistics::Pdrgpdxlstatsderrel
//
//	@doc:
//		Returns the list of statistics objects constructed by the parser
//
//---------------------------------------------------------------------------
DrgPdxlstatsderrel *
CParseHandlerStatistics::Pdrgpdxlstatsderrel() const
{
	return m_pdrgpdxlstatsderrel;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatistics::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerStatistics::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(xmlszLocalname, CDXLTokens::XmlstrToken(EdxltokenStatistics)))
	{
		// start of the statistics section in the DXL document
		GPOS_ASSERT(NULL == m_pdrgpdxlstatsderrel);

		m_pdrgpdxlstatsderrel = GPOS_NEW(m_pmp) DrgPdxlstatsderrel(m_pmp);
	}
	else
	{
		// currently we only have derived relation statistics objects
		GPOS_ASSERT(NULL != m_pdrgpdxlstatsderrel);

		// install a parse handler for the given element
		CParseHandlerBase *pph = CParseHandlerFactory::Pph(m_pmp, xmlszLocalname, m_pphm, this);

		m_pphm->ActivateParseHandler(pph);

		// store parse handler
		this->Append(pph);

		pph->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatistics::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerStatistics::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(xmlszLocalname, CDXLTokens::XmlstrToken(EdxltokenStatistics)))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(NULL != m_pdrgpdxlstatsderrel);

	const ULONG ulStats = this->UlLength();
	for (ULONG ul = 0; ul < ulStats; ul++)
	{
		CParseHandlerStatsDerivedRelation *pph = dynamic_cast<CParseHandlerStatsDerivedRelation *>((*this)[ul]);

		CDXLStatsDerivedRelation *pdxlstatsderrel = pph->Pdxlstatsderrel();
		pdxlstatsderrel->AddRef();
		m_pdrgpdxlstatsderrel->Append(pdxlstatsderrel);
	}

	m_pphm->DeactivateHandler();
}


// EOF
