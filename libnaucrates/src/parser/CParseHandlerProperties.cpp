//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerProperties.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing physical properties.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerProperties.h"
#include "naucrates/dxl/parser/CParseHandlerCost.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerStatsDerivedRelation.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerProperties::CParseHandlerProperties
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerProperties::CParseHandlerProperties
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pdxlprop(NULL),
	m_pdxlstatsderrel(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerProperties::~CParseHandlerProperties
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerProperties::~CParseHandlerProperties()
{
	CRefCount::SafeRelease(m_pdxlprop);
	CRefCount::SafeRelease(m_pdxlstatsderrel);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerProperties::Pdxlprop
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLPhysicalProperties *
CParseHandlerProperties::Pdxlprop() const
{
	GPOS_ASSERT(NULL != m_pdxlprop);
	return m_pdxlprop;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerProperties::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerProperties::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenProperties), xmlszLocalname))
	{
		// create and install cost and output column parsers
		CParseHandlerBase *pph = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenCost), m_pphm, this);
		m_pphm->ActivateParseHandler(pph);

		// store parse handler
		this->Append(pph);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenStatsDerivedRelation), xmlszLocalname))
	{
		GPOS_ASSERT(1 == this->UlLength());

		// create and install derived relation statistics parsers
		CParseHandlerBase *pphStats = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenStatsDerivedRelation), m_pphm, this);
		m_pphm->ActivateParseHandler(pphStats);

		// store parse handler
		this->Append(pphStats);

		pphStats->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerProperties::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerProperties::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenProperties), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	GPOS_ASSERT((1 == this->UlLength()) || (2 == this->UlLength()));
	
	// assemble the properties container from the cost
	CParseHandlerCost *pph = dynamic_cast<CParseHandlerCost *>((*this)[0]);

	CDXLOperatorCost *pdxlopcost = pph->Pdxlopcost();
	pdxlopcost->AddRef();
	
	if (2 == this->UlLength())
	{
		CParseHandlerStatsDerivedRelation *pphStats = dynamic_cast<CParseHandlerStatsDerivedRelation *>((*this)[1]);

		CDXLStatsDerivedRelation *pdxlstatsderrel = pphStats->Pdxlstatsderrel();
		pdxlstatsderrel->AddRef();
		m_pdxlstatsderrel = pdxlstatsderrel;
	}

	m_pdxlprop = GPOS_NEW(m_pmp) CDXLPhysicalProperties(pdxlopcost);

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF

