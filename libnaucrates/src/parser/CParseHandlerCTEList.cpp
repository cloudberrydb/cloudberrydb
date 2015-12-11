//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerCTEList.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing CTE lists
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerCTEList.h"
#include "naucrates/dxl/parser/CParseHandlerLogicalCTEProducer.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCTEList::CParseHandlerCTEList
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerCTEList::CParseHandlerCTEList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pdrgpdxln(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCTEList::~CParseHandlerCTEList
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerCTEList::~CParseHandlerCTEList()
{
	CRefCount::SafeRelease(m_pdrgpdxln);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCTEList::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerCTEList::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCTEList), xmlszLocalname))
	{
		GPOS_ASSERT(NULL == m_pdrgpdxln);
		m_pdrgpdxln = GPOS_NEW(m_pmp) DrgPdxln(m_pmp);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalCTEProducer), xmlszLocalname))
	{
		GPOS_ASSERT(NULL != m_pdrgpdxln);

		// start new CTE producer
		CParseHandlerBase *pphCTEProducer = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenLogicalCTEProducer), m_pphm, this);
		m_pphm->ActivateParseHandler(pphCTEProducer);
		
		// store parse handler
		this->Append(pphCTEProducer);
		
		pphCTEProducer->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCTEList::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerCTEList::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCTEList), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(NULL != m_pdrgpdxln);

	const ULONG ulLen = this->UlLength();

	// add CTEs
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CParseHandlerLogicalCTEProducer *pphCTE = dynamic_cast<CParseHandlerLogicalCTEProducer *>((*this)[ul]);
		CDXLNode *pdxlnCTE = pphCTE->Pdxln();
		pdxlnCTE->AddRef();
		m_pdrgpdxln->Append(pdxlnCTE);
	}
		
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
