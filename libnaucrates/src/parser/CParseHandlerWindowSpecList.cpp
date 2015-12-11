//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerWindowSpecList.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing the list of
//		window specifications in the logical window operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerWindowSpec.h"
#include "naucrates/dxl/parser/CParseHandlerWindowSpecList.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerWindowSpecList::CParseHandlerWindowSpecList
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerWindowSpecList::CParseHandlerWindowSpecList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pdrgpdxlws(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerWindowSpecList::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerWindowSpecList::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenWindowSpecList), xmlszLocalname))
	{
		m_pdrgpdxlws = GPOS_NEW(m_pmp) DrgPdxlws(m_pmp);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenWindowSpec), xmlszLocalname))
	{
		// we must have seen a window specification list already
		GPOS_ASSERT(NULL != m_pdrgpdxlws);
		// start new window specification element
		CParseHandlerBase *pphWs =
				CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenWindowSpec), m_pphm, this);
		m_pphm->ActivateParseHandler(pphWs);

		// store parse handler
		this->Append(pphWs);

		pphWs->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerWindowSpecList::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerWindowSpecList::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenWindowSpecList), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	GPOS_ASSERT(NULL != m_pdrgpdxlws);

	const ULONG ulSize = this->UlLength();
	// add the window specifications to the list
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CParseHandlerWindowSpec *pphWs = dynamic_cast<CParseHandlerWindowSpec *>((*this)[ul]);
		m_pdrgpdxlws->Append(pphWs->Pdxlws());
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
