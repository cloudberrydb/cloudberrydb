//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerWindowKeyList.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing the list of
//		window keys in the window operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerWindowKey.h"
#include "naucrates/dxl/parser/CParseHandlerWindowKeyList.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerWindowKeyList::CParseHandlerWindowKeyList
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerWindowKeyList::CParseHandlerWindowKeyList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pdrgpdxlwk(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerWindowKeyList::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerWindowKeyList::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenWindowKeyList), xmlszLocalname))
	{
		m_pdrgpdxlwk = GPOS_NEW(m_pmp) DrgPdxlwk(m_pmp);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenWindowKey), xmlszLocalname))
	{
		// we must have seen a window key list already
		GPOS_ASSERT(NULL != m_pdrgpdxlwk);
		// start new window key element
		CParseHandlerBase *pphWk =
				CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenWindowKey), m_pphm, this);
		m_pphm->ActivateParseHandler(pphWk);

		// store parse handler
		this->Append(pphWk);

		pphWk->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerWindowKeyList::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerWindowKeyList::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if ( 0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenWindowKeyList), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	GPOS_ASSERT(NULL != m_pdrgpdxlwk);

	const ULONG ulSize = this->UlLength();
	// add the window keys to the list
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CParseHandlerWindowKey *pphWk = dynamic_cast<CParseHandlerWindowKey *>((*this)[ul]);
		m_pdrgpdxlwk->Append(pphWk->Pdxlwk());
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
