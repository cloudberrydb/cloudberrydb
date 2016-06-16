//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerColDescr.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing the list of
//		column descriptors in a table descriptor node.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerColDescr.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerColDescr::CParseHandlerColDescr
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerColDescr::CParseHandlerColDescr
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pdxlcd(NULL)
{
	m_pdrgdxlcd = GPOS_NEW(m_pmp) DrgPdxlcd(m_pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerColDescr::~CParseHandlerColDescr
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------

CParseHandlerColDescr::~CParseHandlerColDescr()
{
	CRefCount::SafeRelease(m_pdrgdxlcd);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerColDescr::Pdrgpdxlcd
//
//	@doc:
//		Returns the array of column descriptors.
//
//---------------------------------------------------------------------------
DrgPdxlcd *
CParseHandlerColDescr::Pdrgpdxlcd()
{
	return m_pdrgdxlcd;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerColDescr::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerColDescr::StartElement
	(
		const XMLCh* const, // xmlszUri,
		const XMLCh* const xmlszLocalname,
		const XMLCh* const, // xmlszQname
		const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(xmlszLocalname, CDXLTokens::XmlstrToken(EdxltokenColumns)))
	{
		// start of the columns block
		GPOS_ASSERT(NULL == m_pdxlcd);
	}
	else if (0 == XMLString::compareString(xmlszLocalname, CDXLTokens::XmlstrToken(EdxltokenColumn)))
	{
		// start of a new column descriptor
		m_pdxlcd = CDXLOperatorFactory::Pdxlcd(m_pphm->Pmm(), attrs);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerColDescr::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerColDescr::EndElement
	(
		const XMLCh* const, // xmlszUri,
		const XMLCh* const xmlszLocalname,
		const XMLCh* const // xmlszQname
	)
{
	if (0 == XMLString::compareString(xmlszLocalname, CDXLTokens::XmlstrToken(EdxltokenColumns)))
	{
		// finish the columns block
		GPOS_ASSERT(NULL != m_pdrgdxlcd);
		m_pphm->DeactivateHandler();
	}
	else if (0 == XMLString::compareString(xmlszLocalname, CDXLTokens::XmlstrToken(EdxltokenColumn)))
	{
		// finish up a column descriptor
		GPOS_ASSERT(NULL != m_pdxlcd);
		GPOS_ASSERT(NULL != m_pdrgdxlcd);
		m_pdrgdxlcd->Append(m_pdxlcd);
		// reset column descr
		m_pdxlcd = NULL;
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

// EOF

