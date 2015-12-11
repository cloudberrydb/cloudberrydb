//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerIndexDescr.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing the index
//		 descriptor portion of an index scan node
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerIndexDescr.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerIndexDescr::CParseHandlerIndexDescr
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerIndexDescr::CParseHandlerIndexDescr
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pdxlid(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerIndexDescr::~CParseHandlerIndexDescr
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerIndexDescr::~CParseHandlerIndexDescr()
{
	CRefCount::SafeRelease(m_pdxlid);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerIndexDescr::Pdxlid
//
//	@doc:
//		Returns the index descriptor constructed by the parse handler
//
//---------------------------------------------------------------------------
CDXLIndexDescr *
CParseHandlerIndexDescr::Pdxlid()
{
	return m_pdxlid;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerIndexDescr::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerIndexDescr::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenIndexDescr), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// generate the index descriptor
	m_pdxlid = CDXLOperatorFactory::Pdxlid(m_pphm->Pmm(), attrs);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerIndexDescr::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerIndexDescr::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenIndexDescr), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(0 == this->UlLength());

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
