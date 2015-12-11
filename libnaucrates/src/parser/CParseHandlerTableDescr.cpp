//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerTableDescr.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing a table descriptor portion
//		of a table scan node.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerTableDescr.h"

#include "naucrates/dxl/parser/CParseHandlerColDescr.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTableDescr::CParseHandlerTableDescr
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerTableDescr::CParseHandlerTableDescr
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pdxltabdesc(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTableDescr::~CParseHandlerTableDescr
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CParseHandlerTableDescr::~CParseHandlerTableDescr()
{
	CRefCount::SafeRelease(m_pdxltabdesc);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTableDescr::Pdxltabdesc
//
//	@doc:
//		Returns the table descriptor constructed by the parse handler
//
//---------------------------------------------------------------------------
CDXLTableDescr *
CParseHandlerTableDescr::Pdxltabdesc()
{
	return m_pdxltabdesc;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTableDescr::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerTableDescr::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenTableDescr), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// parse table name from attributes
	m_pdxltabdesc = CDXLOperatorFactory::Pdxltabdesc(m_pphm->Pmm(), attrs);
		
	// install column descriptor parsers
	CParseHandlerBase *pphColDescr = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenColumns), m_pphm, this);
	m_pphm->ActivateParseHandler(pphColDescr);
	
	// store parse handler
	this->Append(pphColDescr);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTableDescr::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerTableDescr::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenTableDescr), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// construct node from the created child nodes
	
	GPOS_ASSERT(1 == this->UlLength());
	
	// assemble the properties container from the cost
	CParseHandlerColDescr *pphColDescr = dynamic_cast<CParseHandlerColDescr *>((*this)[0]);
	
	GPOS_ASSERT(NULL != pphColDescr->Pdrgpdxlcd());
	
	DrgPdxlcd *pdrgpdxlcd = pphColDescr->Pdrgpdxlcd();
	pdrgpdxlcd->AddRef();
	m_pdxltabdesc->SetColumnDescriptors(pdrgpdxlcd);
			
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF

