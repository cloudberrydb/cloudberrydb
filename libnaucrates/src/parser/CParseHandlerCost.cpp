//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerCost.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing cost estimates.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerCost.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCost::CParseHandlerCost
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerCost::CParseHandlerCost
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pdxlopcost(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCost::~CParseHandlerCost
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CParseHandlerCost::~CParseHandlerCost()
{
	CRefCount::SafeRelease(m_pdxlopcost);	
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCost::Pdxlopcost
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLOperatorCost *
CParseHandlerCost::Pdxlopcost()
{
	return m_pdxlopcost;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCost::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerCost::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCost), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// get cost estimates from attributes
	m_pdxlopcost = CDXLOperatorFactory::Pdxlopcost(m_pphm->Pmm(), attrs);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCost::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerCost::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCost), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
