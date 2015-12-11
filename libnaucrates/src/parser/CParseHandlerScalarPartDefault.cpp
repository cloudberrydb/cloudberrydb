//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CParseHandlerScalarPartDefault.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing part default
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarPartDefault.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/operators/CDXLScalarPartDefault.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarPartDefault::CParseHandlerScalarPartDefault
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerScalarPartDefault::CParseHandlerScalarPartDefault
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerScalarOp(pmp, pphm, pphRoot)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarPartDefault::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarPartDefault::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname,
	const Attributes& attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarPartDefault), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	ULONG ulLevel = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenPartLevel, EdxltokenScalarPartDefault);
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode (m_pmp, GPOS_NEW(m_pmp) CDXLScalarPartDefault(m_pmp, ulLevel));
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarPartDefault::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarPartDefault::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarPartDefault), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(NULL != m_pdxln);

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
