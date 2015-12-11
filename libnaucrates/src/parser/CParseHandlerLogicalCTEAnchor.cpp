//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerLogicalCTEAnchor.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing CTE anchor
//		operators
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/operators/CDXLLogicalCTEAnchor.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerLogicalCTEAnchor.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalCTEAnchor::CParseHandlerLogicalCTEAnchor
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerLogicalCTEAnchor::CParseHandlerLogicalCTEAnchor
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerLogicalOp(pmp, pphm, pphRoot)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalCTEAnchor::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerLogicalCTEAnchor::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalCTEAnchor), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// parse cteid and create cte operator
	ULONG ulId = CDXLOperatorFactory::UlValueFromAttrs
											(
											m_pphm->Pmm(),
											attrs,
											EdxltokenCTEId,
											EdxltokenLogicalCTEAnchor
											);
	
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLLogicalCTEAnchor(m_pmp, ulId));

	// create and activate the parse handler for the child expression node
	CParseHandlerBase *pphChild =
			CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenLogical), m_pphm, this);
	m_pphm->ActivateParseHandler(pphChild);

	// store parse handler
	this->Append(pphChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalCTEAnchor::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerLogicalCTEAnchor::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalCTEAnchor), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(NULL != m_pdxln );

	CParseHandlerLogicalOp *pphChild = dynamic_cast<CParseHandlerLogicalOp*>((*this)[0]);
	AddChildFromParseHandler(pphChild);

#ifdef GPOS_DEBUG
		m_pdxln->Pdxlop()->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG

	m_pphm->DeactivateHandler();
}

// EOF
