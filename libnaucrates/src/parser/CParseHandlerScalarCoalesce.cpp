//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerScalarCoalesce.cpp
//
//	@doc:
//
//		Implementation of the SAX parse handler class for a coalesce operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------


#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerScalarCoalesce.h"


using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarCoalesce::CParseHandlerScalarCoalesce
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerScalarCoalesce::CParseHandlerScalarCoalesce
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerScalarOp(pmp, pphm, pphRoot),
	m_pmdidType(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarCoalesce::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarCoalesce::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarCoalesce), xmlszLocalname) && NULL == m_pmdidType)
	{
		// parse type id
		m_pmdidType = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenTypeId, EdxltokenScalarCoalesce);
	}
	else
	{
		// parse scalar child
		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
		m_pphm->ActivateParseHandler(pphChild);

		// store parse handlers
		this->Append(pphChild);

		pphChild->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarCoalesce::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarCoalesce::EndElement
	(
	const XMLCh* const ,// xmlszUri
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarCoalesce), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// construct node
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarCoalesce(m_pmp, m_pmdidType));

	// loop over children and add them to this parsehandler
	const ULONG ulChildren = this->UlLength();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CParseHandlerScalarOp *pphChild = dynamic_cast<CParseHandlerScalarOp *>((*this)[ul]);
		AddChildFromParseHandler(pphChild);
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

//EOF
