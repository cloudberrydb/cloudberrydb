//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerScalarSwitch.cpp
//
//	@doc:
//
//		Implementation of the SAX parse handler class for a Switch operator
//---------------------------------------------------------------------------


#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerScalarSwitch.h"
#include "naucrates/dxl/parser/CParseHandlerScalarSwitchCase.h"


using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSwitch::CParseHandlerScalarSwitch
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerScalarSwitch::CParseHandlerScalarSwitch
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerScalarOp(pmp, pphm, pphRoot),
	m_pmdidType(NULL),
	m_fArgProcessed(false),
	m_fDefaultProcessed(false)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSwitch::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarSwitch::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSwitch), xmlszLocalname) && NULL == m_pmdidType)
	{
		// parse type id
		m_pmdidType = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenTypeId, EdxltokenScalarSwitch);

		// construct node
		CDXLScalarSwitch *pdxlop =  GPOS_NEW(m_pmp) CDXLScalarSwitch(m_pmp, m_pmdidType);
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSwitchCase), xmlszLocalname))
	{
		// we must have already seen the arg child, but have not seen the DEFAULT child
		GPOS_ASSERT(NULL != m_pdxln && m_fArgProcessed && !m_fDefaultProcessed);

		// parse case
		CParseHandlerBase *pphCase = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarSwitchCase), m_pphm, this);
		m_pphm->ActivateParseHandler(pphCase);

		// store parse handlers
		this->Append(pphCase);

		pphCase->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
	else
	{
		GPOS_ASSERT(NULL != m_pdxln && !m_fDefaultProcessed);

		// parse scalar child
		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
		m_pphm->ActivateParseHandler(pphChild);

		// store parse handlers
		this->Append(pphChild);

		pphChild->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);

		if (!m_fArgProcessed)
		{
			// this child was the arg child
			m_fArgProcessed = true;
		}
		else
		{
			// that was the default expr child
			m_fDefaultProcessed = true;
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSwitch::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarSwitch::EndElement
	(
	const XMLCh* const ,// xmlszUri
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSwitch), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	const ULONG ulChildren = this->UlLength();
	GPOS_ASSERT(1 < ulChildren);

	for (ULONG ul = 0; ul < ulChildren ; ul++)
	{
		CParseHandlerScalarOp *pphChild = dynamic_cast<CParseHandlerScalarOp *>((*this)[ul]);
		AddChildFromParseHandler(pphChild);
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

//EOF
