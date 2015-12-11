//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Inc.
//
//	@filename:
//		CParseHandlerScalarMinMax.cpp
//
//	@doc:
//
//		Implementation of the SAX parse handler class for a MinMax operator
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
#include "naucrates/dxl/parser/CParseHandlerScalarMinMax.h"


using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarMinMax::CParseHandlerScalarMinMax
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerScalarMinMax::CParseHandlerScalarMinMax
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerScalarOp(pmp, pphm, pphRoot),
	m_pmdidType(NULL),
	m_emmt(CDXLScalarMinMax::EmmtSentinel)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarMinMax::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarMinMax::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if (((0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarMin), xmlszLocalname)) ||
		(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarMax), xmlszLocalname))) &&
		CDXLScalarMinMax::EmmtSentinel == m_emmt)
	{
		m_emmt = Emmt(xmlszLocalname);
		GPOS_ASSERT(CDXLScalarMinMax::EmmtSentinel != m_emmt);

		Edxltoken edxltoken = EdxltokenScalarMin;
		if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarMax), xmlszLocalname))
		{
			edxltoken = EdxltokenScalarMax;
		}

		// parse type id
		m_pmdidType = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenTypeId, edxltoken);
	}
	else
	{
		// parse child
		CParseHandlerBase *pphOp = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
		m_pphm->ActivateParseHandler(pphOp);

		// store parse handlers
		this->Append(pphOp);

		pphOp->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarMinMax::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarMinMax::EndElement
	(
	const XMLCh* const ,// xmlszUri
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	CDXLScalarMinMax::EdxlMinMaxType emmt = Emmt(xmlszLocalname);

	if (CDXLScalarMinMax::EmmtSentinel == emmt || m_emmt != emmt)
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// construct node
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarMinMax(m_pmp, m_pmdidType, m_emmt));

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

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarMinMax::Emmt
//
//	@doc:
//		Parse the min/max type from the attribute value
//
//---------------------------------------------------------------------------
CDXLScalarMinMax::EdxlMinMaxType
CParseHandlerScalarMinMax::Emmt
	(
	const XMLCh *xmlszLocalname
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarMin), xmlszLocalname))
	{
		return CDXLScalarMinMax::EmmtMin;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarMax), xmlszLocalname))
	{
		return CDXLScalarMinMax::EmmtMax;
	}

	return CDXLScalarMinMax::EmmtSentinel;
}


//EOF
