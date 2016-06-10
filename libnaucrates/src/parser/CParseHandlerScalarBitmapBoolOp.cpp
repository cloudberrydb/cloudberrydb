//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CParseHandlerScalarBitmapBoolOp.cpp
//
//	@doc:
//		
//		Implementation of the SAX parse handler class for parsing scalar bitmap
//		bool op
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

#include "naucrates/dxl/parser/CParseHandlerScalarBitmapBoolOp.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarBitmapBoolOp::CParseHandlerScalarBitmapBoolOp
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerScalarBitmapBoolOp::CParseHandlerScalarBitmapBoolOp
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
//		CParseHandlerScalarBitmapBoolOp::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarBitmapBoolOp::StartElement
	(
	const XMLCh* const , // xmlszUri
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	CDXLScalarBitmapBoolOp::EdxlBitmapBoolOp edxlbitmapboolop = CDXLScalarBitmapBoolOp::EdxlbitmapAnd;
	Edxltoken edxltoken = EdxltokenScalarBitmapAnd;
	
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarBitmapOr), xmlszLocalname))
	{
		edxlbitmapboolop = CDXLScalarBitmapBoolOp::EdxlbitmapOr;
		edxltoken = EdxltokenScalarBitmapOr;
	}
	else if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarBitmapAnd), xmlszLocalname))
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname)->Wsz());
	}
	
	IMDId *pmdid = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenTypeId, edxltoken);
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarBitmapBoolOp(m_pmp, pmdid, edxlbitmapboolop));
	
	// install parse handlers for children
	CParseHandlerBase *pphRight = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
	m_pphm->ActivateParseHandler(pphRight);
	
	CParseHandlerBase *pphLeft = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
	m_pphm->ActivateParseHandler(pphLeft);

	this->Append(pphLeft);
	this->Append(pphRight);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarBitmapBoolOp::EndElement
//
//	@doc:
//		Processes a Xerces end element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarBitmapBoolOp::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarBitmapOr), xmlszLocalname) &&
		0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarBitmapAnd), xmlszLocalname))
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname)->Wsz());
	}

	const ULONG ulSize = this->UlLength();
	GPOS_ASSERT(2 == ulSize);

	// add constructed children from child parse handlers
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CParseHandlerOp *pph = dynamic_cast<CParseHandlerOp*>((*this)[ul]);
		AddChildFromParseHandler(pph);
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
