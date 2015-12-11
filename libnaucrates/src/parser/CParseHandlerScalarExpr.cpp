//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CParseHandlerScalarExpr.cpp
//
//	@doc:
//		@see CParseHandlerScalarExpr.h
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarExpr.h"

#include "naucrates/dxl/parser/CParseHandlerOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarExpr::CParseHandlerScalarExpr
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerScalarExpr::CParseHandlerScalarExpr
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pdxln(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarExpr::~CParseHandlerScalarExpr
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerScalarExpr::~CParseHandlerScalarExpr()
{
	CRefCount::SafeRelease(m_pdxln);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarExpr::Pdxln
//
//	@doc:
//		Root of constructed DXL expression
//
//---------------------------------------------------------------------------
CDXLNode *
CParseHandlerScalarExpr::Pdxln() const
{
	return m_pdxln;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarExpr::Edxlphtype
//
//	@doc:
//		Return the type of the parse handler.
//
//---------------------------------------------------------------------------
EDxlParseHandlerType
CParseHandlerScalarExpr::Edxlphtype() const
{
	return EdxlphScalarExpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarExpr::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag for a scalar expression.
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarExpr::StartElement
	(
	const XMLCh* const,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const,
	const Attributes &
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarExpr), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	GPOS_ASSERT(NULL != m_pmp);

	// parse handler for child node
	CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
	m_pphm->ActivateParseHandler(pphChild);
	Append(pphChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarExpr::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag.
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarExpr::EndElement
	(
	const XMLCh* const, //= xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname,
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarExpr), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	CParseHandlerScalarOp *pphChild = dynamic_cast<CParseHandlerScalarOp *>((*this)[0]);
	// extract constructed element
	GPOS_ASSERT(NULL != pphChild && NULL != pphChild->Pdxln());
	m_pdxln = pphChild->Pdxln();
	m_pdxln->AddRef();

	// deactivate handler
	m_pphm->DeactivateHandler();
}
