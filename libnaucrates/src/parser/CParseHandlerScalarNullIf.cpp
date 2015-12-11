//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerScalarNullIf.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for scalar NullIf
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

#include "naucrates/dxl/parser/CParseHandlerScalarNullIf.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarNullIf::CParseHandlerScalarNullIf
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerScalarNullIf::CParseHandlerScalarNullIf
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
//		CParseHandlerScalarNullIf::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarNullIf::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarNullIf), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	IMDId *pmdidOp = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenOpNo, EdxltokenScalarNullIf);
	IMDId *pmdidRetType = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenTypeId, EdxltokenScalarNullIf);

	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarNullIf(m_pmp, pmdidOp, pmdidRetType));

	// create and activate the parse handler for the children nodes in reverse
	// order of their expected appearance

	// parse handler for right scalar node
	CParseHandlerBase *pphRight = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
	m_pphm->ActivateParseHandler(pphRight);

	// parse handler for left scalar node
	CParseHandlerBase *pphLeft = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
	m_pphm->ActivateParseHandler(pphLeft);

	// store parse handlers
	this->Append(pphLeft);
	this->Append(pphRight);

}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarNullIf::EndElement
//
//	@doc:
//		Processes a Xerces end element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarNullIf::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarNullIf), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	CParseHandlerScalarOp *pphLeft = dynamic_cast<CParseHandlerScalarOp *>((*this)[0]);
	CParseHandlerScalarOp *pphRight = dynamic_cast<CParseHandlerScalarOp *>((*this)[1]);

	// add constructed children
	AddChildFromParseHandler(pphLeft);
	AddChildFromParseHandler(pphRight);

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF

