//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerLogicalSelect.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing logical
//		select operators.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerLogicalSelect.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalSelect::CParseHandlerLogicalSelect
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerLogicalSelect::CParseHandlerLogicalSelect
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
//		CParseHandlerLogicalSelect::~CParseHandlerLogicalSelect
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CParseHandlerLogicalSelect::~CParseHandlerLogicalSelect()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalSelect::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerLogicalSelect::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& //attrs
	)
{
	if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalSelect), xmlszLocalname))
	{
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLLogicalSelect(m_pmp));

		// create child node parsers

		// parse handler for logical operator
		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenLogical), m_pphm, this);
		m_pphm->ActivateParseHandler(pphChild);

		// parse handler for the scalar condition
		CParseHandlerBase *pphOp = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
		m_pphm->ActivateParseHandler(pphOp);

		// store child parse handler in array
		this->Append(pphOp);
		this->Append(pphChild);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalSelect::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerLogicalSelect::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalSelect), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(NULL != m_pdxln );

	CParseHandlerScalarOp *pphOp = dynamic_cast<CParseHandlerScalarOp*>((*this)[0]);
	CParseHandlerLogicalOp *pphChild = dynamic_cast<CParseHandlerLogicalOp*>((*this)[1]);

	AddChildFromParseHandler(pphOp);
	AddChildFromParseHandler(pphChild);
	
#ifdef GPOS_DEBUG
	m_pdxln->Pdxlop()->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG

	// deactivate handler
	m_pphm->DeactivateHandler();
}
// EOF
