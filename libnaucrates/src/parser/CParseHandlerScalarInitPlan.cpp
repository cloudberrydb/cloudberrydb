//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarInitPlan.cpp
//
//	@doc:
//		
//		Implementation of the SAX parse handler class for parsing scalar InitPlan.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

#include "naucrates/dxl/parser/CParseHandlerScalarInitPlan.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarInitPlan::CParseHandlerScalarInitPlan
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerScalarInitPlan::CParseHandlerScalarInitPlan
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
//		CParseHandlerScalarInitPlan::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarInitPlan::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname,
	const Attributes& //attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarInitPlan), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// parse and create InitPlan operator
	CDXLScalarInitPlan *pdxlop = (CDXLScalarInitPlan *) CDXLOperatorFactory::PdxlopInitPlan(m_pphm->Pmm());
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);

	// parse handler for child physical node
	CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenPhysical), m_pphm, this);
	m_pphm->ActivateParseHandler(pphChild);

	// store parse handlers
	this->Append(pphChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarInitPlan::EndElement
//
//	@doc:
//		Processes a Xerces end element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarInitPlan::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarInitPlan), xmlszLocalname) && NULL != m_pdxln)
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	CParseHandlerPhysicalOp *pphChild = dynamic_cast<CParseHandlerPhysicalOp *>((*this)[0]);

	// add constructed child
	AddChildFromParseHandler(pphChild);

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
