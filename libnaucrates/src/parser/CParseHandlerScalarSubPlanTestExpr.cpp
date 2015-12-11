//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CParseHandlerScalarSubPlanTestExpr.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing subplan test expression
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

#include "naucrates/dxl/parser/CParseHandlerScalarSubPlanTestExpr.h"
using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSubPlanTestExpr::CParseHandlerScalarSubPlanTestExpr
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerScalarSubPlanTestExpr::CParseHandlerScalarSubPlanTestExpr
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerScalarOp(pmp, pphm, pphRoot),
	m_pdxlnTestExpr(NULL)
{}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSubPlanTestExpr::~CParseHandlerScalarSubPlanTestExpr
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerScalarSubPlanTestExpr::~CParseHandlerScalarSubPlanTestExpr()
{
	CRefCount::SafeRelease(m_pdxlnTestExpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSubPlanTestExpr::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarSubPlanTestExpr::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubPlanTestExpr), xmlszLocalname))
	{
		// install a scalar element parser for parsing the test expression
		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);

		m_pphm->ActivateParseHandler(pphChild);

		// store parse handler
		this->Append(pphChild);

		pphChild->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSubPlanTestExpr::EndElement
//
//	@doc:
//		Processes a Xerces end element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarSubPlanTestExpr::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubPlanTestExpr), xmlszLocalname) && NULL != m_pdxlnTestExpr)
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	if (0 < this->UlLength())
	{
		CParseHandlerScalarOp *pphChild = dynamic_cast<CParseHandlerScalarOp *>((*this)[0]);
		if (NULL != pphChild->Pdxln())
		{
			m_pdxlnTestExpr = pphChild->Pdxln();
			m_pdxlnTestExpr->AddRef();
		}
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF

