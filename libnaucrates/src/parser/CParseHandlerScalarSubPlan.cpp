//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerScalarSubPlan.cpp
//
//	@doc:
//		
//		Implementation of the SAX parse handler class for parsing scalar SubPlan
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerScalarSubPlan.h"
#include "naucrates/dxl/parser/CParseHandlerScalarSubPlanParamList.h"
#include "naucrates/dxl/parser/CParseHandlerScalarSubPlanTestExpr.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/operators/CDXLScalarSubPlan.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSubPlan::CParseHandlerScalarSubPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerScalarSubPlan::CParseHandlerScalarSubPlan
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerScalarOp(pmp, pphm, pphRoot),
	m_pmdidFirstCol(NULL),
	m_edxlsubplantype(EdxlSubPlanTypeSentinel)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSubPlan::Edxlsubplantype
//
//	@doc:
//		Map character sequence to subplan type
//
//---------------------------------------------------------------------------
EdxlSubPlanType
CParseHandlerScalarSubPlan::Edxlsubplantype
	(
	const XMLCh *xmlszSubplanType
	)
{
	GPOS_ASSERT(NULL != xmlszSubplanType);

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubPlanTypeScalar), xmlszSubplanType))
	{
		return EdxlSubPlanTypeScalar;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubPlanTypeExists), xmlszSubplanType))
	{
		return EdxlSubPlanTypeExists;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubPlanTypeNotExists), xmlszSubplanType))
	{
		return EdxlSubPlanTypeNotExists;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubPlanTypeAny), xmlszSubplanType))
	{
		return EdxlSubPlanTypeAny;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubPlanTypeAll), xmlszSubplanType))
	{
		return EdxlSubPlanTypeAll;
	}

	// turn Xerces exception in optimizer exception
	GPOS_RAISE
		(
		gpdxl::ExmaDXL,
		gpdxl::ExmiDXLInvalidAttributeValue,
		CDXLTokens::PstrToken(EdxltokenScalarSubPlanType)->Wsz(),
		CDXLTokens::PstrToken(EdxltokenScalarSubPlan)->Wsz()
		);

	return EdxlSubPlanTypeSentinel;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSubPlan::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarSubPlan::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname,
	const Attributes &attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubPlan), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	m_pmdidFirstCol = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenTypeId, EdxltokenScalarSubPlanParam);

	const XMLCh *xmlszSubplanType  = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenScalarSubPlanType, EdxltokenScalarSubPlan);
	m_edxlsubplantype = Edxlsubplantype(xmlszSubplanType);

	// parse handler for child physical node
	CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenPhysical), m_pphm, this);
	m_pphm->ActivateParseHandler(pphChild);

	// parse handler for params
	CParseHandlerBase *pphParamList = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarSubPlanParamList), m_pphm, this);
	m_pphm->ActivateParseHandler(pphParamList);

	// parse handler for test expression
	CParseHandlerBase *pphTestExpr = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarSubPlanTestExpr), m_pphm, this);
	m_pphm->ActivateParseHandler(pphTestExpr);

	// store parse handlers
	this->Append(pphTestExpr);
	this->Append(pphParamList);
	this->Append(pphChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSubPlan::EndElement
//
//	@doc:
//		Processes a Xerces end element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarSubPlan::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubPlan), xmlszLocalname) && NULL != m_pdxln)
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	CParseHandlerScalarSubPlanTestExpr *pphTestExpr = dynamic_cast<CParseHandlerScalarSubPlanTestExpr *>((*this)[0]);
	CParseHandlerScalarSubPlanParamList *pphParamList = dynamic_cast<CParseHandlerScalarSubPlanParamList *>((*this)[1]);
	CParseHandlerPhysicalOp *pphChild = dynamic_cast<CParseHandlerPhysicalOp *>((*this)[2]);

	DrgPdxlcr *pdrgdxlcr = pphParamList->Pdrgdxlcr();
	pdrgdxlcr->AddRef();

	CDXLNode *pdxlnTestExpr = pphTestExpr->PdxlnTestExpr();
	if (NULL != pdxlnTestExpr)
	{
		pdxlnTestExpr->AddRef();
	}
	CDXLScalarSubPlan *pdxlop = (CDXLScalarSubPlan *) CDXLOperatorFactory::PdxlopSubPlan(m_pphm->Pmm(), m_pmdidFirstCol, pdrgdxlcr, m_edxlsubplantype, pdxlnTestExpr);

	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);

	// add constructed children
	AddChildFromParseHandler(pphChild);

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
