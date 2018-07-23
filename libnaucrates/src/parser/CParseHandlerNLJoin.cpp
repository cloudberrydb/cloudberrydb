//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerNLJoin.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing nested loop join operators.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerNLJoin.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFilter.h"
#include "naucrates/dxl/parser/CParseHandlerProjList.h"
#include "naucrates/dxl/parser/CParseHandlerProperties.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerUtils.h"
#include "naucrates/dxl/parser/CParseHandlerNLJIndexParamList.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerNLJoin::CParseHandlerNLJoin
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerNLJoin::CParseHandlerNLJoin
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerPhysicalOp(pmp, pphm, pphRoot),
	m_pdxlop(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerNLJoin::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerNLJoin::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalNLJoin), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// parse and create nest loop join operator
	m_pdxlop = (CDXLPhysicalNLJoin *) CDXLOperatorFactory::PdxlopNLJoin(m_pphm->Pmm(), attrs);
	
	// create and activate the parse handler for the children nodes in reverse
	// order of their expected appearance

	// parse handler for the nest loop params list
	CParseHandlerBase *nest_params_parse_handler = NULL;
	if (m_pdxlop->NestParamsExists())
	{
		nest_params_parse_handler = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenNLJIndexParamList), m_pphm, this);
		m_pphm->ActivateParseHandler(nest_params_parse_handler);
	}

	// parse handler for right child
	CParseHandlerBase *pphRight = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenPhysical), m_pphm, this);
	m_pphm->ActivateParseHandler(pphRight);
	
	// parse handler for left child
	CParseHandlerBase *pphLeft = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenPhysical), m_pphm, this);
	m_pphm->ActivateParseHandler(pphLeft);

	// parse handler for the join filter
	CParseHandlerBase *pphJoinFilter = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarFilter), m_pphm, this);
	m_pphm->ActivateParseHandler(pphJoinFilter);

	// parse handler for the filter
	CParseHandlerBase *pphFilter = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarFilter), m_pphm, this);
	m_pphm->ActivateParseHandler(pphFilter);
	
	// parse handler for the proj list
	CParseHandlerBase *pphPrL = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarProjList), m_pphm, this);
	m_pphm->ActivateParseHandler(pphPrL);
	
	//parse handler for the properties of the operator
	CParseHandlerBase *pphProp = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenProperties), m_pphm, this);
	m_pphm->ActivateParseHandler(pphProp);
	
	// store parse handlers
	this->Append(pphProp);
	this->Append(pphPrL);
	this->Append(pphFilter);
	this->Append(pphJoinFilter);
	this->Append(pphLeft);
	this->Append(pphRight);
	if (NULL != nest_params_parse_handler)
	{
		this->Append(nest_params_parse_handler);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerNLJoin::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerNLJoin::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalNLJoin), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLUnexpectedTag,
			pstr->Wsz()
			);
	}
	
	// construct node from the created child nodes
	CParseHandlerProperties *prop_parse_handler = dynamic_cast<CParseHandlerProperties *>((*this)[EdxlParseHandlerNLJIndexProp]);
	CParseHandlerProjList *proj_list_parse_handler = dynamic_cast<CParseHandlerProjList*>((*this)[EdxlParseHandlerNLJIndexProjList]);
	CParseHandlerFilter *filter_parse_handler = dynamic_cast<CParseHandlerFilter *>((*this)[EdxlParseHandlerNLJIndexFilter]);
	CParseHandlerFilter *join_filter_parse_handler = dynamic_cast<CParseHandlerFilter *>((*this)[EdxlParseHandlerNLJIndexJoinFilter]);
	CParseHandlerPhysicalOp *left_child_parse_handler = dynamic_cast<CParseHandlerPhysicalOp *>((*this)[EdxlParseHandlerNLJIndexLeftChild]);
	CParseHandlerPhysicalOp *right_child_parse_handler = dynamic_cast<CParseHandlerPhysicalOp *>((*this)[EdxlParseHandlerNLJIndexRightChild]);

	if (m_pdxlop->NestParamsExists())
	{
		CParseHandlerNLJIndexParamList *nest_params_parse_handler = dynamic_cast<CParseHandlerNLJIndexParamList*>((*this)[EdxlParseHandlerNLJIndexNestLoopParams]);
		GPOS_ASSERT(nest_params_parse_handler);
		DrgPdxlcr *nest_params_colrefs = nest_params_parse_handler->GetNLParamsColRefs();
		nest_params_colrefs->AddRef();
		m_pdxlop->SetNestLoopParamsColRefs(nest_params_colrefs);
	}

	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, m_pdxlop);
	// set statictics and physical properties
	CParseHandlerUtils::SetProperties(m_pdxln, prop_parse_handler);
	
	// add constructed children
	AddChildFromParseHandler(proj_list_parse_handler);
	AddChildFromParseHandler(filter_parse_handler);
	AddChildFromParseHandler(join_filter_parse_handler);
	AddChildFromParseHandler(left_child_parse_handler);
	AddChildFromParseHandler(right_child_parse_handler);
	
#ifdef GPOS_DEBUG
	m_pdxlop->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
