//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CParseHandlerScalarSubqueryQuantified.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for ANY and ALL subquery 
//		operators.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarSubqueryQuantified.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerLogicalOp.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSubqueryQuantified::CParseHandlerScalarSubqueryQuantified
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerScalarSubqueryQuantified::CParseHandlerScalarSubqueryQuantified
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerScalarOp(pmp, pphm, pphRoot),
	m_pdxlop(NULL)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSubqueryQuantified::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarSubqueryQuantified::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	
	GPOS_ASSERT(NULL == m_pdxlop);
		
	// is this a subquery any or subquery all operator
	Edxltoken edxltokenElement = EdxltokenScalarSubqueryAll;
	if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubqueryAny), xmlszLocalname))
	{
		edxltokenElement = EdxltokenScalarSubqueryAny;
	}
	else if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubqueryAll), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// parse operator id
	IMDId *pmdidOp = CDXLOperatorFactory::PmdidFromAttrs
							(
							m_pphm->Pmm(),
							attrs,
							EdxltokenOpNo,
							edxltokenElement
							);

	// parse operator name
	const XMLCh *xmlszScalarOpName = CDXLOperatorFactory::XmlstrFromAttrs
										(
										attrs,
										EdxltokenOpName,
										edxltokenElement
										);
	
	CWStringDynamic *pstrScalarOpName = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszScalarOpName);
	CMDName *pmdnameScalarOp = GPOS_NEW(m_pmp) CMDName(m_pmp, pstrScalarOpName);
	GPOS_DELETE(pstrScalarOpName);
		
	// parse column id
	ULONG ulColId = CDXLOperatorFactory::UlValueFromAttrs
										(
										m_pphm->Pmm(),
										attrs, 
										EdxltokenColId,
										edxltokenElement
										);
	
	if (EdxltokenScalarSubqueryAny == edxltokenElement)
	{
		m_pdxlop = GPOS_NEW(m_pmp) CDXLScalarSubqueryAny(m_pmp, pmdidOp, pmdnameScalarOp, ulColId);
	}
	else
	{
		m_pdxlop = GPOS_NEW(m_pmp) CDXLScalarSubqueryAll(m_pmp, pmdidOp, pmdnameScalarOp, ulColId);
	}
	
	// parse handler for the child nodes
	CParseHandlerBase *pphLgChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenLogical), m_pphm, this);
	m_pphm->ActivateParseHandler(pphLgChild);

	CParseHandlerBase *pphScChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
	m_pphm->ActivateParseHandler(pphScChild);

	// store child parse handler in array
	this->Append(pphScChild);
	this->Append(pphLgChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSubqueryQuantified::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarSubqueryQuantified::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubqueryAll), xmlszLocalname) &&
		0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubqueryAny), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// construct node from parsed components
	GPOS_ASSERT(NULL != m_pdxlop);
	GPOS_ASSERT(2 == this->UlLength());
	
	CParseHandlerScalarOp *pphScChild = dynamic_cast<CParseHandlerScalarOp *>((*this)[0]);
	CParseHandlerLogicalOp *pphLgChild = dynamic_cast<CParseHandlerLogicalOp *>((*this)[1]);
		
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, m_pdxlop);

	// add constructed child
	AddChildFromParseHandler(pphScChild);
	AddChildFromParseHandler(pphLgChild);

#ifdef GPOS_DEBUG
	m_pdxlop->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG
	
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF

