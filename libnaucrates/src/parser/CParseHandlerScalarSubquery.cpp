//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CParseHandlerScalarSubquery.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing scalar subquery 
//		operators.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarSubquery.h"
#include "naucrates/dxl/parser/CParseHandlerLogicalOp.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSubquery::CParseHandlerScalarSubquery
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerScalarSubquery::CParseHandlerScalarSubquery
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
//		CParseHandlerScalarSubquery::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarSubquery::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubquery), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
		
	// parse column id
	ULONG ulColId = CDXLOperatorFactory::UlValueFromAttrs
										(
										m_pphm->Pmm(),
										attrs, 
										EdxltokenColId,
										EdxltokenScalarSubquery
										);
	m_pdxlop = GPOS_NEW(m_pmp) CDXLScalarSubquery(m_pmp, ulColId);

	// parse handler for child node
	CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenLogical), m_pphm, this);
	m_pphm->ActivateParseHandler(pphChild);
		
	// store child parse handler in array
	this->Append(pphChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSubquery::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarSubquery::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubquery), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// construct node from parsed components
	GPOS_ASSERT(1 == this->UlLength());
	GPOS_ASSERT(NULL != m_pdxlop);
	
	CParseHandlerLogicalOp *pphChild = dynamic_cast<CParseHandlerLogicalOp *>((*this)[0]);

	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, m_pdxlop);	

	// add constructed child
	AddChildFromParseHandler(pphChild);

#ifdef GPOS_DEBUG
	m_pdxlop->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG
	
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF

