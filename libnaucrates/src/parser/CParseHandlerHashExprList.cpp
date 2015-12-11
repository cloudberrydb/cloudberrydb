//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerHashExprList.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing hash expression lists.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerHashExprList.h"
#include "naucrates/dxl/parser/CParseHandlerHashExpr.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/operators/CDXLScalarProjList.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerHashExprList::CParseHandlerHashExprList
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerHashExprList::CParseHandlerHashExprList
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
//		CParseHandlerHashExprList::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerHashExprList::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarHashExprList), xmlszLocalname))
	{
		// start the hash expr list
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode (m_pmp, GPOS_NEW(m_pmp) CDXLScalarHashExprList(m_pmp));
	}
	else if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarHashExpr), xmlszLocalname))
	{
		// we must have seen a hash expr list already and initialized the hash expr list node
		GPOS_ASSERT(NULL != m_pdxln);
		// start new hash expr element
		CParseHandlerBase *pphHashExpr = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarHashExpr), m_pphm, this);
		m_pphm->ActivateParseHandler(pphHashExpr);
		
		// store parse handler
		this->Append(pphHashExpr);
		
		pphHashExpr->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerHashExprList::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerHashExprList::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarHashExprList), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	const ULONG ulLen = this->UlLength();
	// add hash expressions from child parse handlers
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CParseHandlerHashExpr *pphHashExpr = dynamic_cast<CParseHandlerHashExpr *>((*this)[ul]);
		
		AddChildFromParseHandler(pphHashExpr);
	}
		
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
