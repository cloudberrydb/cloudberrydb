//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarConstValue.cpp
//
//	@doc:
//		
//		Implementation of the SAX parse handler class for parsing scalar ConstVal.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarConstValue.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarConstValue::CParseHandlerScalarConstValue
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerScalarConstValue::CParseHandlerScalarConstValue
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
//		CParseHandlerScalarConstValue::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarConstValue::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarConstValue), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// parse and create scalar const operator
	CDXLScalarConstValue *pdxlop = (CDXLScalarConstValue*) CDXLOperatorFactory::PdxlopConstValue(m_pphm->Pmm(), attrs);

	// construct scalar Const node
	GPOS_ASSERT(NULL != pdxlop);
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp);
	m_pdxln->SetOperator(pdxlop);

}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarConstValue::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarConstValue::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarConstValue), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
