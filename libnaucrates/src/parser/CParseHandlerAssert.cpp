//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerAssert.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing physical 
//		assert operators
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerAssert.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerScalarAssertConstraintList.h"
#include "naucrates/dxl/parser/CParseHandlerProjList.h"
#include "naucrates/dxl/parser/CParseHandlerProperties.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerUtils.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerAssert::CParseHandlerAssert
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerAssert::CParseHandlerAssert
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerPhysicalOp(pmp, pphm, pphRoot)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerAssert::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerAssert::StartElement
	(
	const XMLCh* const , // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const , // xmlszQname,
	const Attributes& attrs
	)
{	
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalAssert), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
		
	CHAR *szErrorCode = CDXLOperatorFactory::SzValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenErrorCode, EdxltokenPhysicalAssert);
	if (NULL == szErrorCode || GPOS_SQLSTATE_LENGTH != clib::UlStrLen(szErrorCode))
	{
		GPOS_RAISE
			(
			gpdxl::ExmaDXL, 
			gpdxl::ExmiDXLInvalidAttributeValue, 
			CDXLTokens::PstrToken(EdxltokenPhysicalAssert)->Wsz(),
			CDXLTokens::PstrToken(EdxltokenErrorCode)->Wsz()
			);
	}
	
	m_pdxlop = GPOS_NEW(m_pmp) CDXLPhysicalAssert(m_pmp, szErrorCode);

	// ctor created a copy of the error code
	GPOS_DELETE_ARRAY(szErrorCode);
	
	// parse handler for child node
	CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenPhysical), m_pphm, this);
	m_pphm->ActivateParseHandler(pphChild);

	// parse handler for the predicate
	CParseHandlerBase *pphAssertPredicate = CParseHandlerFactory::Pph
											(
											m_pmp, 
											CDXLTokens::XmlstrToken(EdxltokenScalarAssertConstraintList), 
											m_pphm, 
											this
											);
	m_pphm->ActivateParseHandler(pphAssertPredicate);

	// parse handler for the proj list
	CParseHandlerBase *pphPrL = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarProjList), m_pphm, this);
	m_pphm->ActivateParseHandler(pphPrL);

	//parse handler for the properties of the operator
	CParseHandlerBase *pphProp = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenProperties), m_pphm, this);
	m_pphm->ActivateParseHandler(pphProp);

	this->Append(pphProp);
	this->Append(pphPrL);
	this->Append(pphAssertPredicate);
	this->Append(pphChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerAssert::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerAssert::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalAssert), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// construct node from the created child nodes
	CParseHandlerProperties *pphProp = dynamic_cast<CParseHandlerProperties *>((*this)[0]);
	CParseHandlerProjList *pphPrL = dynamic_cast<CParseHandlerProjList*>((*this)[1]);
	CParseHandlerScalarAssertConstraintList *pphAssertPredicate = dynamic_cast<CParseHandlerScalarAssertConstraintList *>((*this)[2]);
	CParseHandlerPhysicalOp *pphChild = dynamic_cast<CParseHandlerPhysicalOp*>((*this)[3]);

	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, m_pdxlop);
	CParseHandlerUtils::SetProperties(m_pdxln, pphProp);

	// add constructed children
	AddChildFromParseHandler(pphPrL);
	AddChildFromParseHandler(pphAssertPredicate);
	AddChildFromParseHandler(pphChild);
	
#ifdef GPOS_DEBUG
	m_pdxlop->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
