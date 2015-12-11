//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerDistinctComp.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing distinct
//		comparison operators.
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

#include "naucrates/dxl/parser/CParseHandlerDistinctComp.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDistinctComp::CParseHandlerDistinctComp
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerDistinctComp::CParseHandlerDistinctComp
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
//		CParseHandlerDistinctComp::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerDistinctComp::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarDistinctComp), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// parse and create distinct operator
	m_pdxlop = (CDXLScalarDistinctComp *) CDXLOperatorFactory::PdxlopDistinctCmp(m_pphm->Pmm(), attrs);
	
	// create and activate the parse handler for the children nodes in reverse
	// order of their expected appearance
	
	// parse handler for right scalar node
	CParseHandlerBase *pphRight = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
	m_pphm->ActivateParseHandler(pphRight);
	
	// parse handler for left scalar node
	CParseHandlerBase *pphLeft = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
	m_pphm->ActivateParseHandler(pphLeft);
	
	// store parse handlers
	this->Append(pphLeft);
	this->Append(pphRight);

}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDistinctComp::EndElement
//
//	@doc:
//		Processes a Xerces end element event
//
//---------------------------------------------------------------------------
void
CParseHandlerDistinctComp::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarDistinctComp), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// construct node from the created child nodes
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, m_pdxlop);

	CParseHandlerScalarOp *pphLeft = dynamic_cast<CParseHandlerScalarOp *>((*this)[0]);
	CParseHandlerScalarOp *pphRight = dynamic_cast<CParseHandlerScalarOp *>((*this)[1]);

	// add constructed children
	AddChildFromParseHandler(pphLeft);
	AddChildFromParseHandler(pphRight);
	
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF

