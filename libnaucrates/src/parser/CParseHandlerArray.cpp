//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerArray.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing arrays
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerArray.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerArray::CParseHandlerArray
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerArray::CParseHandlerArray
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
//		CParseHandlerArray::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerArray::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{	
	if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarArray), xmlszLocalname) &&
		NULL == m_pdxln)
	{
		// parse and create array
		CDXLScalarArray *pdxlop = (CDXLScalarArray *) CDXLOperatorFactory::PdxlopArray(m_pphm->Pmm(), attrs);
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);
	}
	else
	{
		// parse child of array
		GPOS_ASSERT(NULL != m_pdxln);
		
		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
		m_pphm->ActivateParseHandler(pphChild);
		this->Append(pphChild);
		pphChild->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerArray::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerArray::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarArray), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// construct node from the created child nodes
	
	GPOS_ASSERT(0 < this->UlLength());
	
	for (ULONG ul = 0; ul < this->UlLength(); ul++)
	{
		CParseHandlerScalarOp *pphChild = dynamic_cast<CParseHandlerScalarOp*>((*this)[ul]);
		GPOS_ASSERT(NULL != pphChild);
		AddChildFromParseHandler(pphChild);
	}
	
#ifdef GPOS_DEBUG
	m_pdxln->Pdxlop()->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG
	
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
