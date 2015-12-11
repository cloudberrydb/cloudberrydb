//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerCondList.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing condition lists.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerCondList.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"


#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCondList::CParseHandlerCondList
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerCondList::CParseHandlerCondList
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
//		CParseHandlerCondList::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerCondList::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarHashCondList), xmlszLocalname))
	{
		// start the hash cond list
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode (m_pmp, GPOS_NEW(m_pmp) CDXLScalarHashCondList(m_pmp));
	}
	else if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarMergeCondList), xmlszLocalname))
	{
		// start the merge cond list
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode (m_pmp, GPOS_NEW(m_pmp) CDXLScalarMergeCondList(m_pmp));
	}
	else
	{
		// we must have seen a cond list already and initialized the cond list node
		GPOS_ASSERT(NULL != m_pdxln);
		// start new hash cond element
		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
		m_pphm->ActivateParseHandler(pphChild);
		
		// store parse handler
		this->Append(pphChild);
		
		pphChild->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCondList::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerCondList::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarHashCondList), xmlszLocalname) &&
			0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarMergeCondList), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	const ULONG ulLen = this->UlLength();
	// add conditions from child parse handlers
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CParseHandlerScalarOp *pphChild = dynamic_cast<CParseHandlerScalarOp *>((*this)[ul]);
		
		AddChildFromParseHandler(pphChild);
	}
		
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
