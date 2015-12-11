//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerSortColList.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing sorting column lists.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerSortColList.h"
#include "naucrates/dxl/parser/CParseHandlerSortCol.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerSortColList::CParseHandlerSortColList
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerSortColList::CParseHandlerSortColList
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
//		CParseHandlerSortColList::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerSortColList::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSortColList), xmlszLocalname))
	{
		// start the sorting column list
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode (m_pmp, GPOS_NEW(m_pmp) CDXLScalarSortColList(m_pmp));
	}
	else if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSortCol), xmlszLocalname))
	{
		// we must have seen a sorting col list already and initialized the sort col list node
		GPOS_ASSERT(NULL != m_pdxln);

		// start new sort column
		CParseHandlerBase *pphSortCol = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarSortCol), m_pphm, this);
		m_pphm->ActivateParseHandler(pphSortCol);
		
		// store parse handler
		this->Append(pphSortCol);
		
		pphSortCol->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerSortColList::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerSortColList::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSortColList), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLUnexpectedTag,
			pstr->Wsz()
			);
	}
	
	const ULONG ulLen = this->UlLength();
	// add sorting columns from child parse handlers
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CParseHandlerSortCol *pphSortCol = dynamic_cast<CParseHandlerSortCol *>((*this)[ul]);
		AddChildFromParseHandler(pphSortCol);
	}
	
#ifdef GPOS_DEBUG
	m_pdxln->Pdxlop()->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG
		
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
