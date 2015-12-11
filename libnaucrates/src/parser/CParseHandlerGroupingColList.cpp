//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerGroupingColList.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing grouping column
//		id lists.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerGroupingColList.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerGroupingColList::CParseHandlerGroupingColList
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerGroupingColList::CParseHandlerGroupingColList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pdrgpulGroupingCols(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerGroupingColList::~CParseHandlerGroupingColList
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CParseHandlerGroupingColList::~CParseHandlerGroupingColList()
{
	CRefCount::SafeRelease(m_pdrgpulGroupingCols);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerGroupingColList::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerGroupingColList::StartElement
	(
	const XMLCh* const , //xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const , //xmlszQname,
	const Attributes& attrs
	)
{	
	if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarGroupingColList), xmlszLocalname))
	{
		// start the grouping column list
		m_pdrgpulGroupingCols = GPOS_NEW(m_pmp) DrgPul(m_pmp);
	}
	else if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGroupingCol), xmlszLocalname))
	{
		// we must have seen a grouping cols list already and initialized the grouping cols array
		GPOS_ASSERT(NULL != m_pdrgpulGroupingCols);
		
		// parse grouping col id
		ULONG *pulColId = GPOS_NEW(m_pmp) ULONG(CDXLOperatorFactory::UlGroupingColId(m_pphm->Pmm(), attrs));
		
		m_pdrgpulGroupingCols->Append(pulColId);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerGroupingColList::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerGroupingColList::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarGroupingColList), xmlszLocalname))
	{
		// deactivate handler
		m_pphm->DeactivateHandler();	
	}
	else if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGroupingCol), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerGroupingColList::PdrgpulGroupingCols
//
//	@doc:
//		Returns the array of parsed grouping column ids
//
//---------------------------------------------------------------------------
DrgPul *
CParseHandlerGroupingColList::PdrgpulGroupingCols()
{
	return m_pdrgpulGroupingCols;
}
// EOF
