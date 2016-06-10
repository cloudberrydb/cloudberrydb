//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerStatsBound.cpp
//
//	@doc:
//		
//		Implementation of the SAX parse handler class for parsing
//	    the bounds of the bucket
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerStatsBound.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatsBound::CParseHandlerStatsBound
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerStatsBound::CParseHandlerStatsBound
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pdxldatum(NULL),
	m_fStatsBoundClosed(false)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatsBound::~CParseHandlerStatsBound
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerStatsBound::~CParseHandlerStatsBound()
{
	m_pdxldatum->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatsBound::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerStatsBound::StartElement
	(
	const XMLCh* const,// xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const,// xmlszQname,
	const Attributes& attrs
	)
{
	if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenStatsBucketLowerBound), xmlszLocalname)
	   || 0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenStatsBucketUpperBound), xmlszLocalname))
	{
		GPOS_ASSERT(NULL == m_pdxldatum);

		// translate the datum and add it to the datum array
		CDXLDatum *pdxldatum = CDXLOperatorFactory::Pdxldatum(m_pphm->Pmm(), attrs, EdxltokenDatum);
		m_pdxldatum = pdxldatum;

		if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenStatsBucketLowerBound), xmlszLocalname))
		{
			m_fStatsBoundClosed = CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenStatsBoundClosed, EdxltokenStatsBucketLowerBound);
		}
		else
		{
			m_fStatsBoundClosed = CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenStatsBoundClosed, EdxltokenStatsBucketUpperBound);
		}
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatsBound::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerStatsBound::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenStatsBucketLowerBound), xmlszLocalname)
	   && 0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenStatsBucketUpperBound), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(NULL != m_pdxldatum);

	// deactivate handler
  	m_pphm->DeactivateHandler();
}

// EOF
