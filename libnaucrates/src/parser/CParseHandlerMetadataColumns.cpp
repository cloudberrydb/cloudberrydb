//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerMetadataColumns.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing a list of
//		columns in a relation's metadata.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerMetadataColumns.h"

#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataColumn.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMetadataColumns::CParseHandlerMetadataColumns
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerMetadataColumns::CParseHandlerMetadataColumns
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pdrgpmdcol(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMetadataColumns::~CParseHandlerMetadataColumns
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CParseHandlerMetadataColumns::~CParseHandlerMetadataColumns()
{
	CRefCount::SafeRelease(m_pdrgpmdcol);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMetadataColumns::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMetadataColumns::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenColumns), xmlszLocalname))
	{
		// start of a columns' list
		GPOS_ASSERT(NULL == m_pdrgpmdcol);
		
		m_pdrgpmdcol = GPOS_NEW(m_pmp) DrgPmdcol(m_pmp);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenColumn), xmlszLocalname))
	{
		// column list must be initialized already
		GPOS_ASSERT(NULL != m_pdrgpmdcol);
		
		// activate parse handler to parse the column info
		CParseHandlerBase *pphCol = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenMetadataColumn), m_pphm, this);
		
		m_pphm->ActivateParseHandler(pphCol);
		this->Append(pphCol);
		
		pphCol->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMetadataColumns::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMetadataColumns::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenColumns), xmlszLocalname))
	{
		// end of the columns' list
		GPOS_ASSERT(NULL != m_pdrgpmdcol);
		
		const ULONG ulSize = this->UlLength();
		// add parsed columns to the list
		for (ULONG ul = 0; ul < ulSize; ul++)
		{
			CParseHandlerMetadataColumn *pphCol = dynamic_cast<CParseHandlerMetadataColumn *>((*this)[ul]);
			
			GPOS_ASSERT(NULL != pphCol->Pmdcol());
			
			CMDColumn *pmdcol = pphCol->Pmdcol();
			pmdcol->AddRef();
			
			m_pdrgpmdcol->Append(pmdcol);
		}
		// deactivate handler
		m_pphm->DeactivateHandler();
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMetadataColumns::Pdrgpmdcol
//
//	@doc:
//		Return the constructed list of metadata columns
//
//---------------------------------------------------------------------------
DrgPmdcol *
CParseHandlerMetadataColumns::Pdrgpmdcol()
{
	return m_pdrgpmdcol;
}

// EOF
