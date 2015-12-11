//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CParseHandlerQueryOutput.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class parsing the list of
//		output column references in a DXL query.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerQueryOutput.h"
#include "naucrates/dxl/parser/CParseHandlerScalarIdent.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"



using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerQueryOutput::CParseHandlerQueryOutput
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerQueryOutput::CParseHandlerQueryOutput
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pdrgpdxln(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerQueryOutput::~CParseHandlerQueryOutput
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CParseHandlerQueryOutput::~CParseHandlerQueryOutput()
{
	m_pdrgpdxln->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerQueryOutput::PdrgpdxlnOutputCols
//
//	@doc:
//		Return the list of query output columns
//
//---------------------------------------------------------------------------
DrgPdxln *
CParseHandlerQueryOutput::PdrgpdxlnOutputCols()
{
	GPOS_ASSERT(NULL != m_pdrgpdxln);
	return m_pdrgpdxln;
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerQueryOutput::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerQueryOutput::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenQueryOutput), xmlszLocalname))
	{
		// start the query output section in the DXL document
		GPOS_ASSERT(NULL == m_pdrgpdxln);

		m_pdrgpdxln = GPOS_NEW(m_pmp) DrgPdxln(m_pmp);
	}
	else if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarIdent), xmlszLocalname))
	{
		// we must have seen a proj list already and initialized the proj list node
		GPOS_ASSERT(NULL != m_pdrgpdxln);

		// start new scalar ident element
		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarIdent), m_pphm, this);
		m_pphm->ActivateParseHandler(pphChild);

		// store parse handler
		this->Append(pphChild);

		pphChild->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerQueryOutput::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerQueryOutput::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenQueryOutput), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	const ULONG ulSize = this->UlLength();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CParseHandlerScalarIdent *pphChild = dynamic_cast<CParseHandlerScalarIdent *>((*this)[ul]);

		GPOS_ASSERT(NULL != pphChild);

		CDXLNode *pdxlnIdent = pphChild->Pdxln();
		pdxlnIdent->AddRef();
		m_pdrgpdxln->Append(pdxlnIdent);
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
