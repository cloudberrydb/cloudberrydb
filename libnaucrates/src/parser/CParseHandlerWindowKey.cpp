//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerWindowKey.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing the window key
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerWindowKey.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerWindowFrame.h"
#include "naucrates/dxl/parser/CParseHandlerSortColList.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerWindowKey::CParseHandlerWindowKey
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerWindowKey::CParseHandlerWindowKey
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pdxlwk(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerWindowKey::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerWindowKey::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenWindowKey), xmlszLocalname))
	{
		GPOS_ASSERT(NULL == m_pdxlwk);
		m_pdxlwk = GPOS_NEW(m_pmp) CDXLWindowKey(m_pmp);

		// parse handler for the sorting column list
		CParseHandlerBase *pphSortColList =
				CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarSortColList), m_pphm, this);
		m_pphm->ActivateParseHandler(pphSortColList);

		// store parse handler
		this->Append(pphSortColList);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenWindowFrame), xmlszLocalname))
	{
		GPOS_ASSERT(1 == this->UlLength());

		// parse handler for the leading and trailing scalar values
		CParseHandlerBase *pphWf =
				CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenWindowFrame), m_pphm, this);
		m_pphm->ActivateParseHandler(pphWf);

		// store parse handler
		this->Append(pphWf);
		pphWf->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
	else
	{
			CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerWindowKey::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerWindowKey::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenWindowKey), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	GPOS_ASSERT(NULL != m_pdxlwk);
	GPOS_ASSERT(1 <= this->UlLength());

	CParseHandlerSortColList *pphSortColList = dynamic_cast<CParseHandlerSortColList*>((*this)[0]);
	CDXLNode *pdxlnSortColList = pphSortColList->Pdxln();
	pdxlnSortColList->AddRef();
	m_pdxlwk->SetSortColList(pdxlnSortColList);

	if (2 == this->UlLength())
	{
		CParseHandlerWindowFrame *pphWf = dynamic_cast<CParseHandlerWindowFrame *>((*this)[1]);
		CDXLWindowFrame *pdxlwf = pphWf->Pdxlwf();
		m_pdxlwk->SetWindowFrame(pdxlwf);
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
