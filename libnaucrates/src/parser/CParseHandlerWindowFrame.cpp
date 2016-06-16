//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerWindowFrame.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing a window frame
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerWindowFrame.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerWindowFrame::CParseHandlerWindowFrame
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerWindowFrame::CParseHandlerWindowFrame
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_edxlfs(EdxlfsSentinel),
	m_edxlfes(EdxlfesSentinel),
	m_pdxlwf(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerWindowFrame::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerWindowFrame::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenWindowFrame), xmlszLocalname))
	{
		m_edxlfs = CDXLOperatorFactory::Edxlfs(attrs);
		m_edxlfes = CDXLOperatorFactory::Edxlfes(attrs);

		// parse handler for the trailing window frame edge
		CParseHandlerBase *pphTrailingVal =
				CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarWindowFrameTrailingEdge), m_pphm, this);
		m_pphm->ActivateParseHandler(pphTrailingVal);

		// parse handler for the leading scalar values
		CParseHandlerBase *pphLeadingVal =
				CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarWindowFrameLeadingEdge), m_pphm, this);
		m_pphm->ActivateParseHandler(pphLeadingVal);

		this->Append(pphLeadingVal);
		this->Append(pphTrailingVal);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerWindowFrame::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerWindowFrame::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenWindowFrame), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	GPOS_ASSERT(NULL == m_pdxlwf);
	GPOS_ASSERT(2 == this->UlLength());

	CParseHandlerScalarOp *pphTrailingVal = dynamic_cast<CParseHandlerScalarOp *>((*this)[0]);
	GPOS_ASSERT(NULL != pphTrailingVal);
	CDXLNode *pdxlnTrailing = pphTrailingVal->Pdxln();
	pdxlnTrailing->AddRef();

	CParseHandlerScalarOp *pphLeadingVal = dynamic_cast<CParseHandlerScalarOp *>((*this)[1]);
	GPOS_ASSERT(NULL != pphLeadingVal);
	CDXLNode *pdxlnLeading = pphLeadingVal->Pdxln();
	pdxlnLeading->AddRef();

	m_pdxlwf = GPOS_NEW(m_pmp) CDXLWindowFrame(m_pmp, m_edxlfs, m_edxlfes, pdxlnLeading, pdxlnTrailing);

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
