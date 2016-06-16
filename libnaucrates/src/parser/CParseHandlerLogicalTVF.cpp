//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerLogicalTVF.cpp
//
//	@doc:
//
//		Implementation of the SAX parse handler class for parsing table-valued
//		functions
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerLogicalTVF.h"
#include "naucrates/dxl/parser/CParseHandlerColDescr.h"
#include "naucrates/dxl/parser/CParseHandlerLogicalOp.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalTVF::CParseHandlerLogicalTVF
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerLogicalTVF::CParseHandlerLogicalTVF
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerLogicalOp(pmp, pphm, pphRoot),
	m_pmdidFunc(NULL),
	m_pmdidRetType(NULL),
	m_pmdname(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalTVF::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerLogicalTVF::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes &attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalTVF), xmlszLocalname))
	{
		// parse function id
		m_pmdidFunc = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenFuncId, EdxltokenLogicalTVF);

		// parse function name
		const XMLCh *xmlszFuncName = CDXLOperatorFactory::XmlstrFromAttrs
																(
																attrs,
																EdxltokenName,
																EdxltokenLogicalTVF
																);

		CWStringDynamic *pstrFuncName = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszFuncName);
		m_pmdname = GPOS_NEW(m_pmp) CMDName(m_pmp, pstrFuncName);
		GPOS_DELETE(pstrFuncName);

		// parse function return type
		m_pmdidRetType = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenTypeId, EdxltokenLogicalTVF);

	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenColumns), xmlszLocalname))
	{
		// parse handler for columns
		CParseHandlerBase *pphColDescr = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenColumns), m_pphm, this);
		m_pphm->ActivateParseHandler(pphColDescr);

		// store parse handlers
		this->Append(pphColDescr);

		pphColDescr->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
	else
	{
		// parse scalar child
		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
		m_pphm->ActivateParseHandler(pphChild);

		// store parse handlers
		this->Append(pphChild);

		pphChild->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalTVF::EndElement
//
//	@doc:
//		Processes a Xerces end element event
//
//---------------------------------------------------------------------------
void
CParseHandlerLogicalTVF::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalTVF), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	CParseHandlerColDescr *pphColDescr = dynamic_cast<CParseHandlerColDescr *>((*this)[0]);

	GPOS_ASSERT(NULL != pphColDescr);

	// get column descriptors
	DrgPdxlcd *pdrgpdxlcd = pphColDescr->Pdrgpdxlcd();
	GPOS_ASSERT(NULL != pdrgpdxlcd);

	pdrgpdxlcd->AddRef();
	CDXLLogicalTVF *pdxlopTVF = GPOS_NEW(m_pmp) CDXLLogicalTVF(m_pmp, m_pmdidFunc, m_pmdidRetType, m_pmdname, pdrgpdxlcd);

	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopTVF);

	const ULONG ulLen = this->UlLength();
	// loop over arglist children and add them to this parsehandler
	for (ULONG ul = 1; ul < ulLen; ul++)
	{
		CParseHandlerScalarOp *pphChild = dynamic_cast<CParseHandlerScalarOp *>((*this)[ul]);
		AddChildFromParseHandler(pphChild);
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
