//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CParseHandlerScalarBitmapIndexProbe.cpp
//
//	@doc:
//		SAX parse handler class for parsing bitmap index probe operator nodes
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarBitmapIndexProbe.h"

#include "naucrates/dxl/operators/CDXLScalarBitmapIndexProbe.h"
#include "naucrates/dxl/parser/CParseHandlerIndexCondList.h"
#include "naucrates/dxl/parser/CParseHandlerTableDescr.h"
#include "naucrates/dxl/parser/CParseHandlerIndexDescr.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFilter.h"
#include "naucrates/dxl/parser/CParseHandlerProjList.h"
#include "naucrates/dxl/parser/CParseHandlerProperties.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerUtils.h"

using namespace gpdxl;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarBitmapIndexProbe::CParseHandlerScalarBitmapIndexProbe
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerScalarBitmapIndexProbe::CParseHandlerScalarBitmapIndexProbe
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
//		CParseHandlerScalarBitmapIndexProbe::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarBitmapIndexProbe::StartElement
	(
	const XMLCh* const,  // xmlszUri
 	const XMLCh* const xmlszLocalname,
	const XMLCh* const,  // xmlszQname
	const Attributes&  // attrs
	)
{
	if (0 != XMLString::compareString
					(
					CDXLTokens::XmlstrToken(EdxltokenScalarBitmapIndexProbe),
					xmlszLocalname
					))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// create and activate the parse handler for the children nodes in reverse
	// order of their expected appearance

	// parse handler for the index descriptor
	CParseHandlerBase *pphIdxD =
			CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenIndexDescr), m_pphm, this);
	m_pphm->ActivateParseHandler(pphIdxD);

	// parse handler for the index condition list
	CParseHandlerBase *pphIdxCondList =
			CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarIndexCondList), m_pphm, this);
	m_pphm->ActivateParseHandler(pphIdxCondList);

	// store parse handlers
	this->Append(pphIdxCondList);
	this->Append(pphIdxD);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarBitmapIndexProbe::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarBitmapIndexProbe::EndElement
	(
	const XMLCh* const,  // xmlszUri
	const XMLCh* const xmlszLocalname,
	const XMLCh* const  // xmlszQname
	)
{
	if (0 != XMLString::compareString
				(
				CDXLTokens::XmlstrToken(EdxltokenScalarBitmapIndexProbe),
				xmlszLocalname
				))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// construct node from the created child nodes
	CParseHandlerIndexCondList *pphIdxCondList = dynamic_cast<CParseHandlerIndexCondList *>((*this)[0]);
	CParseHandlerIndexDescr *pphIdxD = dynamic_cast<CParseHandlerIndexDescr *>((*this)[1]);

	CDXLIndexDescr *pdxlid = pphIdxD->Pdxlid();
	pdxlid->AddRef();

	CDXLScalar *pdxlop = GPOS_NEW(m_pmp) CDXLScalarBitmapIndexProbe(m_pmp, pdxlid);
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);

	// add children
	AddChildFromParseHandler(pphIdxCondList);

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
