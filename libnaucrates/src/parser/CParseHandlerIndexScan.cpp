//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerIndexScan.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for the index scan operator
//
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerIndexScan.h"

#include "naucrates/dxl/parser/CParseHandlerIndexCondList.h"
#include "naucrates/dxl/parser/CParseHandlerTableDescr.h"
#include "naucrates/dxl/parser/CParseHandlerIndexDescr.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFilter.h"
#include "naucrates/dxl/parser/CParseHandlerProjList.h"
#include "naucrates/dxl/parser/CParseHandlerProperties.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerUtils.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerIndexScan::CParseHandlerIndexScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerIndexScan::CParseHandlerIndexScan
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerPhysicalOp(pmp, pphm, pphRoot),
	m_edxlisd(EdxlisdSentinel)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerIndexScan::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerIndexScan::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	StartElementHelper(xmlszLocalname, attrs, EdxltokenPhysicalIndexScan);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerIndexScan::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerIndexScan::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	EndElementHelper(xmlszLocalname, EdxltokenPhysicalIndexScan);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerIndexScan::StartElementHelper
//
//	@doc:
//		Common StartElement functionality for IndexScan and IndexOnlyScan
//
//---------------------------------------------------------------------------
void
CParseHandlerIndexScan::StartElementHelper
	(
	const XMLCh* const xmlszLocalname,
	const Attributes& attrs,
	Edxltoken edxltoken
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(edxltoken), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// get the index scan direction from the attribute
	const XMLCh *xmlszIndexScanDirection = CDXLOperatorFactory::XmlstrFromAttrs
																	(
																	attrs,
																	EdxltokenIndexScanDirection,
																	edxltoken
																	);
	m_edxlisd = CDXLOperatorFactory::EdxljtParseIndexScanDirection
										(
										xmlszIndexScanDirection,
										CDXLTokens::PstrToken(edxltoken)
										);
	GPOS_ASSERT(EdxlisdSentinel != m_edxlisd);

	// create and activate the parse handler for the children nodes in reverse
	// order of their expected appearance

	CParseHandlerBase *pphTD =
			CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenTableDescr), m_pphm, this);
	m_pphm->ActivateParseHandler(pphTD);

	// parse handler for the index descriptor
	CParseHandlerBase *pphIdxD =
			CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenIndexDescr), m_pphm, this);
	m_pphm->ActivateParseHandler(pphIdxD);

	// parse handler for the index condition list
	CParseHandlerBase *pphIdxCondList =
			CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarIndexCondList), m_pphm, this);
	m_pphm->ActivateParseHandler(pphIdxCondList);

	// parse handler for the filter
	CParseHandlerBase *pphFilter =
			CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarFilter), m_pphm, this);
	m_pphm->ActivateParseHandler(pphFilter);

	// parse handler for the proj list
	CParseHandlerBase *pphPrL =
			CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarProjList), m_pphm, this);
	m_pphm->ActivateParseHandler(pphPrL);

	//parse handler for the properties of the operator
	CParseHandlerBase *pphProp =
			CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenProperties), m_pphm, this);
	m_pphm->ActivateParseHandler(pphProp);

	// store parse handlers
	this->Append(pphProp);
	this->Append(pphPrL);
	this->Append(pphFilter);
	this->Append(pphIdxCondList);
	this->Append(pphIdxD);
	this->Append(pphTD);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerIndexScan::EndElementHelper
//
//	@doc:
//		Common EndElement functionality for IndexScan and IndexOnlyScan
//
//---------------------------------------------------------------------------
void
CParseHandlerIndexScan::EndElementHelper
	(
	const XMLCh* const xmlszLocalname,
	Edxltoken edxltoken,
	ULONG ulPartIndexId,
	ULONG ulPartIndexIdPrintable
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(edxltoken), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// construct node from the created child nodes
	CParseHandlerProperties *pphProp = dynamic_cast<CParseHandlerProperties *>((*this)[0]);
	CParseHandlerProjList *pphPrL = dynamic_cast<CParseHandlerProjList*>((*this)[1]);
	CParseHandlerFilter *pphFilter = dynamic_cast<CParseHandlerFilter *>((*this)[2]);
	CParseHandlerIndexCondList *pphIdxCondList = dynamic_cast<CParseHandlerIndexCondList *>((*this)[3]);
	CParseHandlerIndexDescr *pphIdxD = dynamic_cast<CParseHandlerIndexDescr *>((*this)[4]);
	CParseHandlerTableDescr *pphTD = dynamic_cast<CParseHandlerTableDescr *>((*this)[5]);

	CDXLTableDescr *pdxltabdesc = pphTD->Pdxltabdesc();
	pdxltabdesc->AddRef();

	CDXLIndexDescr *pdxlid = pphIdxD->Pdxlid();
	pdxlid->AddRef();

	CDXLPhysical *pdxlop = NULL;
	if (EdxltokenPhysicalIndexOnlyScan == edxltoken)
	{
		pdxlop = GPOS_NEW(m_pmp) CDXLPhysicalIndexOnlyScan(m_pmp, pdxltabdesc, pdxlid, m_edxlisd);
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);
	}
	else if (EdxltokenPhysicalIndexScan == edxltoken)
	{
		pdxlop = GPOS_NEW(m_pmp) CDXLPhysicalIndexScan(m_pmp, pdxltabdesc, pdxlid, m_edxlisd);
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);
	}
	else
	{
		GPOS_ASSERT(EdxltokenPhysicalDynamicIndexScan == edxltoken);

		pdxlop = GPOS_NEW(m_pmp) CDXLPhysicalDynamicIndexScan(m_pmp, pdxltabdesc, ulPartIndexId, ulPartIndexIdPrintable, pdxlid, m_edxlisd);
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);
	}

	// set statistics and physical properties
	CParseHandlerUtils::SetProperties(m_pdxln, pphProp);

	// add children
	AddChildFromParseHandler(pphPrL);
	AddChildFromParseHandler(pphFilter);
	AddChildFromParseHandler(pphIdxCondList);

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
