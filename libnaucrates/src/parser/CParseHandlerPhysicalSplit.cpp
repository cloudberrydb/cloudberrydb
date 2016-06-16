//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerPhysicalSplit.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing physical split
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerPhysicalSplit.h"

#include "naucrates/dxl/parser/CParseHandlerProperties.h"
#include "naucrates/dxl/parser/CParseHandlerProjList.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerUtils.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalSplit::CParseHandlerPhysicalSplit
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerPhysicalSplit::CParseHandlerPhysicalSplit
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerPhysicalOp(pmp, pphm, pphRoot),
	m_pdrgpulDelete(NULL),
	m_pdrgpulInsert(NULL),
	m_ulAction(0),
	m_ulCtid(0),
	m_ulSegmentId(0),	
	m_fPreserveOids(false),
	m_ulTupleOidColId(0)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalSplit::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalSplit::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes &attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalSplit), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	const XMLCh *xmlszDeleteColIds = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenDeleteCols, EdxltokenPhysicalSplit);
	m_pdrgpulDelete = CDXLOperatorFactory::PdrgpulFromXMLCh(m_pphm->Pmm(), xmlszDeleteColIds, EdxltokenDeleteCols, EdxltokenPhysicalSplit);

	const XMLCh *xmlszInsertColIds = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenInsertCols, EdxltokenPhysicalSplit);
	m_pdrgpulInsert = CDXLOperatorFactory::PdrgpulFromXMLCh(m_pphm->Pmm(), xmlszInsertColIds, EdxltokenInsertCols, EdxltokenPhysicalSplit);

	m_ulAction = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenActionColId, EdxltokenPhysicalSplit);
	m_ulCtid = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenCtidColId, EdxltokenPhysicalSplit);
	m_ulSegmentId = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenGpSegmentIdColId, EdxltokenPhysicalSplit);

	const XMLCh *xmlszPreserveOids = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenUpdatePreservesOids));
	if (NULL != xmlszPreserveOids)
	{
		m_fPreserveOids = CDXLOperatorFactory::FValueFromXmlstr
											(
											m_pphm->Pmm(),
											xmlszPreserveOids,
											EdxltokenUpdatePreservesOids,
											EdxltokenPhysicalSplit
											);
	}	
	if (m_fPreserveOids)
	{
		m_ulTupleOidColId = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenTupleOidColId, EdxltokenPhysicalSplit);
	}

	// parse handler for physical operator
	CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenPhysical), m_pphm, this);
	m_pphm->ActivateParseHandler(pphChild);

	// parse handler for the proj list
	CParseHandlerBase *pphPrL = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarProjList), m_pphm, this);
	m_pphm->ActivateParseHandler(pphPrL);

	//parse handler for the properties of the operator
	CParseHandlerBase *pphProp = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenProperties), m_pphm, this);
	m_pphm->ActivateParseHandler(pphProp);

	// store child parse handlers in array
	this->Append(pphProp);
	this->Append(pphPrL);
	this->Append(pphChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalSplit::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalSplit::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalSplit), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(3 == this->UlLength());

	CParseHandlerProperties *pphProp = dynamic_cast<CParseHandlerProperties *>((*this)[0]);
	CParseHandlerProjList *pphPrL = dynamic_cast<CParseHandlerProjList*>((*this)[1]);
	GPOS_ASSERT(NULL != pphPrL->Pdxln());

	CParseHandlerPhysicalOp *pphChild = dynamic_cast<CParseHandlerPhysicalOp*>((*this)[2]);
	GPOS_ASSERT(NULL != pphChild->Pdxln());

	CDXLPhysicalSplit *pdxlop = GPOS_NEW(m_pmp) CDXLPhysicalSplit
												(
												m_pmp,
												m_pdrgpulDelete,
												m_pdrgpulInsert,
												m_ulAction,
												m_ulCtid,
												m_ulSegmentId,
												m_fPreserveOids,
												m_ulTupleOidColId
												);

	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);

	// set statistics and physical properties
	CParseHandlerUtils::SetProperties(m_pdxln, pphProp);

	AddChildFromParseHandler(pphPrL);
	AddChildFromParseHandler(pphChild);

#ifdef GPOS_DEBUG
	m_pdxln->Pdxlop()->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
