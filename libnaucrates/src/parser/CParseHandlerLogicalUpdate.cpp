//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerLogicalUpdate.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing logical
//		update operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerLogicalUpdate.h"

#include "naucrates/dxl/parser/CParseHandlerTableDescr.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalUpdate::CParseHandlerLogicalUpdate
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerLogicalUpdate::CParseHandlerLogicalUpdate
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerLogicalOp(pmp, pphm, pphRoot),
	m_ulCtid(0),
	m_ulSegmentId(0),
	m_pdrgpulDelete(NULL),
	m_pdrgpulInsert(NULL),
	m_fPreserveOids(false),
	m_ulTupleOidColId(0)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalUpdate::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerLogicalUpdate::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes &attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalUpdate), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	m_ulCtid = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenCtidColId, EdxltokenLogicalUpdate);
	m_ulSegmentId = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenGpSegmentIdColId, EdxltokenLogicalUpdate);

	const XMLCh *xmlszDeleteColIds = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenDeleteCols, EdxltokenLogicalUpdate);
	m_pdrgpulDelete = CDXLOperatorFactory::PdrgpulFromXMLCh(m_pphm->Pmm(), xmlszDeleteColIds, EdxltokenDeleteCols, EdxltokenLogicalUpdate);

	const XMLCh *xmlszInsertColIds = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenInsertCols, EdxltokenLogicalUpdate);
	m_pdrgpulInsert = CDXLOperatorFactory::PdrgpulFromXMLCh(m_pphm->Pmm(), xmlszInsertColIds, EdxltokenInsertCols, EdxltokenLogicalUpdate);

	const XMLCh *xmlszPreserveOids = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenUpdatePreservesOids));
	if (NULL != xmlszPreserveOids)
	{
		m_fPreserveOids = CDXLOperatorFactory::FValueFromXmlstr
											(
											m_pphm->Pmm(),
											xmlszPreserveOids,
											EdxltokenUpdatePreservesOids,
											EdxltokenLogicalUpdate
											);
	}
	
	if (m_fPreserveOids)
	{
		m_ulTupleOidColId = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenTupleOidColId, EdxltokenLogicalUpdate);
	}

	// parse handler for logical operator
	CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenLogical), m_pphm, this);
	m_pphm->ActivateParseHandler(pphChild);

	//parse handler for the table descriptor
	CParseHandlerBase *pphTabDesc = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenTableDescr), m_pphm, this);
	m_pphm->ActivateParseHandler(pphTabDesc);

	// store child parse handler in array
	this->Append(pphTabDesc);
	this->Append(pphChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalUpdate::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerLogicalUpdate::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalUpdate), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(2 == this->UlLength());

	CParseHandlerTableDescr *pphTabDesc = dynamic_cast<CParseHandlerTableDescr*>((*this)[0]);
	CParseHandlerLogicalOp *pphChild = dynamic_cast<CParseHandlerLogicalOp*>((*this)[1]);

	GPOS_ASSERT(NULL != pphTabDesc->Pdxltabdesc());
	GPOS_ASSERT(NULL != pphChild->Pdxln());

	CDXLTableDescr *pdxltabdesc = pphTabDesc->Pdxltabdesc();
	pdxltabdesc->AddRef();

	m_pdxln = GPOS_NEW(m_pmp) CDXLNode
							(
							m_pmp,
							GPOS_NEW(m_pmp) CDXLLogicalUpdate(m_pmp, pdxltabdesc, m_ulCtid, m_ulSegmentId, m_pdrgpulDelete, m_pdrgpulInsert, m_fPreserveOids, m_ulTupleOidColId)
							);

	AddChildFromParseHandler(pphChild);

#ifdef GPOS_DEBUG
	m_pdxln->Pdxlop()->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG

	// deactivate handler
	m_pphm->DeactivateHandler();
}
// EOF
