//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerLogicalDelete.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing logical
//		delete operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerLogicalDelete.h"

#include "naucrates/dxl/parser/CParseHandlerTableDescr.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalDelete::CParseHandlerLogicalDelete
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerLogicalDelete::CParseHandlerLogicalDelete
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerLogicalOp(pmp, pphm, pphRoot)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalDelete::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerLogicalDelete::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes &attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalDelete), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	m_ulCtid = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenCtidColId, EdxltokenLogicalDelete);
	m_ulSegmentId = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenGpSegmentIdColId, EdxltokenLogicalDelete);

	const XMLCh *xmlszDeleteColIds = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenDeleteCols, EdxltokenLogicalDelete);
	m_pdrgpulDelete = CDXLOperatorFactory::PdrgpulFromXMLCh(m_pphm->Pmm(), xmlszDeleteColIds, EdxltokenDeleteCols, EdxltokenLogicalDelete);

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
//		CParseHandlerLogicalDelete::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerLogicalDelete::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalDelete), xmlszLocalname))
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
							GPOS_NEW(m_pmp) CDXLLogicalDelete(m_pmp, pdxltabdesc, m_ulCtid, m_ulSegmentId, m_pdrgpulDelete)
							);

	AddChildFromParseHandler(pphChild);

#ifdef GPOS_DEBUG
	m_pdxln->Pdxlop()->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG

	// deactivate handler
	m_pphm->DeactivateHandler();
}
// EOF
