//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerPhysicalRowTrigger.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing physical
//		row trigger
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerPhysicalRowTrigger.h"

#include "naucrates/dxl/parser/CParseHandlerProperties.h"
#include "naucrates/dxl/parser/CParseHandlerProjList.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerUtils.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalRowTrigger::CParseHandlerPhysicalRowTrigger
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerPhysicalRowTrigger::CParseHandlerPhysicalRowTrigger
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerPhysicalOp(pmp, pphm, pphRoot),
	m_pdxlop(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalRowTrigger::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalRowTrigger::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes &attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalRowTrigger), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	IMDId *pmdidRel = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenRelationMdid, EdxltokenPhysicalRowTrigger);

	INT iType = CDXLOperatorFactory::IValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenMDType, EdxltokenPhysicalRowTrigger);

	const XMLCh *xmlszOldColIds = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenOldCols));
	DrgPul *pdrgpulOld = NULL;
	if (NULL != xmlszOldColIds)
	{
		pdrgpulOld = CDXLOperatorFactory::PdrgpulFromXMLCh(m_pphm->Pmm(), xmlszOldColIds, EdxltokenOldCols, EdxltokenPhysicalRowTrigger);
	}

	const XMLCh *xmlszNewColIds = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenNewCols));
	DrgPul *pdrgpulNew = NULL;
	if (NULL != xmlszNewColIds)
	{
		pdrgpulNew = CDXLOperatorFactory::PdrgpulFromXMLCh(m_pphm->Pmm(), xmlszNewColIds, EdxltokenNewCols, EdxltokenPhysicalRowTrigger);
	}

	m_pdxlop = GPOS_NEW(m_pmp) CDXLPhysicalRowTrigger(m_pmp, pmdidRel, iType, pdrgpulOld, pdrgpulNew);

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
//		CParseHandlerPhysicalRowTrigger::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalRowTrigger::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalRowTrigger), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(3 == this->UlLength());

	CParseHandlerProperties *pphProp = dynamic_cast<CParseHandlerProperties *>((*this)[0]);

	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, m_pdxlop);

	// set statistics and physical properties
	CParseHandlerUtils::SetProperties(m_pdxln, pphProp);

	CParseHandlerProjList *pphPrL = dynamic_cast<CParseHandlerProjList*>((*this)[1]);
	GPOS_ASSERT(NULL != pphPrL->Pdxln());
	AddChildFromParseHandler(pphPrL);

	CParseHandlerPhysicalOp *pphChild = dynamic_cast<CParseHandlerPhysicalOp*>((*this)[2]);
	GPOS_ASSERT(NULL != pphChild->Pdxln());
	AddChildFromParseHandler(pphChild);

#ifdef GPOS_DEBUG
	m_pdxln->Pdxlop()->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
