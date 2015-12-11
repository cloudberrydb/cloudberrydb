//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc
//
//	@filename:
//		CParseHandlerPhysicalCTAS.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing physical
//		CTAS operators.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerPhysicalCTAS.h"

#include "naucrates/dxl/parser/CParseHandlerColDescr.h"
#include "naucrates/dxl/parser/CParseHandlerCtasStorageOptions.h"
#include "naucrates/dxl/parser/CParseHandlerProjList.h"
#include "naucrates/dxl/parser/CParseHandlerProperties.h"
#include "naucrates/dxl/parser/CParseHandlerUtils.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalCTAS::CParseHandlerPhysicalCTAS
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerPhysicalCTAS::CParseHandlerPhysicalCTAS
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerPhysicalOp(pmp, pphm, pphRoot),
	m_pmdnameSchema(NULL),
	m_pmdname(NULL),	
	m_pdrgpulDistr(NULL),
	m_pdrgpulSource(NULL),
	m_fTemporary(false),
	m_fHasOids(false)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalCTAS::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalCTAS::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes &attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalCTAS), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// parse table name
	const XMLCh *xmlszTableName = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenName, EdxltokenPhysicalCTAS);
	m_pmdname = CDXLUtils::PmdnameFromXmlsz(m_pphm->Pmm(), xmlszTableName);
	
	const XMLCh *xmlszSchema = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenSchema));
	if (NULL != xmlszSchema)
	{
		m_pmdnameSchema = CDXLUtils::PmdnameFromXmlsz(m_pphm->Pmm(), xmlszSchema);
	}
	
	// parse distribution policy
	const XMLCh *xmlszDistrPolicy = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenRelDistrPolicy, EdxltokenPhysicalCTAS);
	m_ereldistrpolicy = CDXLOperatorFactory::EreldistrpolicyFromXmlstr(xmlszDistrPolicy);

	if (IMDRelation::EreldistrHash == m_ereldistrpolicy)
	{
		// parse distribution columns
		const XMLCh *xmlszDistrColumns = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenDistrColumns, EdxltokenPhysicalCTAS);
		m_pdrgpulDistr = CDXLOperatorFactory::PdrgpulFromXMLCh(m_pphm->Pmm(), xmlszDistrColumns, EdxltokenDistrColumns, EdxltokenPhysicalCTAS);
	}
	
	// parse storage type
	const XMLCh *xmlszStorage = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenRelStorageType, EdxltokenPhysicalCTAS);
	m_erelstorage = CDXLOperatorFactory::ErelstoragetypeFromXmlstr(xmlszStorage);

	const XMLCh *xmlszSourceColIds = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenInsertCols, EdxltokenPhysicalCTAS);
	m_pdrgpulSource = CDXLOperatorFactory::PdrgpulFromXMLCh(m_pphm->Pmm(), xmlszSourceColIds, EdxltokenInsertCols, EdxltokenPhysicalCTAS);
	
	const XMLCh *xmlszVarTypeMod = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenVarTypeModList, EdxltokenPhysicalCTAS);
	m_pdrgpiVarTypeMod =
			CDXLOperatorFactory::PdrgpiFromXMLCh(m_pphm->Pmm(), xmlszVarTypeMod, EdxltokenVarTypeModList, EdxltokenPhysicalCTAS);

	m_fTemporary = CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenRelTemporary, EdxltokenPhysicalCTAS);
	m_fHasOids = CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenRelHasOids, EdxltokenPhysicalCTAS);

	// create child node parsers

	// parse handler for logical operator
	CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenLogical), m_pphm, this);
	m_pphm->ActivateParseHandler(pphChild);

	// parse handler for the proj list
	CParseHandlerBase *pphPrL = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarProjList), m_pphm, this);
	m_pphm->ActivateParseHandler(pphPrL);
	
	//parse handler for the storage options
	CParseHandlerBase *pphCTASOptions = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenCTASOptions), m_pphm, this);
	m_pphm->ActivateParseHandler(pphCTASOptions);
	
	//parse handler for the column descriptors
	CParseHandlerBase *pphColDescr = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenColumns), m_pphm, this);
	m_pphm->ActivateParseHandler(pphColDescr);
	
	//parse handler for the properties of the operator
	CParseHandlerBase *pphProp = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenProperties), m_pphm, this);
	m_pphm->ActivateParseHandler(pphProp);

	// store child parse handler in array
	this->Append(pphProp);
	this->Append(pphColDescr);
	this->Append(pphCTASOptions);
	this->Append(pphPrL);	
	this->Append(pphChild);	
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalCTAS::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalCTAS::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalCTAS), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(5 == this->UlLength());

	CParseHandlerProperties *pphProp = dynamic_cast<CParseHandlerProperties *>((*this)[0]);
	CParseHandlerColDescr *pphColDescr = dynamic_cast<CParseHandlerColDescr *>((*this)[1]);
	CParseHandlerCtasStorageOptions *pphCTASOptions = dynamic_cast<CParseHandlerCtasStorageOptions *>((*this)[2]);
	CParseHandlerProjList *pphPrL = dynamic_cast<CParseHandlerProjList*>((*this)[3]);
	GPOS_ASSERT(NULL != pphPrL->Pdxln());
	CParseHandlerPhysicalOp *pphChild = dynamic_cast<CParseHandlerPhysicalOp*>((*this)[4]);

	GPOS_ASSERT(NULL != pphProp->Pdxlprop());
	GPOS_ASSERT(NULL != pphColDescr->Pdrgpdxlcd());
	GPOS_ASSERT(NULL != pphCTASOptions->Pdxlctasopt());
	GPOS_ASSERT(NULL != pphPrL->Pdxln());
	GPOS_ASSERT(NULL != pphChild->Pdxln());
	
	DrgPdxlcd *pdrgpdxlcd = pphColDescr->Pdrgpdxlcd();
	pdrgpdxlcd->AddRef();
	
	CDXLCtasStorageOptions *pdxlctasopt = pphCTASOptions->Pdxlctasopt();
	pdxlctasopt->AddRef();
	
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode
							(
							m_pmp,
							GPOS_NEW(m_pmp) CDXLPhysicalCTAS
									(
									m_pmp,
									m_pmdnameSchema,
									m_pmdname,
									pdrgpdxlcd,
									pdxlctasopt,
									m_ereldistrpolicy,
									m_pdrgpulDistr,
									m_fTemporary,
									m_fHasOids,
									m_erelstorage,
									m_pdrgpulSource,
									m_pdrgpiVarTypeMod
									)
							);
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
