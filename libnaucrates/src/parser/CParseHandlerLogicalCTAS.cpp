//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc
//
//	@filename:
//		CParseHandlerLogicalCTAS.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing logical
//		CTAS operators.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerLogicalCTAS.h"

#include "naucrates/dxl/parser/CParseHandlerColDescr.h"
#include "naucrates/dxl/parser/CParseHandlerCtasStorageOptions.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalCTAS::CParseHandlerLogicalCTAS
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerLogicalCTAS::CParseHandlerLogicalCTAS
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerLogicalOp(pmp, pphm, pphRoot),
	m_pmdid(NULL),
	m_pmdnameSchema(NULL),	
	m_pmdname(NULL),	
	m_pdrgpulDistr(NULL),
	m_pdrgpulSource(NULL),
	m_pdrgpiVarTypeMod(NULL),
	m_fTemporary(false)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalCTAS::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerLogicalCTAS::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes &attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalCTAS), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// parse metadata id
	m_pmdid = CDXLOperatorFactory::PmdidFromAttrs
						(
						m_pphm->Pmm(),
						attrs,
						EdxltokenMdid,
						EdxltokenLogicalCTAS
						);
	
	GPOS_ASSERT(IMDId::EmdidGPDBCtas == m_pmdid->Emdidt());
	
	// parse table name
	const XMLCh *xmlszTableName = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenName, EdxltokenLogicalCTAS);
	m_pmdname = CDXLUtils::PmdnameFromXmlsz(m_pphm->Pmm(), xmlszTableName);
	
	const XMLCh *xmlszSchema = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenSchema));
	if (NULL != xmlszSchema)
	{
		m_pmdnameSchema = CDXLUtils::PmdnameFromXmlsz(m_pphm->Pmm(), xmlszSchema);
	}
	
	// parse distribution policy
	const XMLCh *xmlszDistrPolicy = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenRelDistrPolicy, EdxltokenLogicalCTAS);
	m_ereldistrpolicy = CDXLOperatorFactory::EreldistrpolicyFromXmlstr(xmlszDistrPolicy);

	if (IMDRelation::EreldistrHash == m_ereldistrpolicy)
	{
		// parse distribution columns
		const XMLCh *xmlszDistrColumns = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenDistrColumns, EdxltokenLogicalCTAS);
		m_pdrgpulDistr = CDXLOperatorFactory::PdrgpulFromXMLCh(m_pphm->Pmm(), xmlszDistrColumns, EdxltokenDistrColumns, EdxltokenLogicalCTAS);
	}
	
	// parse storage type
	const XMLCh *xmlszStorage = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenRelStorageType, EdxltokenLogicalCTAS);
	m_erelstorage = CDXLOperatorFactory::ErelstoragetypeFromXmlstr(xmlszStorage);

	const XMLCh *xmlszSourceColIds = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenInsertCols, EdxltokenLogicalCTAS);
	m_pdrgpulSource = CDXLOperatorFactory::PdrgpulFromXMLCh(m_pphm->Pmm(), xmlszSourceColIds, EdxltokenInsertCols, EdxltokenLogicalCTAS);

	const XMLCh *xmlszVarTypeMod = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenVarTypeModList, EdxltokenLogicalCTAS);
	m_pdrgpiVarTypeMod =
			CDXLOperatorFactory::PdrgpiFromXMLCh(m_pphm->Pmm(), xmlszVarTypeMod, EdxltokenVarTypeModList, EdxltokenLogicalCTAS);
	
	m_fTemporary = CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenRelTemporary, EdxltokenLogicalCTAS);
	m_fHasOids = CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenRelHasOids, EdxltokenLogicalCTAS);

	// create child node parsers

	// parse handler for logical operator
	CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenLogical), m_pphm, this);
	m_pphm->ActivateParseHandler(pphChild);

	//parse handler for the storage options
	CParseHandlerBase *pphCTASOptions = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenCTASOptions), m_pphm, this);
	m_pphm->ActivateParseHandler(pphCTASOptions);
	
	//parse handler for the column descriptors
	CParseHandlerBase *pphColDescr = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenColumns), m_pphm, this);
	m_pphm->ActivateParseHandler(pphColDescr);

	// store child parse handler in array
	this->Append(pphColDescr);
	this->Append(pphCTASOptions);
	this->Append(pphChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalCTAS::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerLogicalCTAS::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalCTAS), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(3 == this->UlLength());

	CParseHandlerColDescr *pphColDescr = dynamic_cast<CParseHandlerColDescr *>((*this)[0]);
	CParseHandlerCtasStorageOptions *pphCTASOptions = dynamic_cast<CParseHandlerCtasStorageOptions *>((*this)[1]);
	CParseHandlerLogicalOp *pphChild = dynamic_cast<CParseHandlerLogicalOp*>((*this)[2]);

	GPOS_ASSERT(NULL != pphColDescr->Pdrgpdxlcd());
	GPOS_ASSERT(NULL != pphCTASOptions->Pdxlctasopt());
	GPOS_ASSERT(NULL != pphChild->Pdxln());
	
	DrgPdxlcd *pdrgpdxlcd = pphColDescr->Pdrgpdxlcd();
	pdrgpdxlcd->AddRef();
	
	CDXLCtasStorageOptions *pdxlctasopt = pphCTASOptions->Pdxlctasopt();
	pdxlctasopt->AddRef();
	
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode
							(
							m_pmp,
							GPOS_NEW(m_pmp) CDXLLogicalCTAS
										(
										m_pmp, 
										m_pmdid, 
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
	
	AddChildFromParseHandler(pphChild);

#ifdef GPOS_DEBUG
	m_pdxln->Pdxlop()->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
