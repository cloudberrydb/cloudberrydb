//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CParseHandlerMDRelationExternal.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing external
//		relation metadata.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerMDRelationExternal.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataColumns.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataIdList.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerMDIndexInfoList.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

#define GPDXL_DEFAULT_REJLIMIT -1

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDRelationExternal::CParseHandlerMDRelationExternal
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerMDRelationExternal::CParseHandlerMDRelationExternal
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerMDRelation(pmp, pphm, pphRoot),
	m_iRejectLimit(GPDXL_DEFAULT_REJLIMIT),
	m_fRejLimitInRows(false),
	m_pmdidFmtErrRel(NULL)
{
	m_erelstorage = IMDRelation::ErelstorageExternal;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDRelationExternal::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDRelationExternal::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenRelationExternal), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// parse main relation attributes: name, id, distribution policy and keys
	ParseRelationAttributes(attrs, EdxltokenRelation);

	// parse reject limit
	const XMLCh *xmlszRejLimit = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenExtRelRejLimit));
	if (NULL != xmlszRejLimit)
	{
		m_iRejectLimit = CDXLOperatorFactory::IValueFromXmlstr(m_pphm->Pmm(), xmlszRejLimit, EdxltokenExtRelRejLimit, EdxltokenRelationExternal);
		m_fRejLimitInRows = CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenExtRelRejLimitInRows, EdxltokenRelationExternal);
	}

	// format error table id
	const XMLCh *xmlszErrRel = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenExtRelFmtErrRel));
	if (NULL != xmlszErrRel)
	{
		m_pmdidFmtErrRel = CDXLOperatorFactory::PmdidFromXMLCh(m_pphm->Pmm(), xmlszErrRel, EdxltokenExtRelFmtErrRel, EdxltokenRelationExternal);
	}

	// parse whether a hash distributed relation needs to be considered as random distributed
	const XMLCh *xmlszConvertHashToRandom = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenConvertHashToRandom));
	if (NULL != xmlszConvertHashToRandom)
	{
		m_fConvertHashToRandom = CDXLOperatorFactory::FValueFromXmlstr
										(
										m_pphm->Pmm(),
										xmlszConvertHashToRandom,
										EdxltokenConvertHashToRandom,
										EdxltokenRelationExternal
										);
	}

	// parse children
	ParseChildNodes();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDRelationExternal::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDRelationExternal::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenRelationExternal), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// construct metadata object from the created child elements
	CParseHandlerMetadataColumns *pphMdCol = dynamic_cast<CParseHandlerMetadataColumns *>((*this)[0]);
	CParseHandlerMDIndexInfoList *pphMdlIndexInfo = dynamic_cast<CParseHandlerMDIndexInfoList*>((*this)[1]);
	CParseHandlerMetadataIdList *pphMdidlTriggers = dynamic_cast<CParseHandlerMetadataIdList*>((*this)[2]);
	CParseHandlerMetadataIdList *pphMdidlCheckConstraints = dynamic_cast<CParseHandlerMetadataIdList*>((*this)[3]);

	GPOS_ASSERT(NULL != pphMdCol->Pdrgpmdcol());
	GPOS_ASSERT(NULL != pphMdlIndexInfo->PdrgpmdIndexInfo());
	GPOS_ASSERT(NULL != pphMdidlCheckConstraints->Pdrgpmdid());

	// refcount child objects
	DrgPmdcol *pdrgpmdcol = pphMdCol->Pdrgpmdcol();
	DrgPmdIndexInfo *pdrgpmdIndexInfo = pphMdlIndexInfo->PdrgpmdIndexInfo();
	DrgPmdid *pdrgpmdidTriggers = pphMdidlTriggers->Pdrgpmdid();
	DrgPmdid *pdrgpmdidCheckConstraint = pphMdidlCheckConstraints->Pdrgpmdid();

	pdrgpmdcol->AddRef();
	pdrgpmdIndexInfo->AddRef();
 	pdrgpmdidTriggers->AddRef();
 	pdrgpmdidCheckConstraint->AddRef();

	m_pimdobj = GPOS_NEW(m_pmp) CMDRelationExternalGPDB
								(
									m_pmp,
									m_pmdid,
									m_pmdname,
									m_ereldistrpolicy,
									pdrgpmdcol,
									m_pdrgpulDistrColumns,
									m_fConvertHashToRandom,
									m_pdrgpdrgpulKeys,
									pdrgpmdIndexInfo,
									pdrgpmdidTriggers,
									pdrgpmdidCheckConstraint,
									m_iRejectLimit,
									m_fRejLimitInRows,
									m_pmdidFmtErrRel
								);

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
