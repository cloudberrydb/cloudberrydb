//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerMDRelation.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing relation metadata.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerMDRelation.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataColumns.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataIdList.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDRelation::CParseHandlerMDRelation
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerMDRelation::CParseHandlerMDRelation
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerMetadataObject(pmp, pphm, pphRoot),
	m_pmdid(NULL),
	m_pmdnameSchema(NULL),
	m_pmdname(NULL),
	m_fTemporary(false),
	m_fHasOids(false),
	m_erelstorage(IMDRelation::ErelstorageSentinel),
	m_ereldistrpolicy(IMDRelation::EreldistrSentinel),
	m_pdrgpulDistrColumns(NULL),
	m_fConvertHashToRandom(false),
	m_pdrgpulPartColumns(NULL),
	m_pdrgpszPartTypes(NULL),
	m_ulPartitions(0),
	m_pdrgpdrgpulKeys(NULL),
	m_ppartcnstr(NULL),
	m_pdrgpulDefaultParts(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDRelation::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDRelation::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPartConstraint), xmlszLocalname))
	{
		GPOS_ASSERT(NULL == m_ppartcnstr);

		const XMLCh *xmlszDefParts = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenDefaultPartition));
		if (NULL != xmlszDefParts)
		{
			m_pdrgpulDefaultParts = CDXLOperatorFactory::PdrgpulFromXMLCh(m_pphm->Pmm(), xmlszDefParts, EdxltokenDefaultPartition, EdxltokenRelation);
		}
		else
		{
			// construct an empty keyset
			m_pdrgpulDefaultParts = GPOS_NEW(m_pmp) DrgPul(m_pmp);
		}
		m_fPartConstraintUnbounded = CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenPartConstraintUnbounded, EdxltokenRelation);

		CParseHandlerMetadataIdList *pphMdidlIndices = dynamic_cast<CParseHandlerMetadataIdList*>((*this)[1]);

		// relcache translator will send partition constraint expression only when a partitioned relation has indices
		if (pphMdidlIndices->Pdrgpmdid()->UlLength() > 0)
		{
			// parse handler for part constraints
			CParseHandlerBase *pphPartConstraint= CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
			m_pphm->ActivateParseHandler(pphPartConstraint);
			this->Append(pphPartConstraint);
		}

		return;
	}

	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenRelation), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// parse main relation attributes: name, id, distribution policy and keys
	ParseRelationAttributes(attrs, EdxltokenRelation);

	// parse whether relation is temporary
	m_fTemporary = CDXLOperatorFactory::FValueFromAttrs
											(
											m_pphm->Pmm(),
											attrs,
											EdxltokenRelTemporary,
											EdxltokenRelation
											);

	// parse whether relation has oids
	const XMLCh *xmlszHasOids = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenRelHasOids));
	if (NULL != xmlszHasOids)
	{
		m_fHasOids = CDXLOperatorFactory::FValueFromXmlstr(m_pphm->Pmm(), xmlszHasOids, EdxltokenRelHasOids, EdxltokenRelation);
	}

	// parse storage type
	const XMLCh *xmlszStorageType = CDXLOperatorFactory::XmlstrFromAttrs
															(
															attrs,
															EdxltokenRelStorageType,
															EdxltokenRelation
															);
	
	m_erelstorage = CDXLOperatorFactory::ErelstoragetypeFromXmlstr(xmlszStorageType);

	const XMLCh *xmlszPartColumns = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenPartKeys));

	if (NULL != xmlszPartColumns)
	{
		m_pdrgpulPartColumns = CDXLOperatorFactory::PdrgpulFromXMLCh
														(
														m_pphm->Pmm(),
														xmlszPartColumns,
														EdxltokenPartKeys,
														EdxltokenRelation
														);
	}

	const XMLCh *xmlszPartTypes = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenPartTypes));

	if (NULL != xmlszPartTypes)
	{
		m_pdrgpszPartTypes = CDXLOperatorFactory::PdrgpszFromXMLCh
														(
														m_pphm->Pmm(),
														xmlszPartTypes,
														EdxltokenPartTypes,
														EdxltokenRelation
														);
	}

	const XMLCh *xmlszPartitions = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenNumLeafPartitions));

	if (NULL != xmlszPartitions)
	{
		m_ulPartitions = CDXLOperatorFactory::UlValueFromXmlstr
														(
														m_pphm->Pmm(),
														xmlszPartitions,
														EdxltokenNumLeafPartitions,
														EdxltokenRelation
														);
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
										EdxltokenRelation
										);
	}
	
	// parse children
	ParseChildNodes();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDRelation::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDRelation::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	CParseHandlerMetadataIdList *pphMdidlIndices = dynamic_cast<CParseHandlerMetadataIdList*>((*this)[1]);
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPartConstraint), xmlszLocalname))
	{
		// relcache translator will send partition constraint expression only when a partitioned relation has indices
		if (pphMdidlIndices->Pdrgpmdid()->UlLength() > 0)
		{
			CParseHandlerScalarOp *pphPartCnstr = dynamic_cast<CParseHandlerScalarOp *>((*this)[UlLength() - 1]);
			CDXLNode *pdxlnPartConstraint = pphPartCnstr->Pdxln();
			pdxlnPartConstraint->AddRef();
			m_ppartcnstr = GPOS_NEW(m_pmp) CMDPartConstraintGPDB(m_pmp, m_pdrgpulDefaultParts, m_fPartConstraintUnbounded, pdxlnPartConstraint);
		}
		else
		{
			// no partition constraint expression
			m_ppartcnstr = GPOS_NEW(m_pmp) CMDPartConstraintGPDB(m_pmp, m_pdrgpulDefaultParts, m_fPartConstraintUnbounded, NULL);
		}
		return;
	}

	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenRelation), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// construct metadata object from the created child elements
	CParseHandlerMetadataColumns *pphMdCol = dynamic_cast<CParseHandlerMetadataColumns *>((*this)[0]);
	CParseHandlerMetadataIdList *pphMdidlTriggers = dynamic_cast<CParseHandlerMetadataIdList*>((*this)[2]);
	CParseHandlerMetadataIdList *pphMdidlCheckConstraints = dynamic_cast<CParseHandlerMetadataIdList*>((*this)[3]);

	GPOS_ASSERT(NULL != pphMdCol->Pdrgpmdcol());
	GPOS_ASSERT(NULL != pphMdidlIndices->Pdrgpmdid());
	GPOS_ASSERT(NULL != pphMdidlCheckConstraints->Pdrgpmdid());

	// refcount child objects
	DrgPmdcol *pdrgpmdcol = pphMdCol->Pdrgpmdcol();
	DrgPmdid *pdrgpmdidIndices = pphMdidlIndices->Pdrgpmdid();
	DrgPmdid *pdrgpmdidTriggers = pphMdidlTriggers->Pdrgpmdid();
	DrgPmdid *pdrgpmdidCheckConstraint = pphMdidlCheckConstraints->Pdrgpmdid();
 
	pdrgpmdcol->AddRef();
	pdrgpmdidIndices->AddRef();
 	pdrgpmdidTriggers->AddRef();
 	pdrgpmdidCheckConstraint->AddRef();

	m_pimdobj = GPOS_NEW(m_pmp) CMDRelationGPDB
								(
									m_pmp,
									m_pmdid,
									m_pmdname,
									m_fTemporary,
									m_erelstorage,
									m_ereldistrpolicy,
									pdrgpmdcol,
									m_pdrgpulDistrColumns,
									m_pdrgpulPartColumns,
									m_pdrgpszPartTypes,
									m_ulPartitions,
									m_fConvertHashToRandom,
									m_pdrgpdrgpulKeys,
									pdrgpmdidIndices,
									pdrgpmdidTriggers,
									pdrgpmdidCheckConstraint,
									m_ppartcnstr,
									m_fHasOids
								);

	// deactivate handler
	m_pphm->DeactivateHandler();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDRelation::ParseRelationAttributes
//
//	@doc:
//		Helper function to parse relation attributes: name, id,
//		distribution policy and keys
//
//---------------------------------------------------------------------------
void
CParseHandlerMDRelation::ParseRelationAttributes
	(
	const Attributes& attrs,
	Edxltoken edxltokenElement
	)
{
	// parse table name
	const XMLCh *xmlszTableName = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenName, edxltokenElement);
	CWStringDynamic *pstrTableName = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszTableName);
	m_pmdname = GPOS_NEW(m_pmp) CMDName(m_pmp, pstrTableName);
	GPOS_DELETE(pstrTableName);

	// parse metadata id info
	m_pmdid = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenMdid, edxltokenElement);

	// parse distribution policy
	const XMLCh *xmlszDistrPolicy = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenRelDistrPolicy, edxltokenElement);
	m_ereldistrpolicy = CDXLOperatorFactory::EreldistrpolicyFromXmlstr(xmlszDistrPolicy);

	if (m_ereldistrpolicy == IMDRelation::EreldistrHash)
	{
		// parse distribution columns
		const XMLCh *xmlszDistrColumns = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenDistrColumns, edxltokenElement);
		m_pdrgpulDistrColumns = CDXLOperatorFactory::PdrgpulFromXMLCh(m_pphm->Pmm(), xmlszDistrColumns, EdxltokenDistrColumns, edxltokenElement);
	}

	// parse keys
	const XMLCh *xmlszKeys = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenKeys));
	if (NULL != xmlszKeys)
	{
		m_pdrgpdrgpulKeys = CDXLOperatorFactory::PdrgpdrgpulFromXMLCh(m_pphm->Pmm(), xmlszKeys, EdxltokenKeys, edxltokenElement);
	}
	else
	{
		// construct an empty keyset
		m_pdrgpdrgpulKeys = GPOS_NEW(m_pmp) DrgPdrgPul(m_pmp);
	}

	m_ulPartitions = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenNumLeafPartitions, edxltokenElement,
															true /* optional */, 0 /* default value */);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDRelation::ParseChildNodes
//
//	@doc:
//		Create and activate the parse handler for the children nodes
//
//---------------------------------------------------------------------------
void
CParseHandlerMDRelation::ParseChildNodes()
{
	// parse handler for check constraints
	CParseHandlerBase *pphCheckConstraintList = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenMetadataIdList), m_pphm, this);
	m_pphm->ActivateParseHandler(pphCheckConstraintList);

	// parse handler for trigger list
	CParseHandlerBase *pphTriggerList = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenMetadataIdList), m_pphm, this);
	m_pphm->ActivateParseHandler(pphTriggerList);

	// parse handler for indices list
	CParseHandlerBase *pphIndexList = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenMetadataIdList), m_pphm, this);
	m_pphm->ActivateParseHandler(pphIndexList);

	// parse handler for the columns
	CParseHandlerBase *pphColumns = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenMetadataColumns), m_pphm, this);
	m_pphm->ActivateParseHandler(pphColumns);

	// store parse handlers
	this->Append(pphColumns);
	this->Append(pphIndexList);
 	this->Append(pphTriggerList);
 	this->Append(pphCheckConstraintList);
}


// EOF
