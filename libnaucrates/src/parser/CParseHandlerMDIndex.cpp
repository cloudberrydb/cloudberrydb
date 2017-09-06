//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerMDIndex.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing an MD index
//---------------------------------------------------------------------------

#include "naucrates/md/CMDIndexGPDB.h"

#include "naucrates/dxl/parser/CParseHandlerMDIndex.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataIdList.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;
using namespace gpmd;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDIndex::CParseHandlerMDIndex
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerMDIndex::CParseHandlerMDIndex
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerMetadataObject(pmp, pphm, pphRoot),
	m_pmdid(NULL),
	m_pmdname(NULL),
	m_fClustered(false),
	m_emdindt(IMDIndex::EmdindSentinel),
	m_pmdidItemType(NULL),
	m_pdrgpulKeyCols(NULL),
	m_pdrgpulIncludedCols(NULL),
	m_ppartcnstr(NULL),
	m_pdrgpulDefaultParts(NULL),
	m_fPartConstraintUnbounded(false)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDIndex::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDIndex::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPartConstraint), xmlszLocalname))
	{
		GPOS_ASSERT(NULL == m_ppartcnstr);
		
		const XMLCh *xmlszDefParts = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenDefaultPartition));
		if (NULL != xmlszDefParts)
		{
			m_pdrgpulDefaultParts = CDXLOperatorFactory::PdrgpulFromXMLCh(m_pphm->Pmm(), xmlszDefParts, EdxltokenDefaultPartition, EdxltokenIndex);
		}
		else
		{
			// construct an empty keyset
			m_pdrgpulDefaultParts = GPOS_NEW(m_pmp) DrgPul(m_pmp);
		}

		m_fPartConstraintUnbounded = CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenPartConstraintUnbounded, EdxltokenIndex);

		// parse handler for part constraints
		CParseHandlerBase *pphPartConstraint= CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
		m_pphm->ActivateParseHandler(pphPartConstraint);
		this->Append(pphPartConstraint);
		return;
	}
	
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenIndex), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// new index object 
	GPOS_ASSERT(NULL == m_pmdid);

	// parse mdid
	m_pmdid = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenMdid, EdxltokenIndex);
	
	// parse index name
	const XMLCh *xmlszColName = CDXLOperatorFactory::XmlstrFromAttrs
															(
															attrs,
															EdxltokenName,
															EdxltokenIndex
															);
	CWStringDynamic *pstrColName = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszColName);
	
	// create a copy of the string in the CMDName constructor
	m_pmdname = GPOS_NEW(m_pmp) CMDName(m_pmp, pstrColName);
	GPOS_DELETE(pstrColName);

	// parse index clustering, key columns and included columns information
	m_fClustered = CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenIndexClustered, EdxltokenIndex);
	
	m_emdindt = CDXLOperatorFactory::EmdindtFromAttr(attrs);
	const XMLCh *xmlszItemType = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenIndexItemType));
	if (NULL != xmlszItemType)
	{
		m_pmdidItemType = CDXLOperatorFactory::PmdidFromXMLCh
							(
							m_pphm->Pmm(),
							xmlszItemType,
							EdxltokenIndexItemType,
							EdxltokenIndex
							);
	}

	const XMLCh *xmlszIndexKeys = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenIndexKeyCols, EdxltokenIndex);
	m_pdrgpulKeyCols = CDXLOperatorFactory::PdrgpulFromXMLCh(m_pphm->Pmm(), xmlszIndexKeys, EdxltokenIndexKeyCols, EdxltokenIndex);

	const XMLCh *xmlszIndexIncludedCols = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenIndexIncludedCols, EdxltokenIndex);
	m_pdrgpulIncludedCols = CDXLOperatorFactory::PdrgpulFromXMLCh
													(
													m_pphm->Pmm(),
													xmlszIndexIncludedCols,
													EdxltokenIndexIncludedCols,
													EdxltokenIndex
													);
	
	// parse handler for operator class list
	CParseHandlerBase *pphOpClassList = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenMetadataIdList), m_pphm, this);
	this->Append(pphOpClassList);
	m_pphm->ActivateParseHandler(pphOpClassList);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDIndex::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDIndex::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPartConstraint), xmlszLocalname))
	{
		GPOS_ASSERT(2 == UlLength());
		
		CParseHandlerScalarOp *pphPartCnstr = dynamic_cast<CParseHandlerScalarOp *>((*this)[1]);
		CDXLNode *pdxlnPartConstraint = pphPartCnstr->Pdxln();
		pdxlnPartConstraint->AddRef();
		m_ppartcnstr = GPOS_NEW(m_pmp) CMDPartConstraintGPDB(m_pmp, m_pdrgpulDefaultParts, m_fPartConstraintUnbounded, pdxlnPartConstraint);
		return;
	}
	
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenIndex), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	CParseHandlerMetadataIdList *pphMdidOpClasses = dynamic_cast<CParseHandlerMetadataIdList*>((*this)[0]);
	DrgPmdid *pdrgpmdidOpClasses = pphMdidOpClasses->Pdrgpmdid();
	pdrgpmdidOpClasses->AddRef();

	m_pimdobj = GPOS_NEW(m_pmp) CMDIndexGPDB
							(
							m_pmp, 
							m_pmdid, 
							m_pmdname,
							m_fClustered, 
							m_emdindt,
							m_pmdidItemType,
							m_pdrgpulKeyCols, 
							m_pdrgpulIncludedCols, 
							pdrgpmdidOpClasses,
							m_ppartcnstr
							);
	
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
