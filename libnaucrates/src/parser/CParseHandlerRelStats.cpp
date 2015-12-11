//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerRelStats.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing base relation
//		statistics.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/md/CDXLRelStats.h"

#include "naucrates/dxl/parser/CParseHandlerRelStats.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;
using namespace gpmd;
using namespace gpnaucrates;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerRelStats::CParseHandlerRelStats
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerRelStats::CParseHandlerRelStats
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerMetadataObject(pmp, pphm, pphRoot)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerRelStats::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerRelStats::StartElement
	(
	const XMLCh* const , // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const , // xmlszQname,
	const Attributes& attrs
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenRelationStats), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// parse table name
	const XMLCh *xmlszTableName = CDXLOperatorFactory::XmlstrFromAttrs
															(
															attrs,
															EdxltokenName,
															EdxltokenRelationStats
															);

	CWStringDynamic *pstrTableName = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszTableName);
	
	// create a copy of the string in the CMDName constructor
	CMDName *pmdname = GPOS_NEW(m_pmp) CMDName(m_pmp, pstrTableName);
	
	GPOS_DELETE(pstrTableName);
	

	// parse metadata id info
	IMDId *pmdid = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenMdid, EdxltokenRelationStats);
	
	// parse rows

	CDouble dRows = CDXLOperatorFactory::DValueFromAttrs
											(
											m_pphm->Pmm(),
											attrs,
											EdxltokenRows,
											EdxltokenRelationStats
											);
	
	BOOL fEmpty = false;
	const XMLCh *xmlszEmpty = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenEmptyRelation));
	if (NULL != xmlszEmpty)
	{
		fEmpty = CDXLOperatorFactory::FValueFromXmlstr
										(
										m_pphm->Pmm(),
										xmlszEmpty,
										EdxltokenEmptyRelation,
										EdxltokenStatsDerivedRelation
										);
	}

	m_pimdobj = GPOS_NEW(m_pmp) CDXLRelStats(m_pmp, CMDIdRelStats::PmdidConvert(pmdid), pmdname, dRows, fEmpty);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerRelStats::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerRelStats::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenRelationStats), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
