//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software, Inc.
//
//	@filename:
//		CParseHandlerMDIndexInfoList.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing indexinfo list
//---------------------------------------------------------------------------

#include "naucrates/md/CMDIndexInfo.h"

#include "naucrates/dxl/parser/CParseHandlerMDIndexInfoList.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataIdList.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;
using namespace gpmd;

XERCES_CPP_NAMESPACE_USE

// ctor
CParseHandlerMDIndexInfoList::CParseHandlerMDIndexInfoList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pdrgpmdIndexInfo(NULL)
{
}

// dtor
CParseHandlerMDIndexInfoList::~CParseHandlerMDIndexInfoList()
{
	CRefCount::SafeRelease(m_pdrgpmdIndexInfo);
}

// returns array of indexinfo
DrgPmdIndexInfo *
CParseHandlerMDIndexInfoList::PdrgpmdIndexInfo()
{
	return m_pdrgpmdIndexInfo;
}

// invoked by Xerces to process an opening tag
void
CParseHandlerMDIndexInfoList::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenIndexInfoList), xmlszLocalname))
	{
		m_pdrgpmdIndexInfo = GPOS_NEW(m_pmp) DrgPmdIndexInfo(m_pmp);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenIndexInfo), xmlszLocalname))
	{
		// parse mdid
		IMDId *pmdid = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenMdid, EdxltokenIndexInfo);

		// parse index partial info
		BOOL fPartial = CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenIndexPartial, EdxltokenIndexInfo);

		CMDIndexInfo *pmdIndexInfo = GPOS_NEW(m_pmp) CMDIndexInfo
								(
								pmdid,
								fPartial
								);
		m_pdrgpmdIndexInfo->Append(pmdIndexInfo);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

// invoked by Xerces to process a closing tag
void
CParseHandlerMDIndexInfoList::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenIndexInfoList), xmlszLocalname))
	{
		// deactivate handler
		m_pphm->DeactivateHandler();
	}
	else if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenIndexInfo), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

// EOF
