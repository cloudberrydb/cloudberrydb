//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CParseHandlerCostModel.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing cost model
//		config params
//---------------------------------------------------------------------------

#include "gpos/common/CBitSet.h"

#include "naucrates/dxl/parser/CParseHandlerCostModel.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerCostParams.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/traceflags/traceflags.h"

#include "naucrates/dxl/xml/dxltokens.h"

#include "gpdbcost/CCostModelGPDB.h"
#include "gpdbcost/CCostModelGPDBLegacy.h"

using namespace gpdxl;
using namespace gpdbcost;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCostModel::CParseHandlerCostModel
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerCostModel::CParseHandlerCostModel
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_ulSegments(0),
	m_pcm(NULL),
	m_pphcp(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCostModel::~CParseHandlerCostModel
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerCostModel::~CParseHandlerCostModel()
{
	CRefCount::SafeRelease(m_pcm);
	GPOS_DELETE(m_pphcp);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCostModel::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerCostModel::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCostModelConfig), xmlszLocalname))
	{
		m_ulSegments = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenSegmentsForCosting,
															 EdxltokenCostModelConfig);

		m_ecmt = (ICostModel::ECostModelType) CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs,
																					EdxltokenCostModelType,
																					EdxltokenCostModelConfig);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCostParams), xmlszLocalname))
	{
		CParseHandlerBase *pphCostParams = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenCostParams), m_pphm, this);
		m_pphcp = static_cast<CParseHandlerCostParams *>(pphCostParams);
		m_pphm->ActivateParseHandler(pphCostParams);

		pphCostParams->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCostModel::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerCostModel::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCostModelConfig), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE( gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	switch (m_ecmt)
	{
		case ICostModel::EcmtGPDBLegacy:
			m_pcm = GPOS_NEW(m_pmp) CCostModelGPDBLegacy(m_pmp, m_ulSegments);
			break;
		case ICostModel::EcmtGPDBCalibrated:
			CCostModelParamsGPDB *pcp;

			if (NULL == m_pphcp)
			{
				pcp = NULL;
				GPOS_ASSERT(false && "CostModelParam handler not set");
			}
			else
			{
				pcp = dynamic_cast<CCostModelParamsGPDB *>(m_pphcp->Pcp());
				GPOS_ASSERT(NULL != pcp);
				pcp->AddRef();
			}
			m_pcm = GPOS_NEW(m_pmp) CCostModelGPDB(m_pmp, m_ulSegments, pcp);
			break;
		case ICostModel::EcmtSentinel:
			GPOS_ASSERT(false && "Unexpected cost model type");
			break;
	}

    // deactivate handler
	m_pphm->DeactivateHandler();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCostModel::Pmc
//
//	@doc:
//		Returns the cost model config object
//
//---------------------------------------------------------------------------
ICostModel *
CParseHandlerCostModel::Pcm() const
{
	return m_pcm;
}

// EOF
