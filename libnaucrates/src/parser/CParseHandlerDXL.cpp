//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerDXL.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing a DXL document
//---------------------------------------------------------------------------

#include "gpos/task/CWorker.h"

#include "naucrates/dxl/parser/CParseHandlerDXL.h"
#include "naucrates/dxl/parser/CParseHandlerMetadata.h"
#include "naucrates/dxl/parser/CParseHandlerMDRequest.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerPlan.h"
#include "naucrates/dxl/parser/CParseHandlerQuery.h"
#include "naucrates/dxl/parser/CParseHandlerScalarExpr.h"
#include "naucrates/dxl/parser/CParseHandlerStatistics.h"
#include "naucrates/dxl/parser/CParseHandlerOptimizerConfig.h"
#include "naucrates/dxl/parser/CParseHandlerTraceFlags.h"
#include "naucrates/dxl/parser/CParseHandlerSearchStrategy.h"
#include "naucrates/dxl/parser/CParseHandlerCostParams.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

#include "gpopt/optimizer/COptimizerConfig.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::CParseHandlerDXL
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerDXL::CParseHandlerDXL
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm
	)
	:
	CParseHandlerBase(pmp, pphm, NULL),
	m_pbs(NULL),
	m_poconf(NULL),
	m_pmdr(NULL),
	m_pdxlnQuery(NULL),
	m_pdrgpdxlnOutputCols(NULL),
	m_pdrgpdxlnCTE(NULL),
	m_pdxlnPlan(NULL),
	m_pdrgpmdobj(NULL),
	m_pdrgpmdid(NULL),
	m_pdxlnScalarExpr(NULL),
	m_pdrgpsysid(NULL),
	m_pdrgpdxlstatsderrel(NULL),
	m_pdrgpss(NULL),
	m_ullPlanId(gpos::ullong_max),
	m_ullPlanSpaceSize(gpos::ullong_max),
	m_pcp(NULL)
{}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::~CParseHandlerDXL
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerDXL::~CParseHandlerDXL()
{
	CRefCount::SafeRelease(m_pbs);
	CRefCount::SafeRelease(m_poconf);
	CRefCount::SafeRelease(m_pmdr);
	CRefCount::SafeRelease(m_pdxlnQuery);
	CRefCount::SafeRelease(m_pdrgpdxlnOutputCols);
	CRefCount::SafeRelease(m_pdrgpdxlnCTE);
	CRefCount::SafeRelease(m_pdxlnPlan);
	CRefCount::SafeRelease(m_pdrgpmdobj);
	CRefCount::SafeRelease(m_pdrgpmdid);
	CRefCount::SafeRelease(m_pdxlnScalarExpr);
	CRefCount::SafeRelease(m_pdrgpsysid);
	CRefCount::SafeRelease(m_pdrgpdxlstatsderrel);
	CRefCount::SafeRelease(m_pdrgpss);
	CRefCount::SafeRelease(m_pcp);

}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::Pbs
//
//	@doc:
//		Returns the bitset of traceflags
//
//---------------------------------------------------------------------------
CBitSet *
CParseHandlerDXL::Pbs() const
{
	return m_pbs;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::Poc
//
//	@doc:
//		Returns the optimizer config object
//
//---------------------------------------------------------------------------
COptimizerConfig *
CParseHandlerDXL::Poconf() const
{
	return m_poconf;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::PdxlnQuery
//
//	@doc:
//		Returns the root of the DXL query constructed by this parser
//
//---------------------------------------------------------------------------
CDXLNode *
CParseHandlerDXL::PdxlnQuery() const
{
	return m_pdxlnQuery;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::PdrgpdxlnOutputCols
//
//	@doc:
//		Returns the list of query output objects constructed by the parser
//
//---------------------------------------------------------------------------
DrgPdxln *
CParseHandlerDXL::PdrgpdxlnOutputCols() const
{
	return m_pdrgpdxlnOutputCols;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::PdrgpdxlnCTE
//
//	@doc:
//		Returns the list of CTE producers
//
//---------------------------------------------------------------------------
DrgPdxln *
CParseHandlerDXL::PdrgpdxlnCTE() const
{
	return m_pdrgpdxlnCTE;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::PdxlnPlan
//
//	@doc:
//		Returns the root of the DXL plan constructed by this parser
//
//---------------------------------------------------------------------------
CDXLNode *
CParseHandlerDXL::PdxlnPlan() const
{
	return m_pdxlnPlan;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::Pdrgpmdobj
//
//	@doc:
//		Returns the list of metadata objects constructed by the parser
//
//---------------------------------------------------------------------------
DrgPimdobj *
CParseHandlerDXL::Pdrgpmdobj() const
{
	return m_pdrgpmdobj;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::Pdrgpmdid
//
//	@doc:
//		Returns the list of metadata ids constructed by the parser
//
//---------------------------------------------------------------------------
DrgPmdid *
CParseHandlerDXL::Pdrgpmdid() const
{
	return m_pdrgpmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::Pmdr
//
//	@doc:
//		Return the md request
//
//---------------------------------------------------------------------------
CMDRequest *
CParseHandlerDXL::Pmdr() const
{
	return m_pmdr;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::PdxlnScalarExpr
//
//	@doc:
//		Returns the DXL node representing the parsed scalar expression
//
//---------------------------------------------------------------------------
CDXLNode *
CParseHandlerDXL::PdxlnScalarExpr() const
{
	return m_pdxlnScalarExpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::Pdrgpsysid
//
//	@doc:
//		Returns the list of source system ids for the metadata 
//
//---------------------------------------------------------------------------
DrgPsysid *
CParseHandlerDXL::Pdrgpsysid() const
{
	return m_pdrgpsysid;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::Pdrgpdxlstatsderrel
//
//	@doc:
//		Returns the list of statistics objects constructed by the parser
//
//---------------------------------------------------------------------------
DrgPdxlstatsderrel *
CParseHandlerDXL::Pdrgpdxlstatsderrel() const
{
	return m_pdrgpdxlstatsderrel;
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::Pdrgpss
//
//	@doc:
//		Returns search strategy
//
//---------------------------------------------------------------------------
DrgPss *
CParseHandlerDXL::Pdrgpss() const
{
	return m_pdrgpss;
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::UllPlanId
//
//	@doc:
//		Returns plan id
//
//---------------------------------------------------------------------------
ULLONG
CParseHandlerDXL::UllPlanId() const
{
	return m_ullPlanId;
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::UllPlanId
//
//	@doc:
//		Returns plan space size
//
//---------------------------------------------------------------------------
ULLONG
CParseHandlerDXL::UllPlanSpaceSize() const
{
	return m_ullPlanSpaceSize;
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::Pcp
//
//	@doc:
//		Returns cost params
//
//---------------------------------------------------------------------------
ICostModelParams *
CParseHandlerDXL::Pcp() const
{
	return m_pcp;
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::FValidStartElement
//
//	@doc:
//		Return true if given element name is valid to start DXL document
//
//---------------------------------------------------------------------------
BOOL
CParseHandlerDXL::FValidStartElement
	(
	const XMLCh* const xmlszName
	)
{
	// names of valid start elements of DXL document
	const XMLCh *xmlstrValidStartElement [] =
		{
		CDXLTokens::XmlstrToken(EdxltokenTraceFlags),
		CDXLTokens::XmlstrToken(EdxltokenOptimizerConfig),
		CDXLTokens::XmlstrToken(EdxltokenPlan),
		CDXLTokens::XmlstrToken(EdxltokenQuery),
		CDXLTokens::XmlstrToken(EdxltokenMetadata),
		CDXLTokens::XmlstrToken(EdxltokenMDRequest),
		CDXLTokens::XmlstrToken(EdxltokenStatistics),
		CDXLTokens::XmlstrToken(EdxltokenStackTrace),
		CDXLTokens::XmlstrToken(EdxltokenSearchStrategy),
		CDXLTokens::XmlstrToken(EdxltokenCostParams),
		CDXLTokens::XmlstrToken(EdxltokenScalarExpr),
		};

	BOOL fValidStartElement = false;
	for (ULONG ul = 0; !fValidStartElement && ul < GPOS_ARRAY_SIZE(xmlstrValidStartElement); ul++)
	{
		fValidStartElement = (0 == XMLString::compareString(xmlszName, xmlstrValidStartElement[ul]));
	}

	return fValidStartElement;
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{		
	// reset time slice counter to ignore time taken by Xerces XSD grammar loader (OPT-491)
#ifdef GPOS_DEBUG
    CWorker::PwrkrSelf()->ResetTimeSlice();
#endif // GPOS_DEBUG
	
	if (0 == XMLString::compareString(xmlszLocalname, CDXLTokens::XmlstrToken(EdxltokenDXLMessage)) ||
		0 == XMLString::compareString(xmlszLocalname, CDXLTokens::XmlstrToken(EdxltokenThread)) ||
		0 == XMLString::compareString(xmlszLocalname, CDXLTokens::XmlstrToken(EdxltokenComment)))
	{
		// beginning of DXL document or a new thread info
		;
	}
	else
	{
		GPOS_ASSERT(FValidStartElement(xmlszLocalname));

		// install a parse handler for the given element
		CParseHandlerBase *pph = CParseHandlerFactory::Pph(m_pmp, xmlszLocalname, m_pphm, this);
	
		m_pphm->ActivateParseHandler(pph);
			
		// store parse handler
		this->Append(pph);
		
		pph->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const, // xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	// ignore
}

void
CParseHandlerDXL::endDocument()
{
	// retrieve plan and/or query and/or list of metadata objects from child parse handler
	for (ULONG ul = 0; ul < this->UlLength(); ul++)
	{
		CParseHandlerBase *pph = (*this)[ul];

		EDxlParseHandlerType edxlphtype = pph->Edxlphtype();

		// find parse handler for the current type
		Pfparse pf = FindParseHandler(edxlphtype);

		if (NULL != pf)
		{
			(this->*pf)(pph);
		}
	}
	
	m_pphm->DeactivateHandler();
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::FindParseHandler
//
//	@doc:
//		Find the parse handler function for the given type
//
//---------------------------------------------------------------------------
Pfparse
CParseHandlerDXL::FindParseHandler
	(
	EDxlParseHandlerType edxlphtype
	)
{
	SParseElem rgParseHandlers[] =
	{
		{EdxlphTraceFlags, &CParseHandlerDXL::ExtractTraceFlags},
		{EdxlphOptConfig, &CParseHandlerDXL::ExtractOptimizerConfig},
		{EdxlphPlan, &CParseHandlerDXL::ExtractDXLPlan},
		{EdxlphMetadata, &CParseHandlerDXL::ExtractMetadataObjects},
		{EdxlphStatistics, &CParseHandlerDXL::ExtractStats},
		{EdxlphQuery, &CParseHandlerDXL::ExtractDXLQuery},
		{EdxlphMetadataRequest, &CParseHandlerDXL::ExtractMDRequest},
		{EdxlphSearchStrategy, &CParseHandlerDXL::ExtractSearchStrategy},
		{EdxlphCostParams, &CParseHandlerDXL::ExtractCostParams},
		{EdxlphScalarExpr, &CParseHandlerDXL::ExtractScalarExpr},
	};

	const ULONG ulParseHandlers = GPOS_ARRAY_SIZE(rgParseHandlers);
	Pfparse pf = NULL;
	for (ULONG ul = 0; ul < ulParseHandlers; ul++)
	{
		SParseElem elem = rgParseHandlers[ul];
		if (edxlphtype == elem.edxlphtype)
		{
			pf = elem.pf;
			break;
		}
	}

	return pf;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::ExtractTraceFlags
//
//	@doc:
//		Extract traceflags
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::ExtractTraceFlags
	(
	CParseHandlerBase *pph
	)
{
	CParseHandlerTraceFlags *pphtf = (CParseHandlerTraceFlags *) pph;
	GPOS_ASSERT(NULL != pphtf);

	GPOS_ASSERT (NULL == m_pbs && "Traceflags already set");
	
	m_pbs = pphtf->Pbs();
	m_pbs->AddRef();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::ExtractOptimizerConfig
//
//	@doc:
//		Extract optimizer config
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::ExtractOptimizerConfig
	(
	CParseHandlerBase *pph
	)
{
	CParseHandlerOptimizerConfig *pphOptConfig = (CParseHandlerOptimizerConfig *) pph;
	GPOS_ASSERT(NULL != pphOptConfig);

	GPOS_ASSERT (NULL == m_pbs && "Traceflags already set");

	m_pbs = pphOptConfig->Pbs();
	m_pbs->AddRef();
	
	GPOS_ASSERT (NULL == m_poconf && "Optimizer configuration already set");

	m_poconf = pphOptConfig->Poconf();
	m_poconf->AddRef();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::ExtractDXLPlan
//
//	@doc:
//		Extract a physical plan
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::ExtractDXLPlan
	(
	CParseHandlerBase *pph
	)
{
	CParseHandlerPlan *pphPlan = (CParseHandlerPlan *) pph;
	GPOS_ASSERT(NULL != pphPlan && NULL != pphPlan->Pdxln());

	m_pdxlnPlan = pphPlan->Pdxln();
	m_pdxlnPlan->AddRef();

	m_ullPlanId = pphPlan->UllId();
	m_ullPlanSpaceSize = pphPlan->UllSpaceSize();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::ExtractMetadataObjects
//
//	@doc:
//		Extract metadata objects
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::ExtractMetadataObjects
	(
	CParseHandlerBase *pph
	)
{
	CParseHandlerMetadata *pphmd = dynamic_cast<CParseHandlerMetadata *>(pph);
	GPOS_ASSERT(NULL != pphmd && NULL != pphmd->Pdrgpmdobj());

	m_pdrgpmdobj = pphmd->Pdrgpmdobj();
	m_pdrgpmdobj->AddRef();
	
	m_pdrgpmdid = pphmd->Pdrgpmdid();
	m_pdrgpmdid->AddRef();

	m_pdrgpsysid = pphmd->Pdrgpsysid();

	if (NULL != m_pdrgpsysid)
	{
		m_pdrgpsysid->AddRef();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::ExtractStats
//
//	@doc:
//		Extract statistics
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::ExtractStats
	(
	CParseHandlerBase *pph
	)
{
	CParseHandlerStatistics *pphStats = dynamic_cast<CParseHandlerStatistics *>(pph);
	GPOS_ASSERT(NULL != pphStats);

	DrgPdxlstatsderrel *pdrgpdxlstatsderrel = pphStats->Pdrgpdxlstatsderrel();
	GPOS_ASSERT(NULL != pdrgpdxlstatsderrel);

	pdrgpdxlstatsderrel->AddRef();
	m_pdrgpdxlstatsderrel = pdrgpdxlstatsderrel;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::ExtractDXLQuery
//
//	@doc:
//		Extract DXL query
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::ExtractDXLQuery
	(
	CParseHandlerBase *pph
	)
{
	CParseHandlerQuery *pphquery = dynamic_cast<CParseHandlerQuery *>(pph);
	GPOS_ASSERT(NULL != pphquery && NULL != pphquery->Pdxln());

	m_pdxlnQuery = pphquery->Pdxln();
	m_pdxlnQuery->AddRef();

	GPOS_ASSERT(NULL != pphquery->PdrgpdxlnOutputCols());

	m_pdrgpdxlnOutputCols = pphquery->PdrgpdxlnOutputCols();
	m_pdrgpdxlnOutputCols->AddRef();
	
	m_pdrgpdxlnCTE = pphquery->PdrgpdxlnCTE();
	m_pdrgpdxlnCTE->AddRef();
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::ExtractMDRequest
//
//	@doc:
//		Extract mdids
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::ExtractMDRequest
	(
	CParseHandlerBase *pph
	)
{
	CParseHandlerMDRequest *pphMDRequest = dynamic_cast<CParseHandlerMDRequest *>(pph);
	GPOS_ASSERT(NULL != pphMDRequest && NULL != pphMDRequest->Pdrgpmdid());
	
	DrgPmdid *pdrgpmdid = pphMDRequest->Pdrgpmdid();
	CMDRequest::DrgPtr *pdrgptr = pphMDRequest->Pdrgptr();
	
	pdrgpmdid->AddRef();
	pdrgptr->AddRef();
	
	m_pmdr = GPOS_NEW(m_pmp) CMDRequest(m_pmp, pdrgpmdid, pdrgptr);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::ExtractSearchStrategy
//
//	@doc:
//		Extract search strategy
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::ExtractSearchStrategy
	(
	CParseHandlerBase *pph
	)
{
	CParseHandlerSearchStrategy *pphSearchStrategy = dynamic_cast<CParseHandlerSearchStrategy *>(pph);
	GPOS_ASSERT(NULL != pphSearchStrategy && NULL != pphSearchStrategy->Pdrgppss());

	DrgPss *pdrgpss = pphSearchStrategy->Pdrgppss();

	pdrgpss->AddRef();
	m_pdrgpss = pdrgpss;
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::ExtractCostParams
//
//	@doc:
//		Extract cost params
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::ExtractCostParams
	(
	CParseHandlerBase *pph
	)
{
	CParseHandlerCostParams *pphCostParams = dynamic_cast<CParseHandlerCostParams *>(pph);
	GPOS_ASSERT(NULL != pphCostParams && NULL != pphCostParams->Pcp());

	ICostModelParams *pcp = pphCostParams->Pcp();

	pcp->AddRef();
	m_pcp = pcp;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::ExtractScalarExpr
//
//	@doc:
//		Extract scalar expressions
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::ExtractScalarExpr
	(
	CParseHandlerBase *pph
	)
{
	CParseHandlerScalarExpr *pphScalarExpr = dynamic_cast<CParseHandlerScalarExpr *>(pph);
	GPOS_ASSERT(NULL != pphScalarExpr && NULL != pphScalarExpr->Pdxln());

	m_pdxlnScalarExpr = pphScalarExpr->Pdxln();
	m_pdxlnScalarExpr->AddRef();
}

// EOF
