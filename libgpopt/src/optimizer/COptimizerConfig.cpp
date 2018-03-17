//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		COptimizerConfig.cpp
//
//	@doc:
//		Implementation of configuration used by the optimizer
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/io/COstreamFile.h"

#include "gpopt/cost/ICostModel.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "gpos/common/CBitSetIter.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		COptimizerConfig::COptimizerConfig
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
COptimizerConfig::COptimizerConfig
	(
	CEnumeratorConfig *pec,
	CStatisticsConfig *pstatsconf,
	CCTEConfig *pcteconf,
	ICostModel *pcm,
	CHint *phint,
	CWindowOids *pwindowoids
	)
	:
	m_pec(pec),
	m_pstatsconf(pstatsconf),
	m_pcteconf(pcteconf),
	m_pcm(pcm),
	m_phint(phint),
	m_pwindowoids(pwindowoids)
{
	GPOS_ASSERT(NULL != pec);
	GPOS_ASSERT(NULL != pstatsconf);
	GPOS_ASSERT(NULL != pcteconf);
	GPOS_ASSERT(NULL != pcm);
	GPOS_ASSERT(NULL != phint);
	GPOS_ASSERT(NULL != m_pwindowoids);
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizerConfig::~COptimizerConfig
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
COptimizerConfig::~COptimizerConfig()
{
	m_pec->Release();
	m_pstatsconf->Release();
	m_pcteconf->Release();
	m_pcm->Release();
	m_phint->Release();
	m_pwindowoids->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizerConfig::PocDefault
//
//	@doc:
//		Default optimizer configuration
//
//---------------------------------------------------------------------------
COptimizerConfig *
COptimizerConfig::PoconfDefault
	(
	IMemoryPool *pmp
	)
{
	return GPOS_NEW(pmp) COptimizerConfig
						(
						GPOS_NEW(pmp) CEnumeratorConfig(pmp, 0 /*ullPlanId*/, 0 /*ullSamples*/),
						CStatisticsConfig::PstatsconfDefault(pmp),
						CCTEConfig::PcteconfDefault(pmp),
						ICostModel::PcmDefault(pmp),
						CHint::PhintDefault(pmp),
						CWindowOids::Pwindowoids(pmp)
						);
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizerConfig::PocDefault
//
//	@doc:
//		Default optimizer configuration with the given cost model
//
//---------------------------------------------------------------------------
COptimizerConfig *
COptimizerConfig::PoconfDefault
	(
	IMemoryPool *pmp,
	ICostModel *pcm
	)
{
	GPOS_ASSERT(NULL != pcm);
	
	return GPOS_NEW(pmp) COptimizerConfig
						(
						GPOS_NEW(pmp) CEnumeratorConfig(pmp, 0 /*ullPlanId*/, 0 /*ullSamples*/),
						CStatisticsConfig::PstatsconfDefault(pmp),
						CCTEConfig::PcteconfDefault(pmp),
						pcm,
						CHint::PhintDefault(pmp),
						CWindowOids::Pwindowoids(pmp)
						);
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizerConfig::Serialize
//
//	@doc:
//		Serialize optimizer configuration
//
//---------------------------------------------------------------------------
void
COptimizerConfig::Serialize(IMemoryPool *pmp, CXMLSerializer *pxmlser, CBitSet *pbsTrace) const
{

	GPOS_ASSERT(NULL != pxmlser);
	GPOS_ASSERT(NULL != pbsTrace);

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenOptimizerConfig));

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenEnumeratorConfig));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenPlanId), m_pec->UllPlanId());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenPlanSamples), m_pec->UllPlanId());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenCostThreshold), m_pec->UllPlanId());
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenEnumeratorConfig));

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenStatisticsConfig));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenDampingFactorFilter), m_pstatsconf->DDampingFactorFilter());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenDampingFactorJoin), m_pstatsconf->DDampingFactorJoin());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenDampingFactorGroupBy), m_pstatsconf->DDampingFactorGroupBy());
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenStatisticsConfig));

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCTEConfig));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenCTEInliningCutoff), m_pcteconf->UlCTEInliningCutoff());
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCTEConfig));

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenWindowOids));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenOidRowNumber), m_pwindowoids->OidRowNumber());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenOidRank), m_pwindowoids->OidRank());
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenWindowOids));

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCostModelConfig));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenCostModelType), m_pcm->Ecmt());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenSegmentsForCosting), m_pcm->UlHosts());
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCostModelConfig));

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenHint));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenMinNumOfPartsToRequireSortOnInsert), m_phint->UlMinNumOfPartsToRequireSortOnInsert());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenJoinArityForAssociativityCommutativity), m_phint->UlJoinArityForAssociativityCommutativity());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenArrayExpansionThreshold), m_phint->UlArrayExpansionThreshold());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenJoinOrderDPThreshold), m_phint->UlJoinOrderDPLimit());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenBroadcastThreshold), m_phint->UlBroadcastThreshold());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenEnforceConstraintsOnDML), m_phint->FEnforceConstraintsOnDML());
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenHint));

	// Serialize traceflags represented in bitset into stream
	gpos::CBitSetIter bsi(*pbsTrace);
	CWStringDynamic wsTraceFlags(pmp);
	for (ULONG ul = 0; bsi.FAdvance(); ul++)
	{
		if (0 < ul)
		{
			wsTraceFlags.AppendCharArray(",");
		}

		wsTraceFlags.AppendFormat(GPOS_WSZ_LIT("%d"), bsi.UlBit());
	}

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenTraceFlags));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenValue), &wsTraceFlags);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenTraceFlags));

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenOptimizerConfig));
}

// EOF
