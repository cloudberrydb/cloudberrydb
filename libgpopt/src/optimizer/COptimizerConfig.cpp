//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		COptimizerConfig.cpp
//
//	@doc:
//		Implementation of configuration used by the optimizer
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/io/COstreamFile.h"

#include "gpopt/cost/ICostModel.h"
#include "gpopt/optimizer/COptimizerConfig.h"

using namespace gpos;
using namespace gpopt;
using namespace gpnaucrates;

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

// EOF
