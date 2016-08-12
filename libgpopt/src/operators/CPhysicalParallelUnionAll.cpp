//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.

#include "gpopt/operators/CPhysicalParallelUnionAll.h"
#include "gpopt/base/CDistributionSpecStrictHashed.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CHashedDistributions.h"

namespace gpopt
{
	CPhysicalParallelUnionAll::CPhysicalParallelUnionAll
		(
			IMemoryPool *pmp,
			DrgPcr *pdrgpcrOutput,
			DrgDrgPcr *pdrgpdrgpcrInput,
			ULONG ulScanIdPartialIndex
		) : CPhysicalUnionAll
		(
			pmp,
			pdrgpcrOutput,
			pdrgpdrgpcrInput,
			ulScanIdPartialIndex
		),
			m_pdrgpds(GPOS_NEW(pmp) CHashedDistributions<CDistributionSpecStrictHashed>(pmp, pdrgpcrOutput, pdrgpdrgpcrInput))
	{
	}

	COperator::EOperatorId CPhysicalParallelUnionAll::Eopid() const
	{
		return EopPhysicalParallelUnionAll;
	}

	const CHAR *CPhysicalParallelUnionAll::SzId() const
	{
		return "CPhysicalParallelUnionAll";
	}

	CDistributionSpec *CPhysicalParallelUnionAll::PdsDerive(IMemoryPool*, CExpressionHandle&) const
	{
		CDistributionSpec *pdsFirstChild = (*m_pdrgpds)[0];
		pdsFirstChild->AddRef();
		return pdsFirstChild;
	}

	CEnfdProp::EPropEnforcingType
	CPhysicalParallelUnionAll::EpetDistribution(CExpressionHandle& , const CEnfdDistribution* ) const
	{
		return CEnfdProp::EpetRequired;
	}

	CDistributionSpec *
	CPhysicalParallelUnionAll::PdsRequired
		(
			IMemoryPool *,
			CExpressionHandle &,
			CDistributionSpec *,
			ULONG ulChildIndex,
			DrgPdp *,
			ULONG
		)
	const
	{
		CDistributionSpec *pdsChild = (*m_pdrgpds)[ulChildIndex];
		pdsChild->AddRef();
		return pdsChild;
	}

	CEnfdDistribution::EDistributionMatching
	CPhysicalParallelUnionAll::Edm
		(
		CReqdPropPlan *, // prppInput
		ULONG,  // ulChildIndex
		DrgPdp *, //pdrgpdpCtxt
		ULONG // ulOptReq
		)
	{
		return CEnfdDistribution::EdmExact;
	}

	CPhysicalParallelUnionAll::~CPhysicalParallelUnionAll()
	{
		m_pdrgpds->Release();
	}
}
