//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.

#include "gpopt/operators/CPhysicalParallelUnionAll.h"
#include "gpopt/base/CDistributionSpecRandom.h"
#include "gpopt/base/CDistributionSpecStrictHashed.h"
#include "gpopt/base/CDistributionSpecHashedNoOp.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CStrictHashedDistributions.h"

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
			m_pdrgpds(GPOS_NEW(pmp) CStrictHashedDistributions(pmp, pdrgpcrOutput, pdrgpdrgpcrInput))
	{
		// ParallelUnionAll creates two distribution requests to enforce distribution of its children:
		// (1) (StrictHashed, StrictHashed, ...): used to force redistribute motions that mirror the
		//     output columns
		// (2) (HashedNoOp, HashedNoOp, ...): used to force redistribution motions that mirror the
		//     underlying distribution of each relational child

		SetDistrRequests(2);
	}

	COperator::EOperatorId CPhysicalParallelUnionAll::Eopid() const
	{
		return EopPhysicalParallelUnionAll;
	}

	const CHAR *CPhysicalParallelUnionAll::SzId() const
	{
		return "CPhysicalParallelUnionAll";
	}

	CDistributionSpec *CPhysicalParallelUnionAll::PdsDerive(IMemoryPool *pmp, CExpressionHandle&) const
	{
		// This is not necessarily the optimal thing to do, but it's the safest, un-wrong thing to return
		// We will work harder at providing a better-effort answer here, à la CPhysicalSerialUnionAll::PdsDerive
		return GPOS_NEW(pmp) CDistributionSpecRandom();
	}

	CDistributionSpec *
	CPhysicalParallelUnionAll::PdsRequired
		(
			IMemoryPool *pmp,
			CExpressionHandle &,
			CDistributionSpec *,
			ULONG ulChildIndex,
			DrgPdp *,
			ULONG ulOptReq
		)
	const
	{
		if (0 == ulOptReq)
		{
			CDistributionSpec *pdsChild = (*m_pdrgpds)[ulChildIndex];
			pdsChild->AddRef();
			return pdsChild;
		}
		else
		{
			DrgPcr *pdrgpcrChildInputColumns = (*PdrgpdrgpcrInput())[ulChildIndex];
			DrgPexpr *pdrgpexprFakeRequestedColumns = GPOS_NEW(pmp) DrgPexpr(pmp);

			CColRef *pcrFirstColumn = (*pdrgpcrChildInputColumns)[0];
			CExpression *pexprScalarIdent = CUtils::PexprScalarIdent(pmp, pcrFirstColumn);
			pdrgpexprFakeRequestedColumns->Append(pexprScalarIdent);

			return GPOS_NEW(pmp) CDistributionSpecHashedNoOp(pdrgpexprFakeRequestedColumns);
		}
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
