//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.

#ifndef GPOPT_CPhysicalParallelUnionAll_H
#define GPOPT_CPhysicalParallelUnionAll_H

#include "gpopt/operators/CPhysicalUnionAll.h"

namespace gpopt
{
	// Operator that implements logical union all, but creates a slice for each
	// child relation to maximize concurrency.
	// See gpopt::CPhysicalSerialUnionAll for its serial sibling.
	class CPhysicalParallelUnionAll : public CPhysicalUnionAll
	{
		private:
			// array of child hashed distributions -- used locally for distribution derivation
			DrgPds *m_pdrgpds;

		public:
			CPhysicalParallelUnionAll(IMemoryPool *pmp, DrgPcr *pdrgpcrOutput, DrgDrgPcr *pdrgpdrgpcrInput,
									  ULONG ulScanIdPartialIndex);

			virtual EOperatorId Eopid() const;

			virtual const CHAR *SzId() const;

			virtual CDistributionSpec *
			PdsRequired(IMemoryPool *pmp, CExpressionHandle &exprhdl, CDistributionSpec *pdsRequired,
						ULONG ulChildIndex, DrgPdp *pdrgpdpCtxt, ULONG ulOptReq) const;

			virtual CDistributionSpec *PdsDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			virtual CEnfdProp::EPropEnforcingType
			EpetDistribution(CExpressionHandle &exprhdl, const CEnfdDistribution *ped) const;

			virtual
			CEnfdDistribution::EDistributionMatching Edm
				(
				CReqdPropPlan *, // prppInput
				ULONG,  // ulChildIndex
				DrgPdp *, //pdrgpdpCtxt
				ULONG // ulOptReq
				);

			virtual ~CPhysicalParallelUnionAll();
	};
}

#endif //GPOPT_CPhysicalParallelUnionAll_H
