//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.

#ifndef GPOPT_CHashedDistributions_H
#define GPOPT_CHashedDistributions_H

#include "gpopt/base/CDistributionSpec.h"
#include "gpos/memory/IMemoryPool.h"
#include "gpopt/base/CColRef.h"
#include "gpopt/base/CUtils.h"

namespace gpopt
{
	// Build hashed distributions used in physical union all during
	// distribution derivation. The class is an array of hashed
	// distribution on input column of each child, and an output hashed
	// distribution on UnionAll output columns

	template<typename TDistribution>
	class CHashedDistributions : public DrgPds
	{
		public:
			CHashedDistributions
			(
			IMemoryPool *pmp,
			DrgPcr *pdrgpcrOutput,
			DrgDrgPcr *pdrgpdrgpcrInput
			)
			:
			DrgPds(pmp)
			{
				const ULONG ulCols = pdrgpcrOutput->UlLength();
				const ULONG ulArity = pdrgpdrgpcrInput->UlLength();
				for (ULONG ulChild = 0; ulChild < ulArity; ulChild++)
				{
					DrgPcr *pdrgpcr = (*pdrgpdrgpcrInput)[ulChild];
					DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
					for (ULONG ulCol = 0; ulCol < ulCols; ulCol++)
					{
						CExpression *pexpr = CUtils::PexprScalarIdent(pmp, (*pdrgpcr)[ulCol]);
						pdrgpexpr->Append(pexpr);
					}

					// create a hashed distribution on input columns of the current child
					CDistributionSpec *pdshashed = GPOS_NEW(pmp) TDistribution(pdrgpexpr,
																			   true /*fNullsColocated*/);
					Append(pdshashed);
				}
			}
	};
}

#endif //GPOPT_CHashedDistributions_H
