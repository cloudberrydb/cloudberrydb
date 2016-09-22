//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.

#include "gpopt/operators/CStrictHashedDistributions.h"
#include "gpopt/base/CDistributionSpecRandom.h"

using namespace gpopt;

CStrictHashedDistributions::CStrictHashedDistributions
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
			CColRef *pcr = (*pdrgpcr)[ulCol];
			if (pcr->Pmdtype()->FRedistributable())
			{
				CExpression *pexpr = CUtils::PexprScalarIdent(pmp, pcr);
				pdrgpexpr->Append(pexpr);
			}
		}

		CDistributionSpec *pdshashed;
		ULONG ulColumnsToRedistribute = pdrgpexpr->UlLength();
		if (0 < ulColumnsToRedistribute)
		{
			// create a hashed distribution on input columns of the current child
			BOOL fNullsColocated = true;
			pdshashed = GPOS_NEW(pmp) CDistributionSpecStrictHashed(pdrgpexpr, fNullsColocated);
		}
		else
		{
			// None of the input columns are redistributable, but we want to
			// parallelize the relations we are concatenating, so we generate
			// a random redistribution.
			// When given a plan containing a "hash" redistribution on _no_ columns,
			// Some databases actually execute it as if it's a random redistribution.
			// We should not generate such a plan, for clarity and our own sanity

			pdshashed = GPOS_NEW(pmp) CDistributionSpecRandom();
			pdrgpexpr->Release();
		}
		Append(pdshashed);
	}
}
