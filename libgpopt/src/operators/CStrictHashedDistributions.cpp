//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.

#include "gpopt/operators/CStrictHashedDistributions.h"

using namespace gpopt;

CStrictHashedDistributions::CStrictHashedDistributions(IMemoryPool *pmp, DrgPcr *pdrgpcrOutput,
													   DrgDrgPcr *pdrgpdrgpcrInput)
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
			if (pcr->Pmdtype()->FHashable())
			{
				CExpression *pexpr = CUtils::PexprScalarIdent(pmp, pcr);
				pdrgpexpr->Append(pexpr);
			}
		}

		// create a hashed distribution on input columns of the current child
		BOOL fNullsColocated = true;
		CDistributionSpec *pdshashed = GPOS_NEW(pmp) CDistributionSpecStrictHashed(pdrgpexpr, fNullsColocated);
		Append(pdshashed);
	}
}
