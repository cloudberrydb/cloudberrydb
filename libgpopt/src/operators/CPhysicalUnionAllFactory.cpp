//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.


#include "gpopt/operators/CPhysicalUnionAllFactory.h"
#include "gpopt/operators/CPhysicalSerialUnionAll.h"
#include "gpopt/operators/CPhysicalParallelUnionAll.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/exception.h"
#include "gpos/base.h"

namespace gpopt
{

	CPhysicalUnionAllFactory::CPhysicalUnionAllFactory
		(
			CLogicalUnionAll *popLogicalUnionAll,
			BOOL fParallel
		)
		: m_popLogicalUnionAll(popLogicalUnionAll), m_fParallel(fParallel) { }

	CPhysicalUnionAll *CPhysicalUnionAllFactory::PopPhysicalUnionAll(IMemoryPool *pmp)
	{

		DrgPcr *pdrgpcrOutput = m_popLogicalUnionAll->PdrgpcrOutput();
		DrgDrgPcr *pdrgpdrgpcrInput = m_popLogicalUnionAll->PdrgpdrgpcrInput();

		// TODO:  May 2nd 2012; support compatible types
		if (!CXformUtils::FSameDatatype(pdrgpdrgpcrInput))
		{
			GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedOp, GPOS_WSZ_LIT("Union of non-identical types"));
		}

		pdrgpcrOutput->AddRef();
		pdrgpdrgpcrInput->AddRef();

		if (m_fParallel)
		{
			return GPOS_NEW(pmp) CPhysicalParallelUnionAll
				(
					pmp,
					pdrgpcrOutput,
					pdrgpdrgpcrInput,
					m_popLogicalUnionAll->UlScanIdPartialIndex()
				);
		}
		else
		{
			return GPOS_NEW(pmp) CPhysicalSerialUnionAll
				(
					pmp,
					pdrgpcrOutput,
					pdrgpdrgpcrInput,
					m_popLogicalUnionAll->UlScanIdPartialIndex()
				);

		}

	}

}
