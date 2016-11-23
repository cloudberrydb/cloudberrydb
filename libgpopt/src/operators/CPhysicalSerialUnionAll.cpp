//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalSerialUnionAll.cpp
//
//	@doc:
//		Implementation of physical union all operator
//
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/base/CDistributionSpecRandom.h"
#include "gpopt/base/CDistributionSpecNonSingleton.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CDrvdPropCtxtPlan.h"

#include "gpopt/operators/CPhysicalSerialUnionAll.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CScalarIdent.h"

#include "gpopt/exception.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSerialUnionAll::CPhysicalSerialUnionAll
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalSerialUnionAll::CPhysicalSerialUnionAll
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcrOutput,
	DrgDrgPcr *pdrgpdrgpcrInput,
	ULONG ulScanIdPartialIndex
	)
	:
	CPhysicalUnionAll(pmp, pdrgpcrOutput, pdrgpdrgpcrInput, ulScanIdPartialIndex)
{
	// UnionAll creates two distribution requests to enforce distribution of its children:
	// (1) (Hashed, Hashed): used to pass hashed distribution (requested from above)
	//     to child operators and match request Exactly
	// (2) (ANY, matching_distr): used to request ANY distribution from outer child, and
	//     match its response on the distribution requested from inner child

	SetDistrRequests(2 /*ulDistrReq*/);
	GPOS_ASSERT(0 < UlDistrRequests());
}

CPhysicalSerialUnionAll::~CPhysicalSerialUnionAll()
{
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSerialUnionAll::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalSerialUnionAll::PdsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CDistributionSpec *pdsRequired,
	ULONG ulChildIndex,
	DrgPdp *pdrgpdpCtxt,
	ULONG ulOptReq
	)
	const
{
	GPOS_ASSERT(NULL != PdrgpdrgpcrInput());
	GPOS_ASSERT(ulChildIndex < PdrgpdrgpcrInput()->UlLength());
	GPOS_ASSERT(2 > ulOptReq);

	CDistributionSpec *pds = PdsMasterOnlyOrReplicated(pmp, exprhdl, pdsRequired, ulChildIndex, ulOptReq);
	if (NULL != pds)
	{
		return pds;
	}

	if (0 == ulOptReq && CDistributionSpec::EdtHashed == pdsRequired->Edt())
	{
		// attempt passing requested hashed distribution to children
		CDistributionSpecHashed *pdshashed = PdshashedPassThru(pmp, CDistributionSpecHashed::PdsConvert(pdsRequired), ulChildIndex);
		if (NULL != pdshashed)
		{
			return pdshashed;
		}
	}

	if (0 == ulChildIndex)
	{
		// otherwise, ANY distribution is requested from outer child
		return GPOS_NEW(pmp) CDistributionSpecAny(this->Eopid());
	}

	// inspect distribution delivered by outer child
	CDistributionSpec *pdsOuter = CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();

	if (CDistributionSpec::EdtSingleton == pdsOuter->Edt() ||
		CDistributionSpec::EdtStrictSingleton == pdsOuter->Edt())
	{
		// outer child is Singleton, require inner child to have matching Singleton distribution
		return CPhysical::PdssMatching(pmp, CDistributionSpecSingleton::PdssConvert(pdsOuter));
	}

	if (CDistributionSpec::EdtUniversal == pdsOuter->Edt())
	{
		// require inner child to be on the master segment in order to avoid
		// duplicate values when doing UnionAll operation with Universal outer child
		// Example: select 1 union all select i from x;
		return GPOS_NEW(pmp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
	}

	if (CDistributionSpec::EdtReplicated == pdsOuter->Edt())
	{
		// outer child is replicated, require inner child to be replicated
		return GPOS_NEW(pmp) CDistributionSpecReplicated();
	}

	// outer child is non-replicated and is distributed across segments,
	// we need to the inner child to be distributed across segments that does
	// not generate duplicate results. That is, inner child should not be replicated.

	return GPOS_NEW(pmp) CDistributionSpecNonSingleton(false /*fAllowReplicated*/);
}

// EOF
