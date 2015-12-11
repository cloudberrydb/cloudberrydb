//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CPhysicalLeftAntiSemiHashJoinNotIn.cpp
//
//	@doc:
//		Implementation of left anti semi hash join operator with NotIn semantics
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/operators/CPhysicalLeftAntiSemiHashJoinNotIn.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLeftAntiSemiHashJoinNotIn::CPhysicalLeftAntiSemiHashJoinNotIn
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalLeftAntiSemiHashJoinNotIn::CPhysicalLeftAntiSemiHashJoinNotIn
	(
	IMemoryPool *pmp,
	DrgPexpr *pdrgpexprOuterKeys,
	DrgPexpr *pdrgpexprInnerKeys
	)
	:
	CPhysicalLeftAntiSemiHashJoin(pmp, pdrgpexprOuterKeys, pdrgpexprInnerKeys)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLeftAntiSemiHashJoinNotIn::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalLeftAntiSemiHashJoinNotIn::PdsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CDistributionSpec *pdsInput,
	ULONG ulChildIndex,
	DrgPdp *pdrgpdpCtxt,
	ULONG ulOptReq // identifies which optimization request should be created
	)
	const
{
	GPOS_ASSERT(2 > ulChildIndex);
	GPOS_ASSERT(ulOptReq < UlDistrRequests());

	if (0 == ulOptReq && 1 == ulChildIndex &&
			(FNullableHashKeys(exprhdl.Pdprel(0)->PcrsNotNull(), false /*fInner*/) ||
			FNullableHashKeys(exprhdl.Pdprel(1)->PcrsNotNull(), true /*fInner*/)) )
	{
		// we need to replicate the inner if any of the following is true:
		// a. if the outer hash keys are nullable, because the executor needs to detect
		//	  whether the inner is empty, and this needs to be detected everywhere
		// b. if the inner hash keys are nullable, because every segment needs to
		//	  detect nulls coming from the inner child
		return GPOS_NEW(pmp) CDistributionSpecReplicated();
	}

	return CPhysicalHashJoin::PdsRequired(pmp, exprhdl, pdsInput, ulChildIndex, pdrgpdpCtxt, ulOptReq);
}

// EOF

