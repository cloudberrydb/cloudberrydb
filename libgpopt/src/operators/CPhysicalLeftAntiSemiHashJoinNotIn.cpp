//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CPhysicalLeftAntiSemiHashJoinNotIn.cpp
//
//	@doc:
//		Implementation of left anti semi hash join operator with NotIn semantics
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
	IMemoryPool *mp,
	CExpressionArray *pdrgpexprOuterKeys,
	CExpressionArray *pdrgpexprInnerKeys
	)
	:
	CPhysicalLeftAntiSemiHashJoin(mp, pdrgpexprOuterKeys, pdrgpexprInnerKeys)
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
	IMemoryPool *mp,
	CExpressionHandle &exprhdl,
	CDistributionSpec *pdsInput,
	ULONG child_index,
	CDrvdProp2dArray *pdrgpdpCtxt,
	ULONG ulOptReq // identifies which optimization request should be created
	)
	const
{
	GPOS_ASSERT(2 > child_index);
	GPOS_ASSERT(ulOptReq < UlDistrRequests());

	if (0 == ulOptReq && 1 == child_index &&
			(FNullableHashKeys(exprhdl.GetRelationalProperties(0)->PcrsNotNull(), false /*fInner*/) ||
			FNullableHashKeys(exprhdl.GetRelationalProperties(1)->PcrsNotNull(), true /*fInner*/)) )
	{
		// we need to replicate the inner if any of the following is true:
		// a. if the outer hash keys are nullable, because the executor needs to detect
		//	  whether the inner is empty, and this needs to be detected everywhere
		// b. if the inner hash keys are nullable, because every segment needs to
		//	  detect nulls coming from the inner child
		return GPOS_NEW(mp) CDistributionSpecReplicated();
	}

	return CPhysicalHashJoin::PdsRequired(mp, exprhdl, pdsInput, child_index, pdrgpdpCtxt, ulOptReq);
}

// EOF

