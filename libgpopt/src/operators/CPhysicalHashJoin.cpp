//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalHashJoin.cpp
//
//	@doc:
//		Implementation of hash join operator
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/sync/CAutoMutex.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CCastUtils.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/base/CDistributionSpecNonSingleton.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/CPartIndexMap.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalHashJoin.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CScalarConst.h"

using namespace gpopt;

// number of non-redistribute requests created by hash join
#define GPOPT_NON_HASH_DIST_REQUESTS	3

// maximum number of redistribute requests on single hash join keys
#define GPOPT_MAX_HASH_DIST_REQUESTS	6

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::CPhysicalHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalHashJoin::CPhysicalHashJoin
	(
	IMemoryPool *mp,
	CExpressionArray *pdrgpexprOuterKeys,
	CExpressionArray *pdrgpexprInnerKeys
	)
	:
	CPhysicalJoin(mp),
	m_pdrgpexprOuterKeys(pdrgpexprOuterKeys),
	m_pdrgpexprInnerKeys(pdrgpexprInnerKeys),
	m_pdrgpdsRedistributeRequests(NULL)
{
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pdrgpexprOuterKeys);
	GPOS_ASSERT(NULL != pdrgpexprInnerKeys);
	GPOS_ASSERT(pdrgpexprOuterKeys->Size() == pdrgpexprInnerKeys->Size());

	CreateHashRedistributeRequests(mp);

	// given an optimization context, HJN creates three optimization requests
	// to enforce distribution of its children:
	// Req(1 to N) (redistribute, redistribute), where we request the first hash join child
	//		to be distributed on single hash join keys separately, as well as the set
	//		of all hash join keys,
	//		the second hash join child is always required to match the distribution returned
	//		by first child
	// Req(N + 1) (hashed, broadcast)
	// Req(N + 2) (non-singleton, broadcast)
	// Req(N + 3) (singleton, singleton)

	ULONG ulDistrReqs = GPOPT_NON_HASH_DIST_REQUESTS + m_pdrgpdsRedistributeRequests->Size();
	SetDistrRequests(ulDistrReqs);

	// With DP enabled, there are several (max 10 controlled by macro)
	// alternatives generated for a join tree and during optimization of those
	// alternatives expressions PS is inserted in almost all the groups possibly.
	// However, if DP is turned off, i.e in query or greedy join order,
	// PS must be inserted in the group with DTS else in some cases HJ plan
	// cannot be created. So, to ensure pushing PS without DPE 2 partition
	// propagation request are required if DP is disabled.
	//    Req 0 => Push PS with considering DPE possibility
	//    Req 1 => Push PS without considering DPE possibility
	// Ex case: select * from non_part_tbl1 t1, part_tbl t2, non_part_tbl2 t3
	// where t1.b = t2.b and t2.b = t3.b;
	// Note: b is the partitioned column for part_tbl. If DP is turned off, HJ
	// will not be created for the above query if we send only 1 request.
	// Also, increasing the number of request increases the optimization time, so
	// set 2 only when needed.
	if (GPOPT_FDISABLED_XFORM(CXform::ExfExpandNAryJoinDP))
		SetPartPropagateRequests(2);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::~CPhysicalHashJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalHashJoin::~CPhysicalHashJoin()
{
	m_pdrgpexprOuterKeys->Release();
	m_pdrgpexprInnerKeys->Release();
	CRefCount::SafeRelease(m_pdrgpdsRedistributeRequests);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::CreateHashRedistributeRequests
//
//	@doc:
//		Create the set of redistribute requests to send to first
//		hash join child
//
//---------------------------------------------------------------------------
void
CPhysicalHashJoin::CreateHashRedistributeRequests
	(
	IMemoryPool *mp
	)
{
	GPOS_ASSERT(NULL == m_pdrgpdsRedistributeRequests);
	GPOS_ASSERT(NULL != m_pdrgpexprOuterKeys);
	GPOS_ASSERT(NULL != m_pdrgpexprInnerKeys);

	CExpressionArray *pdrgpexpr = NULL;
	if (EceoRightToLeft == Eceo())
	{
		pdrgpexpr = m_pdrgpexprInnerKeys;
	}
	else
	{
		pdrgpexpr = m_pdrgpexprOuterKeys;
	}

	m_pdrgpdsRedistributeRequests = GPOS_NEW(mp) CDistributionSpecArray(mp);
	const ULONG ulExprs = std::min((ULONG) GPOPT_MAX_HASH_DIST_REQUESTS, pdrgpexpr->Size());
	if (1 < ulExprs)
	{
		for (ULONG ul = 0; ul < ulExprs; ul++)
		{
			CExpressionArray *pdrgpexprCurrent = GPOS_NEW(mp) CExpressionArray(mp);
			CExpression *pexpr = (*pdrgpexpr)[ul];
			pexpr->AddRef();
			pdrgpexprCurrent->Append(pexpr);

			// add a separate request for each hash join key

			// TODO:  - Dec 30, 2011; change fNullsColocated to false when our
			// distribution matching can handle differences in NULL colocation
			CDistributionSpecHashed *pdshashedCurrent = GPOS_NEW(mp) CDistributionSpecHashed(pdrgpexprCurrent, true /* fNullsCollocated */);
			m_pdrgpdsRedistributeRequests->Append(pdshashedCurrent);
		}
	}
	// add a request that contains all hash join keys
	pdrgpexpr->AddRef();
	CDistributionSpecHashed *pdshashed = GPOS_NEW(mp) CDistributionSpecHashed(pdrgpexpr, true /* fNullsCollocated */);
	m_pdrgpdsRedistributeRequests->Append(pdshashed);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalHashJoin::PosRequired
	(
	IMemoryPool *mp,
	CExpressionHandle &, //exprhdl
	COrderSpec *, // posInput,
	ULONG
#ifdef GPOS_DEBUG
	child_index
#endif // GPOS_DEBUG
	,
	CDrvdProp2dArray *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(child_index < 2 &&
				"Required sort order can be computed on the relational child only");

	// hash join does not have order requirements to both children, and it
	// does not preserve any sort order
	return GPOS_NEW(mp) COrderSpec(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalHashJoin::PrsRequired
	(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl,
	CRewindabilitySpec *prsRequired,
	ULONG child_index,
	CDrvdProp2dArray *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(child_index < 2 &&
				"Required rewindability can be computed on the relational child only");

	// if there are outer references, then we need a materialize on both children
	if (exprhdl.HasOuterRefs())
	{
		return GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtRewindable, prsRequired->Emht());
	}

	if (1 == child_index)
	{
		// inner child does not have to be rewindable
		return GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtNotRewindable, prsRequired->Emht());
	}
		
	// pass through requirements to outer child
	return PrsPassThru(mp, exprhdl, prsRequired, 0 /*child_index*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PdsMatch
//
//	@doc:
//		Compute a distribution matching the distribution delivered by
//		given child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalHashJoin::PdsMatch
	(
	IMemoryPool *mp,
	CDistributionSpec *pds,
	ULONG ulSourceChildIndex
	)
	const
{
	GPOS_ASSERT(NULL != pds);

	EChildExecOrder eceo = Eceo();

	// check the type of distribution delivered by first child
	switch (pds->Edt())
	{
		case CDistributionSpec::EdtUniversal:
			// first child is universal, request second child to execute on the master to avoid duplicates
			return GPOS_NEW(mp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);

		case CDistributionSpec::EdtSingleton:
		case CDistributionSpec::EdtStrictSingleton:
			// require second child to provide a matching singleton distribution
			return PdssMatching(mp, CDistributionSpecSingleton::PdssConvert(pds));

		case CDistributionSpec::EdtHashed:
			// require second child to provide a matching hashed distribution
			return PdshashedMatching(mp, CDistributionSpecHashed::PdsConvert(pds), ulSourceChildIndex);

		default:
			GPOS_ASSERT(CDistributionSpec::EdtReplicated == pds->Edt());
			if (EceoRightToLeft == eceo)
			{
				GPOS_ASSERT(1 == ulSourceChildIndex);

				// inner child is replicated, request outer child to have non-singleton distribution
				return GPOS_NEW(mp) CDistributionSpecNonSingleton();
			}

			GPOS_ASSERT(0 == ulSourceChildIndex);

			// outer child is replicated, replicate inner child too in order to preserve correctness of semi-join
			return GPOS_NEW(mp) CDistributionSpecReplicated();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PdshashedMatching
//
//	@doc:
//		Compute a hashed distribution matching the given distribution
//
//---------------------------------------------------------------------------
CDistributionSpecHashed *
CPhysicalHashJoin::PdshashedMatching
	(
	IMemoryPool *mp,
	CDistributionSpecHashed *pdshashed,
	ULONG ulSourceChild // index of child that delivered the given hashed distribution
	)
	const
{
	GPOS_ASSERT(2 > ulSourceChild);

	CExpressionArray *pdrgpexprSource = m_pdrgpexprOuterKeys;
	CExpressionArray *pdrgpexprTarget = m_pdrgpexprInnerKeys;
	if (1 == ulSourceChild)
	{
		pdrgpexprSource = m_pdrgpexprInnerKeys;
		pdrgpexprTarget = m_pdrgpexprOuterKeys;
	}

	const CExpressionArray *pdrgpexprDist = pdshashed->Pdrgpexpr();
	const ULONG ulDlvrdSize = pdrgpexprDist->Size();
	const ULONG ulSourceSize = pdrgpexprSource->Size();

	CExpressionArray *pdrgpexprSourceNoCast = GPOS_NEW(mp) CExpressionArray(mp);
	CExpressionArray *pdrgpexprTargetNoCast = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < ulSourceSize; ul++)
	{
		CExpression *pexpr = CCastUtils::PexprWithoutBinaryCoercibleCasts((*pdrgpexprSource)[ul]);
		pexpr->AddRef();
		pdrgpexprSourceNoCast->Append(pexpr);

		pexpr = CCastUtils::PexprWithoutBinaryCoercibleCasts((*pdrgpexprTarget)[ul]);
		pexpr->AddRef();
		pdrgpexprTargetNoCast->Append(pexpr);
	}

	// construct an array of target key expressions matching source key expressions
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ulDlvrdIdx = 0; ulDlvrdIdx < ulDlvrdSize; ulDlvrdIdx++)
	{
		CExpression *pexprDlvrd = CCastUtils::PexprWithoutBinaryCoercibleCasts((*pdrgpexprDist)[ulDlvrdIdx]);
		for (ULONG idx = 0; idx < ulSourceSize; idx++)
		{
			if (CUtils::Equals(pexprDlvrd, (*pdrgpexprSourceNoCast)[idx]))
			{
				// TODO: 02/21/2012 - ; source column may be mapped to multiple
				// target columns (e.g. i=j and i=k);
				// in this case, we need to generate multiple optimization requests to the target child
				CExpression *pexprTarget = (*pdrgpexprTargetNoCast)[idx];
				pexprTarget->AddRef();
				pdrgpexpr->Append(pexprTarget);
				break;
			}
		}
	}

	pdrgpexprSourceNoCast->Release();
	pdrgpexprTargetNoCast->Release();

	// check if we failed to compute required distribution
	if (pdrgpexpr->Size() != ulDlvrdSize)
	{
		pdrgpexpr->Release();
		if (NULL != pdshashed->PdshashedEquiv())
		{
			// try again using the equivalent hashed distribution
			return PdshashedMatching(mp, pdshashed->PdshashedEquiv(), ulSourceChild);
		}
	}
	GPOS_ASSERT(pdrgpexpr->Size() == ulDlvrdSize);

	return GPOS_NEW(mp) CDistributionSpecHashed(pdrgpexpr, true /* fNullsCollocated */);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PdsRequiredSingleton
//
//	@doc:
//		Create (singleton, singleton) optimization request
//
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalHashJoin::PdsRequiredSingleton
	(
	IMemoryPool *mp,
	CExpressionHandle  &, // exprhdl
	CDistributionSpec *, // pdsInput
	ULONG  child_index,
	CDrvdProp2dArray *pdrgpdpCtxt
	)
	const
{
	if (FFirstChildToOptimize(child_index))
	{
		// require first child to be singleton
		return GPOS_NEW(mp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
	}

	// require a matching distribution from second child
	GPOS_ASSERT(NULL != pdrgpdpCtxt);
	CDistributionSpec *pdsFirst = CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();
	GPOS_ASSERT(NULL != pdsFirst);

	if (CDistributionSpec::EdtUniversal == pdsFirst->Edt())
	{
		// first child is universal, request second child to execute on the master to avoid duplicates
		return GPOS_NEW(mp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
	}

	GPOS_ASSERT(CDistributionSpec::EdtSingleton == pdsFirst->Edt() ||
		CDistributionSpec::EdtStrictSingleton == pdsFirst->Edt());

	// require second child to have matching singleton distribution
	return CPhysical::PdssMatching(mp, CDistributionSpecSingleton::PdssConvert(pdsFirst));
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PdsRequiredReplicate
//
//	@doc:
//		Create (hashed/non-singleton, broadcast) optimization request
//
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalHashJoin::PdsRequiredReplicate
	(
	IMemoryPool *mp,
	CExpressionHandle  &exprhdl,
	CDistributionSpec *pdsInput,
	ULONG  child_index,
	CDrvdProp2dArray *pdrgpdpCtxt,
	ULONG ulOptReq
	)
	const
{
	EChildExecOrder eceo = Eceo();
	if (EceoLeftToRight == eceo)
	{
		// if optimization order is left to right, fall-back to implementation of parent Join operator
		return CPhysicalJoin::PdsRequired(mp, exprhdl, pdsInput, child_index, pdrgpdpCtxt, 0 /*ulOptReq*/);
	}
	GPOS_ASSERT(EceoRightToLeft == eceo);

	if (1 == child_index)
	{
		// require inner child to be replicated
		return GPOS_NEW(mp) CDistributionSpecReplicated();
	}
	GPOS_ASSERT(0 == child_index);

	// require a matching distribution from outer child
	CDistributionSpec *pdsInner = CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();
	GPOS_ASSERT(NULL != pdsInner);

	if (CDistributionSpec::EdtUniversal == pdsInner->Edt())
	{
		// first child is universal, request second child to execute on the master to avoid duplicates
		return GPOS_NEW(mp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
	}

	if (ulOptReq == m_pdrgpdsRedistributeRequests->Size() &&
		CDistributionSpec::EdtHashed == pdsInput->Edt())
	{
		// attempt to propagate hashed request to child
		CDistributionSpecHashed *pdshashed =
			PdshashedPassThru(mp, exprhdl, CDistributionSpecHashed::PdsConvert(pdsInput), child_index, pdrgpdpCtxt, ulOptReq);
		if (NULL != pdshashed)
		{
			return pdshashed;
		}
	}

	// otherwise, require second child to deliver non-singleton distribution
	GPOS_ASSERT(CDistributionSpec::EdtReplicated == pdsInner->Edt());
	return GPOS_NEW(mp) CDistributionSpecNonSingleton();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PdshashedPassThru
//
//	@doc:
//		Create a child hashed distribution request based on input hashed
//		distribution,
//		return NULL if no such request can be created
//
//
//---------------------------------------------------------------------------
CDistributionSpecHashed *
CPhysicalHashJoin::PdshashedPassThru
	(
	IMemoryPool *mp,
	CExpressionHandle  &exprhdl,
	CDistributionSpecHashed *pdshashedInput,
	ULONG  , // child_index
	CDrvdProp2dArray *, // pdrgpdpCtxt
	ULONG
#ifdef GPOS_DEBUG
	 ulOptReq
#endif // GPOS_DEBUG
	)
	const
{
	GPOS_ASSERT(ulOptReq == m_pdrgpdsRedistributeRequests->Size());
	GPOS_ASSERT(NULL != pdshashedInput);

	if (!GPOS_FTRACE(EopttraceEnableRedistributeBroadcastHashJoin))
	{
		// this option is disabled
		return NULL;
	}

	// since incoming request is hashed, we attempt here to propagate this request to outer child
	CColRefSet *pcrsOuterOutput = exprhdl.GetRelationalProperties(0 /*child_index*/)->PcrsOutput();
	CExpressionArray *pdrgpexprIncomingRequest = pdshashedInput->Pdrgpexpr();
	CColRefSet *pcrsAllUsed = CUtils::PcrsExtractColumns(mp, pdrgpexprIncomingRequest);
	BOOL fSubset = pcrsOuterOutput->ContainsAll(pcrsAllUsed);
	BOOL fDisjoint = pcrsOuterOutput->IsDisjoint(pcrsAllUsed);
	pcrsAllUsed->Release();
	if (fSubset)
	{
		// incoming request uses columns from outer child only, pass it through
		pdshashedInput->AddRef();
		return pdshashedInput;
	}

	if (!fDisjoint)
	{
		 // incoming request intersects with columns from outer child,
		 // we restrict the request to outer child columns only, then we pass it through
		 CExpressionArray *pdrgpexprChildRequest = GPOS_NEW(mp) CExpressionArray(mp);
		 const ULONG size = pdrgpexprIncomingRequest->Size();
		 for (ULONG ul = 0; ul < size; ul++)
		 {
			 CExpression *pexpr = (*pdrgpexprIncomingRequest)[ul];
			 CColRefSet *pcrsUsed = CDrvdPropScalar::GetDrvdScalarProps(pexpr->PdpDerive())->PcrsUsed();
			 if (pcrsOuterOutput->ContainsAll(pcrsUsed))
			 {
				 // hashed expression uses columns from outer child only, add it to request
				 pexpr->AddRef();
				 pdrgpexprChildRequest->Append(pexpr);
			 }
		 }
		 GPOS_ASSERT(0 < pdrgpexprChildRequest->Size());

		 CDistributionSpecHashed *pdshashed = GPOS_NEW(mp) CDistributionSpecHashed(pdrgpexprChildRequest, pdshashedInput->FNullsColocated());

		 // since the other child of the join is replicated, we need to enforce hashed-distribution across segments here
		 pdshashed->MarkUnsatisfiableBySingleton();

		 return pdshashed;
	}

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PdsRequiredRedistribute
//
//	@doc:
//		Compute (redistribute, redistribute) optimization request
//
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalHashJoin::PdsRequiredRedistribute
	(
	IMemoryPool *mp,
	CExpressionHandle &, // exprhdl
	CDistributionSpec *, // pdsInput
	ULONG  child_index,
	CDrvdProp2dArray *pdrgpdpCtxt,
	ULONG ulOptReq
	)
	const
{
	if (FFirstChildToOptimize(child_index))
	{
		// require first child to provide a hashed distribution,
		return PdshashedRequired(mp, child_index, ulOptReq);
	}

	// find the distribution delivered by first child
	CDistributionSpec *pdsFirst = CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();
	GPOS_ASSERT(NULL != pdsFirst);

	// find the index of the first child
	ULONG ulFirstChild = 0;
	if (EceoRightToLeft == Eceo())
	{
		ulFirstChild = 1;
	}

	// return a matching distribution request for the second child
	return PdsMatch(mp, pdsFirst, ulFirstChild);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child;
//		this function creates three optimization requests to join children:
//		Req(1 to N) (redistribute, redistribute), where we request the first hash join child
//			to be distributed on single hash join keys separately, as well as the set
//			of all hash join keys,
//			the second hash join child is always required to match the distribution returned
//			by first child
// 		Req(N + 1) (hashed, broadcast)
// 		Req(N + 2) (non-singleton, broadcast)
// 		Req(N + 3) (singleton, singleton)
//
//		we always check the distribution delivered by the first child (as
//		given by child optimization order), and then match the delivered
//		distribution on the second child
//
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalHashJoin::PdsRequired
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

	// if expression has to execute on master then we need a gather
	if (exprhdl.FMasterOnly())
	{
		return PdsEnforceMaster(mp, exprhdl, pdsInput, child_index);
	}

	if (exprhdl.HasOuterRefs())
	{
		if (CDistributionSpec::EdtSingleton == pdsInput->Edt() ||
			CDistributionSpec::EdtReplicated == pdsInput->Edt())
		{
			return PdsPassThru(mp, exprhdl, pdsInput, child_index);
		}
		return GPOS_NEW(mp) CDistributionSpecReplicated();
	}

	const ULONG ulHashDistributeRequests = m_pdrgpdsRedistributeRequests->Size();
	if (ulOptReq < ulHashDistributeRequests)
	{
		// requests 1 .. N are (redistribute, redistribute)
		return PdsRequiredRedistribute(mp, exprhdl, pdsInput, child_index, pdrgpdpCtxt, ulOptReq);
	}

	if (ulOptReq == ulHashDistributeRequests ||
		ulOptReq == ulHashDistributeRequests + 1)
	{
		// requests N+1, N+2 are (hashed/non-singleton, replicate)

		return PdsRequiredReplicate(mp, exprhdl, pdsInput, child_index, pdrgpdpCtxt, ulOptReq);
	}

	GPOS_ASSERT(ulOptReq == ulHashDistributeRequests + 2);

	// requests N+3 is (singleton, singleton)

	return PdsRequiredSingleton(mp, exprhdl, pdsInput, child_index, pdrgpdpCtxt);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PdshashedRequired
//
//	@doc:
//		Compute required hashed distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpecHashed *
CPhysicalHashJoin::PdshashedRequired
	(
	IMemoryPool *, // mp
	ULONG, // child_index
	ULONG ulReqIndex
	)
	const
{
	GPOS_ASSERT(ulReqIndex < m_pdrgpdsRedistributeRequests->Size());
	CDistributionSpec *pds = (*m_pdrgpdsRedistributeRequests)[ulReqIndex];

	pds->AddRef();
	return CDistributionSpecHashed::PdsConvert(pds);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator;
//
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalHashJoin::EpetOrder
	(
	CExpressionHandle &, // exprhdl
	const CEnfdOrder *
#ifdef GPOS_DEBUG
	peo
#endif // GPOS_DEBUG
	)
	const
{
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

	// hash join is not order-preserving;
	// any order requirements have to be enforced on top
	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::FNullableInnerHashKeys
//
//	@doc:
//		Check whether the hash keys from one child are nullable. pcrsNotNull must
//		be all the "not null" columns coming from that child
//
//---------------------------------------------------------------------------
BOOL
CPhysicalHashJoin::FNullableHashKeys
	(
	CColRefSet *pcrsNotNull,
	BOOL fInner
	)
	const
{
	ULONG ulHashKeys = 0;
	if (fInner)
	{
		ulHashKeys = m_pdrgpexprInnerKeys->Size();
	}
	else
	{
		ulHashKeys = m_pdrgpexprOuterKeys->Size();
	}

	for (ULONG ul = 0; ul < ulHashKeys; ul++)
	{
		if (FNullableHashKey(ul, pcrsNotNull, fInner))
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::FNullableHashKey
//
//	@doc:
//		Check whether a hash key is nullable
//
//---------------------------------------------------------------------------
BOOL
CPhysicalHashJoin::FNullableHashKey
	(
	ULONG ulKey,
	CColRefSet *pcrsNotNull,
	BOOL fInner
	)
	const
{
	COperator *pop = NULL;
	if (fInner)
	{
		pop = (*m_pdrgpexprInnerKeys)[ulKey]->Pop();
	}
	else
	{
		pop = (*m_pdrgpexprOuterKeys)[ulKey]->Pop();
	}
	EOperatorId op_id = pop->Eopid();

	if (COperator::EopScalarIdent == op_id)
	{
		const CColRef *colref = CScalarIdent::PopConvert(pop)->Pcr();
		return (!pcrsNotNull->FMember(colref));
	}

	if (COperator::EopScalarConst == op_id)
	{
		return CScalarConst::PopConvert(pop)->GetDatum()->IsNull();
	}

	// be conservative for all other scalar expressions where we cannot easily
	// determine nullability
	return true;
}


// EOF
