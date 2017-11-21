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
	IMemoryPool *pmp,
	DrgPexpr *pdrgpexprOuterKeys,
	DrgPexpr *pdrgpexprInnerKeys
	)
	:
	CPhysicalJoin(pmp),
	m_pdrgpexprOuterKeys(pdrgpexprOuterKeys),
	m_pdrgpexprInnerKeys(pdrgpexprInnerKeys),
	m_pdrgpdsRedistributeRequests(NULL)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pdrgpexprOuterKeys);
	GPOS_ASSERT(NULL != pdrgpexprInnerKeys);
	GPOS_ASSERT(pdrgpexprOuterKeys->UlLength() == pdrgpexprInnerKeys->UlLength());

	CreateHashRedistributeRequests(pmp);

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

	ULONG ulDistrReqs = GPOPT_NON_HASH_DIST_REQUESTS + m_pdrgpdsRedistributeRequests->UlLength();
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
	IMemoryPool *pmp
	)
{
	GPOS_ASSERT(NULL == m_pdrgpdsRedistributeRequests);
	GPOS_ASSERT(NULL != m_pdrgpexprOuterKeys);
	GPOS_ASSERT(NULL != m_pdrgpexprInnerKeys);

	DrgPexpr *pdrgpexpr = NULL;
	if (EceoRightToLeft == Eceo())
	{
		pdrgpexpr = m_pdrgpexprInnerKeys;
	}
	else
	{
		pdrgpexpr = m_pdrgpexprOuterKeys;
	}

	m_pdrgpdsRedistributeRequests = GPOS_NEW(pmp) DrgPds(pmp);
	const ULONG ulExprs = std::min((ULONG) GPOPT_MAX_HASH_DIST_REQUESTS, pdrgpexpr->UlLength());
	if (1 < ulExprs)
	{
		for (ULONG ul = 0; ul < ulExprs; ul++)
		{
			DrgPexpr *pdrgpexprCurrent = GPOS_NEW(pmp) DrgPexpr(pmp);
			CExpression *pexpr = (*pdrgpexpr)[ul];
			pexpr->AddRef();
			pdrgpexprCurrent->Append(pexpr);

			// add a separate request for each hash join key

			// TODO:  - Dec 30, 2011; change fNullsColocated to false when our
			// distribution matching can handle differences in NULL colocation
			CDistributionSpecHashed *pdshashedCurrent = GPOS_NEW(pmp) CDistributionSpecHashed(pdrgpexprCurrent, true /* fNullsCollocated */);
			m_pdrgpdsRedistributeRequests->Append(pdshashedCurrent);
		}
	}
	// add a request that contains all hash join keys
	pdrgpexpr->AddRef();
	CDistributionSpecHashed *pdshashed = GPOS_NEW(pmp) CDistributionSpecHashed(pdrgpexpr, true /* fNullsCollocated */);
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
	IMemoryPool *pmp,
	CExpressionHandle &, //exprhdl
	COrderSpec *, // posInput,
	ULONG
#ifdef GPOS_DEBUG
	ulChildIndex
#endif // GPOS_DEBUG
	,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(ulChildIndex < 2 &&
				"Required sort order can be computed on the relational child only");

	// hash join does not have order requirements to both children, and it
	// does not preserve any sort order
	return GPOS_NEW(pmp) COrderSpec(pmp);
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
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CRewindabilitySpec *prsRequired,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(ulChildIndex < 2 &&
				"Required rewindability can be computed on the relational child only");

	// if there are outer references, then we need a materialize on both children
	if (exprhdl.FHasOuterRefs())
	{
		return GPOS_NEW(pmp) CRewindabilitySpec(CRewindabilitySpec::ErtGeneral);
	}

	if (1 == ulChildIndex)
	{
		// inner child does not have to be rewindable
		return GPOS_NEW(pmp) CRewindabilitySpec(CRewindabilitySpec::ErtNone /*ert*/);
	}
		
	// pass through requirements to outer child
	return PrsPassThru(pmp, exprhdl, prsRequired, 0 /*ulChildIndex*/);
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
	IMemoryPool *pmp,
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
			return GPOS_NEW(pmp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);

		case CDistributionSpec::EdtSingleton:
		case CDistributionSpec::EdtStrictSingleton:
			// require second child to provide a matching singleton distribution
			return PdssMatching(pmp, CDistributionSpecSingleton::PdssConvert(pds));

		case CDistributionSpec::EdtHashed:
			// require second child to provide a matching hashed distribution
			return PdshashedMatching(pmp, CDistributionSpecHashed::PdsConvert(pds), ulSourceChildIndex);

		default:
			GPOS_ASSERT(CDistributionSpec::EdtReplicated == pds->Edt());
			if (EceoRightToLeft == eceo)
			{
				GPOS_ASSERT(1 == ulSourceChildIndex);

				// inner child is replicated, request outer child to have non-singleton distribution
				return GPOS_NEW(pmp) CDistributionSpecNonSingleton();
			}

			GPOS_ASSERT(0 == ulSourceChildIndex);

			// outer child is replicated, replicate inner child too in order to preserve correctness of semi-join
			return GPOS_NEW(pmp) CDistributionSpecReplicated();
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
	IMemoryPool *pmp,
	CDistributionSpecHashed *pdshashed,
	ULONG ulSourceChild // index of child that delivered the given hashed distribution
	)
	const
{
	GPOS_ASSERT(2 > ulSourceChild);

	DrgPexpr *pdrgpexprSource = m_pdrgpexprOuterKeys;
	DrgPexpr *pdrgpexprTarget = m_pdrgpexprInnerKeys;
	if (1 == ulSourceChild)
	{
		pdrgpexprSource = m_pdrgpexprInnerKeys;
		pdrgpexprTarget = m_pdrgpexprOuterKeys;
	}

	const DrgPexpr *pdrgpexprDist = pdshashed->Pdrgpexpr();
	const ULONG ulDlvrdSize = pdrgpexprDist->UlLength();
	const ULONG ulSourceSize = pdrgpexprSource->UlLength();

	DrgPexpr *pdrgpexprSourceNoCast = GPOS_NEW(pmp) DrgPexpr(pmp);
	DrgPexpr *pdrgpexprTargetNoCast = GPOS_NEW(pmp) DrgPexpr(pmp);
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
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ulDlvrdIdx = 0; ulDlvrdIdx < ulDlvrdSize; ulDlvrdIdx++)
	{
		CExpression *pexprDlvrd = CCastUtils::PexprWithoutBinaryCoercibleCasts((*pdrgpexprDist)[ulDlvrdIdx]);
		for (ULONG ulIdx = 0; ulIdx < ulSourceSize; ulIdx++)
		{
			if (CUtils::FEqual(pexprDlvrd, (*pdrgpexprSourceNoCast)[ulIdx]))
			{
				// TODO: 02/21/2012 - ; source column may be mapped to multiple
				// target columns (e.g. i=j and i=k);
				// in this case, we need to generate multiple optimization requests to the target child
				CExpression *pexprTarget = (*pdrgpexprTargetNoCast)[ulIdx];
				pexprTarget->AddRef();
				pdrgpexpr->Append(pexprTarget);
				break;
			}
		}
	}

	pdrgpexprSourceNoCast->Release();
	pdrgpexprTargetNoCast->Release();

	// check if we failed to compute required distribution
	if (pdrgpexpr->UlLength() != ulDlvrdSize)
	{
		pdrgpexpr->Release();
		if (NULL != pdshashed->PdshashedEquiv())
		{
			// try again using the equivalent hashed distribution
			return PdshashedMatching(pmp, pdshashed->PdshashedEquiv(), ulSourceChild);
		}
	}
	GPOS_ASSERT(pdrgpexpr->UlLength() == ulDlvrdSize);

	return GPOS_NEW(pmp) CDistributionSpecHashed(pdrgpexpr, true /* fNullsCollocated */);
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
	IMemoryPool *pmp,
	CExpressionHandle  &, // exprhdl
	CDistributionSpec *, // pdsInput
	ULONG  ulChildIndex,
	DrgPdp *pdrgpdpCtxt
	)
	const
{
	if (FFirstChildToOptimize(ulChildIndex))
	{
		// require first child to be singleton
		return GPOS_NEW(pmp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
	}

	// require a matching distribution from second child
	GPOS_ASSERT(NULL != pdrgpdpCtxt);
	CDistributionSpec *pdsFirst = CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();
	GPOS_ASSERT(NULL != pdsFirst);

	if (CDistributionSpec::EdtUniversal == pdsFirst->Edt())
	{
		// first child is universal, request second child to execute on the master to avoid duplicates
		return GPOS_NEW(pmp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
	}

	GPOS_ASSERT(CDistributionSpec::EdtSingleton == pdsFirst->Edt() ||
		CDistributionSpec::EdtStrictSingleton == pdsFirst->Edt());

	// require second child to have matching singleton distribution
	return CPhysical::PdssMatching(pmp, CDistributionSpecSingleton::PdssConvert(pdsFirst));
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
	IMemoryPool *pmp,
	CExpressionHandle  &exprhdl,
	CDistributionSpec *pdsInput,
	ULONG  ulChildIndex,
	DrgPdp *pdrgpdpCtxt,
	ULONG ulOptReq
	)
	const
{
	EChildExecOrder eceo = Eceo();
	if (EceoLeftToRight == eceo)
	{
		// if optimization order is left to right, fall-back to implementation of parent Join operator
		return CPhysicalJoin::PdsRequired(pmp, exprhdl, pdsInput, ulChildIndex, pdrgpdpCtxt, 0 /*ulOptReq*/);
	}
	GPOS_ASSERT(EceoRightToLeft == eceo);

	if (1 == ulChildIndex)
	{
		// require inner child to be replicated
		return GPOS_NEW(pmp) CDistributionSpecReplicated();
	}
	GPOS_ASSERT(0 == ulChildIndex);

	// require a matching distribution from outer child
	CDistributionSpec *pdsInner = CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();
	GPOS_ASSERT(NULL != pdsInner);

	if (CDistributionSpec::EdtUniversal == pdsInner->Edt())
	{
		// first child is universal, request second child to execute on the master to avoid duplicates
		return GPOS_NEW(pmp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
	}

	if (ulOptReq == m_pdrgpdsRedistributeRequests->UlLength() &&
		CDistributionSpec::EdtHashed == pdsInput->Edt())
	{
		// attempt to propagate hashed request to child
		CDistributionSpecHashed *pdshashed =
			PdshashedPassThru(pmp, exprhdl, CDistributionSpecHashed::PdsConvert(pdsInput), ulChildIndex, pdrgpdpCtxt, ulOptReq);
		if (NULL != pdshashed)
		{
			return pdshashed;
		}
	}

	// otherwise, require second child to deliver non-singleton distribution
	GPOS_ASSERT(CDistributionSpec::EdtReplicated == pdsInner->Edt());
	return GPOS_NEW(pmp) CDistributionSpecNonSingleton();
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
	IMemoryPool *pmp,
	CExpressionHandle  &exprhdl,
	CDistributionSpecHashed *pdshashedInput,
	ULONG  , // ulChildIndex
	DrgPdp *, // pdrgpdpCtxt
	ULONG
#ifdef GPOS_DEBUG
	 ulOptReq
#endif // GPOS_DEBUG
	)
	const
{
	GPOS_ASSERT(ulOptReq == m_pdrgpdsRedistributeRequests->UlLength());
	GPOS_ASSERT(NULL != pdshashedInput);

	if (!GPOS_FTRACE(EopttraceEnableRedistributeBroadcastHashJoin))
	{
		// this option is disabled
		return NULL;
	}

	// since incoming request is hashed, we attempt here to propagate this request to outer child
	CColRefSet *pcrsOuterOutput = exprhdl.Pdprel(0 /*ulChildIndex*/)->PcrsOutput();
	DrgPexpr *pdrgpexprIncomingRequest = pdshashedInput->Pdrgpexpr();
	CColRefSet *pcrsAllUsed = CUtils::PcrsExtractColumns(pmp, pdrgpexprIncomingRequest);
	BOOL fSubset = pcrsOuterOutput->FSubset(pcrsAllUsed);
	BOOL fDisjoint = pcrsOuterOutput->FDisjoint(pcrsAllUsed);
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
		 DrgPexpr *pdrgpexprChildRequest = GPOS_NEW(pmp) DrgPexpr(pmp);
		 const ULONG ulSize = pdrgpexprIncomingRequest->UlLength();
		 for (ULONG ul = 0; ul < ulSize; ul++)
		 {
			 CExpression *pexpr = (*pdrgpexprIncomingRequest)[ul];
			 CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexpr->PdpDerive())->PcrsUsed();
			 if (pcrsOuterOutput->FSubset(pcrsUsed))
			 {
				 // hashed expression uses columns from outer child only, add it to request
				 pexpr->AddRef();
				 pdrgpexprChildRequest->Append(pexpr);
			 }
		 }
		 GPOS_ASSERT(0 < pdrgpexprChildRequest->UlLength());

		 CDistributionSpecHashed *pdshashed = GPOS_NEW(pmp) CDistributionSpecHashed(pdrgpexprChildRequest, pdshashedInput->FNullsColocated());

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
	IMemoryPool *pmp,
	CExpressionHandle &, // exprhdl
	CDistributionSpec *, // pdsInput
	ULONG  ulChildIndex,
	DrgPdp *pdrgpdpCtxt,
	ULONG ulOptReq
	)
	const
{
	if (FFirstChildToOptimize(ulChildIndex))
	{
		// require first child to provide a hashed distribution,
		return PdshashedRequired(pmp, ulChildIndex, ulOptReq);
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
	return PdsMatch(pmp, pdsFirst, ulFirstChild);
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

	// if expression has to execute on master then we need a gather
	if (exprhdl.FMasterOnly())
	{
		return PdsEnforceMaster(pmp, exprhdl, pdsInput, ulChildIndex);
	}

	if (exprhdl.FHasOuterRefs())
	{
		if (CDistributionSpec::EdtSingleton == pdsInput->Edt() ||
			CDistributionSpec::EdtReplicated == pdsInput->Edt())
		{
			return PdsPassThru(pmp, exprhdl, pdsInput, ulChildIndex);
		}
		return GPOS_NEW(pmp) CDistributionSpecReplicated();
	}

	const ULONG ulHashDistributeRequests = m_pdrgpdsRedistributeRequests->UlLength();
	if (ulOptReq < ulHashDistributeRequests)
	{
		// requests 1 .. N are (redistribute, redistribute)
		return PdsRequiredRedistribute(pmp, exprhdl, pdsInput, ulChildIndex, pdrgpdpCtxt, ulOptReq);
	}

	if (ulOptReq == ulHashDistributeRequests ||
		ulOptReq == ulHashDistributeRequests + 1)
	{
		// requests N+1, N+2 are (hashed/non-singleton, replicate)

		return PdsRequiredReplicate(pmp, exprhdl, pdsInput, ulChildIndex, pdrgpdpCtxt, ulOptReq);
	}

	GPOS_ASSERT(ulOptReq == ulHashDistributeRequests + 2);

	// requests N+3 is (singleton, singleton)

	return PdsRequiredSingleton(pmp, exprhdl, pdsInput, ulChildIndex, pdrgpdpCtxt);
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
	IMemoryPool *, // pmp
	ULONG, // ulChildIndex
	ULONG ulReqIndex
	)
	const
{
	GPOS_ASSERT(ulReqIndex < m_pdrgpdsRedistributeRequests->UlLength());
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
	GPOS_ASSERT(!peo->PosRequired()->FEmpty());

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
		ulHashKeys = m_pdrgpexprInnerKeys->UlLength();
	}
	else
	{
		ulHashKeys = m_pdrgpexprOuterKeys->UlLength();
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
	EOperatorId eopid = pop->Eopid();

	if (COperator::EopScalarIdent == eopid)
	{
		const CColRef *pcr = CScalarIdent::PopConvert(pop)->Pcr();
		return (!pcrsNotNull->FMember(pcr));
	}

	if (COperator::EopScalarConst == eopid)
	{
		return CScalarConst::PopConvert(pop)->Pdatum()->FNull();
	}

	// be conservative for all other scalar expressions where we cannot easily
	// determine nullability
	return true;
}


// EOF
