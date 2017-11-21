//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalInnerHashJoin.cpp
//
//	@doc:
//		Implementation of inner hash join operator
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/operators/CExpressionHandle.h"

#include "gpopt/operators/CPhysicalInnerHashJoin.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::CPhysicalInnerHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalInnerHashJoin::CPhysicalInnerHashJoin
	(
	IMemoryPool *pmp,
	DrgPexpr *pdrgpexprOuterKeys,
	DrgPexpr *pdrgpexprInnerKeys
	)
	:
	CPhysicalHashJoin(pmp, pdrgpexprOuterKeys, pdrgpexprInnerKeys)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::~CPhysicalInnerHashJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalInnerHashJoin::~CPhysicalInnerHashJoin()
{
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::PdshashedCreateMatching
//
//	@doc:
//		Helper function for creating a matching hashed distribution
//
//---------------------------------------------------------------------------
CDistributionSpecHashed *
CPhysicalInnerHashJoin::PdshashedCreateMatching
	(
	IMemoryPool *pmp,
	CDistributionSpecHashed *pdshashed,
	ULONG ulSourceChild // index of child that delivered the given hashed distribution
	)
	const
{
	GPOS_ASSERT(NULL != pdshashed);

 	CDistributionSpecHashed *pdshashedMatching = PdshashedMatching(pmp, pdshashed, ulSourceChild);
 	pdshashed->Pdrgpexpr()->AddRef();

	// return a hashed distribution equivalent to created matching distribution
 	return GPOS_NEW(pmp) CDistributionSpecHashed
 			 (
 			 pdshashed->Pdrgpexpr(),
 			 pdshashed->FNullsColocated(),
 			 pdshashedMatching // matching distribution spec is equivalent to passed distribution spec
 			 );
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::PdsDeriveFromHashedChildren
//
//	@doc:
//		Derive hash join distribution from hashed children;
//		return NULL if derivation failed
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalInnerHashJoin::PdsDeriveFromHashedChildren
	(
	IMemoryPool *pmp,
	CDistributionSpec *pdsOuter,
	CDistributionSpec *pdsInner
	)
	const
{
	GPOS_ASSERT(NULL != pdsOuter);
	GPOS_ASSERT(NULL != pdsInner);

	CDistributionSpecHashed *pdshashedOuter = CDistributionSpecHashed::PdsConvert(pdsOuter);
 	CDistributionSpecHashed *pdshashedInner = CDistributionSpecHashed::PdsConvert(pdsInner);

	if (CUtils::FContains(PdrgpexprOuterKeys(), pdshashedOuter->Pdrgpexpr()) &&
 		CUtils::FContains(PdrgpexprInnerKeys(), pdshashedInner->Pdrgpexpr()))
 	{
 	 	// if both sides are hashed on subsets of hash join keys, join's output can be
 		// seen as distributed on outer spec or (equivalently) on inner spec,
 	 	// in this case, we create a new spec based on outer side and mark inner
 		// side as an equivalent one,

		return PdshashedCreateMatching(pmp, pdshashedOuter, 0 /*ulSourceChild*/);
 	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::PdsDeriveFromReplicatedOuter
//
//	@doc:
//		Derive hash join distribution from a replicated outer child;
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalInnerHashJoin::PdsDeriveFromReplicatedOuter
	(
	IMemoryPool *pmp,
	CDistributionSpec *
#ifdef GPOS_DEBUG
	pdsOuter
#endif // GPOS_DEBUG
	,
	CDistributionSpec *pdsInner
	)
	const
{
	GPOS_ASSERT(NULL != pdsOuter);
	GPOS_ASSERT(NULL != pdsInner);
	GPOS_ASSERT(CDistributionSpec::EdtReplicated == pdsOuter->Edt());

	// if outer child is replicated, join results distribution is defined by inner child
	if (CDistributionSpec::EdtHashed == pdsInner->Edt())
	{
		CDistributionSpecHashed *pdshashedInner = CDistributionSpecHashed::PdsConvert(pdsInner);
		if (CUtils::FContains(PdrgpexprInnerKeys(), pdshashedInner->Pdrgpexpr()))
		{
			// inner child is hashed on a subset of inner hashkeys,
		 	// return a hashed distribution equivalent to a matching outer distribution
			return PdshashedCreateMatching(pmp, pdshashedInner, 1 /*ulSourceChild*/);
		}
	}

	// otherwise, pass-through inner distribution
	pdsInner->AddRef();
	return pdsInner;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::PdsDeriveFromHashedOuter
//
//	@doc:
//		Derive hash join distribution from a hashed outer child;
//		return NULL if derivation failed
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalInnerHashJoin::PdsDeriveFromHashedOuter
	(
	IMemoryPool *pmp,
	CDistributionSpec *pdsOuter,
	CDistributionSpec *
#ifdef GPOS_DEBUG
	pdsInner
#endif // GPOS_DEBUG
	)
	const
{
	GPOS_ASSERT(NULL != pdsOuter);
	GPOS_ASSERT(NULL != pdsInner);

	GPOS_ASSERT(CDistributionSpec::EdtHashed == pdsOuter->Edt());

	 CDistributionSpecHashed *pdshashedOuter = CDistributionSpecHashed::PdsConvert(pdsOuter);
	 if (CUtils::FContains(PdrgpexprOuterKeys(), pdshashedOuter->Pdrgpexpr()))
	 {
	 	// outer child is hashed on a subset of outer hashkeys,
	 	// return a hashed distribution equivalent to a matching outer distribution
		return PdshashedCreateMatching(pmp, pdshashedOuter, 0 /*ulSourceChild*/);
	 }

	 return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalInnerHashJoin::PdsDerive
(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	CDistributionSpec *pdsOuter = exprhdl.Pdpplan(0 /*ulChildIndex*/)->Pds();
 	CDistributionSpec *pdsInner = exprhdl.Pdpplan(1 /*ulChildIndex*/)->Pds();

 	if (CDistributionSpec::EdtUniversal == pdsOuter->Edt())
 	{
 		// if outer is universal, pass through inner distribution
 		pdsInner->AddRef();
 		return pdsInner;
 	}

 	if (CDistributionSpec::EdtHashed == pdsOuter->Edt() && CDistributionSpec::EdtHashed == pdsInner->Edt())
 	{
 		CDistributionSpec *pdsDerived = PdsDeriveFromHashedChildren(pmp, pdsOuter, pdsInner);
 		if (NULL != pdsDerived)
 		{
 			return pdsDerived;
 		}
 	}

 	if (CDistributionSpec::EdtReplicated == pdsOuter->Edt())
 	{
 		return PdsDeriveFromReplicatedOuter(pmp, pdsOuter, pdsInner);
 	}

 	if (CDistributionSpec::EdtHashed == pdsOuter->Edt())
 	{
 		CDistributionSpec *pdsDerived = PdsDeriveFromHashedOuter(pmp, pdsOuter, pdsInner);
 		 if (NULL != pdsDerived)
 		 {
 		 	return pdsDerived;
 		 }
 	 }

 	// otherwise, pass through outer distribution
 	pdsOuter->AddRef();
 	return pdsOuter;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalInnerHashJoin::PppsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CPartitionPropagationSpec *pppsRequired,
	ULONG ulChildIndex,
	DrgPdp *pdrgpdpCtxt,
	ULONG ulOptReq
	)
{

	if (1 == ulOptReq)
	{
		// request (1): push partition propagation requests to join's children,
		// do not consider possible dynamic partition elimination using join predicate here,
		// this is handled by optimization request (0) below
		return CPhysical::PppsRequiredPushThruNAry(pmp, exprhdl, pppsRequired, ulChildIndex);
	}

	// request (0): push partition progagation requests to join child considering
	// DPE possibility. For HJ, PS request is pushed to the inner child if there
	// is a consumer (DTS) on the outer side of the join.
	GPOS_ASSERT(0 == ulOptReq);
	return PppsRequiredJoinChild(pmp, exprhdl, pppsRequired, ulChildIndex, pdrgpdpCtxt, false);
}

// EOF

