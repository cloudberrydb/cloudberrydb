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
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/exception.h"
#include "gpopt/operators/CHashedDistributions.h"

using namespace gpopt;

static
BOOL FEqual(DrgPul *pdrgpulFst, DrgPul *pdrgpulSnd);

#ifdef GPOS_DEBUG
// helper to assert distribution delivered by UnionAll children
static
void AssertValidChildDistributions
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CDistributionSpec::EDistributionType *pedt, // array of distribution types to check
	ULONG ulDistrs, // number of distribution types to check
	const CHAR *szAssertMsg
	);

// helper to check if UnionAll children have valid distributions
static
void CheckChildDistributions
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	BOOL fSingletonChild,
	BOOL fReplicatedChild,
	BOOL fUniversalOuterChild
	);
#endif // GPOS_DEBUG

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
	CPhysicalUnionAll(pmp, pdrgpcrOutput, pdrgpdrgpcrInput, ulScanIdPartialIndex),
	m_pdrgpds(GPOS_NEW(pmp) CHashedDistributions(pmp, pdrgpcrOutput, pdrgpdrgpcrInput))
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
	m_pdrgpds->Release();
}


// helper to do value equality check of arrays of ULONG pointers
BOOL FEqual
	(
	DrgPul *pdrgpulFst,
	DrgPul *pdrgpulSnd
	)
{
	GPOS_ASSERT(NULL != pdrgpulFst);
	GPOS_ASSERT(NULL != pdrgpulSnd);

	const ULONG ulSizeFst = pdrgpulFst->UlLength();
	const ULONG ulSizeSnd = pdrgpulSnd->UlLength();
	if (ulSizeFst != ulSizeSnd)
	{
		// arrays have different lengths
		return false;
	}

	BOOL fEqual = true;
	for (ULONG ul = 0; fEqual && ul < ulSizeFst; ul++)
	{
		ULONG ulFst = *((*pdrgpulFst)[ul]);
		ULONG ulSnd = *((*pdrgpulSnd)[ul]);
		fEqual = (ulFst == ulSnd);
	}

	return fEqual;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSerialUnionAll::PdrgpulMap
//
//	@doc:
//		Map given array of scalar identifier expressions to positions of
//		UnionAll input columns in the given child;
//		the function returns NULL if no mapping could be constructed
//
//---------------------------------------------------------------------------
DrgPul *
CPhysicalSerialUnionAll::PdrgpulMap
	(
	IMemoryPool *pmp,
	DrgPexpr *pdrgpexpr,
	ULONG ulChildIndex
	)
	const
{
	GPOS_ASSERT(NULL != pdrgpexpr);

	DrgPcr *pdrgpcr = (*PdrgpdrgpcrInput())[ulChildIndex];
	const ULONG ulExprs = pdrgpexpr->UlLength();
	const ULONG ulCols = pdrgpcr->UlLength();
	DrgPul *pdrgpul = GPOS_NEW(pmp) DrgPul(pmp);
	for (ULONG ulExpr = 0; ulExpr < ulExprs; ulExpr++)
	{
		CExpression *pexpr = (*pdrgpexpr)[ulExpr];
		if (COperator::EopScalarIdent != pexpr->Pop()->Eopid())
		{
			continue;
		}
		const CColRef *pcr = CScalarIdent::PopConvert(pexpr->Pop())->Pcr();
		for (ULONG ulCol = 0; ulCol < ulCols; ulCol++)
		{
			if ((*pdrgpcr)[ulCol] == pcr)
			{
				pdrgpul->Append(GPOS_NEW(pmp) ULONG(ulCol));
			}
		}
	}

	if (0 == pdrgpul->UlLength())
	{
		// mapping failed
		pdrgpul->Release();
		pdrgpul = NULL;
	}

	return pdrgpul;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSerialUnionAll::PdshashedDerive
//
//	@doc:
//		Derive hashed distribution from child hashed distributions
//
//---------------------------------------------------------------------------
CDistributionSpecHashed *
CPhysicalSerialUnionAll::PdshashedDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	BOOL fSuccess = true;
	const ULONG ulArity = exprhdl.UlArity();

	// (1) check that all children deliver a hashed distribution that satisfies their input columns
	for (ULONG ulChild = 0; fSuccess && ulChild < ulArity; ulChild++)
	{
		CDistributionSpec *pdsChild = exprhdl.Pdpplan(ulChild)->Pds();
		CDistributionSpec::EDistributionType edtChild = pdsChild->Edt();
		fSuccess =  (CDistributionSpec::EdtHashed == edtChild) && pdsChild->FSatisfies((*m_pdrgpds)[ulChild]);
	}

	if (!fSuccess)
	{
		// a child does not deliver hashed distribution
		return NULL;
	}

	// (2) check that child hashed distributions map to the same output columns

	// map outer child hashed distribution to corresponding UnionAll column positions
	DrgPul *pdrgpulOuter = PdrgpulMap(pmp, CDistributionSpecHashed::PdsConvert(exprhdl.Pdpplan(0 /*ulChildIndex*/)->Pds())->Pdrgpexpr(), 0/*ulChildIndex*/);
	if (NULL == pdrgpulOuter)
	{
		return NULL;
	}

	DrgPul *pdrgpulChild = NULL;
	for (ULONG ulChild = 1; fSuccess && ulChild < ulArity; ulChild++)
	{
		pdrgpulChild = PdrgpulMap(pmp, CDistributionSpecHashed::PdsConvert(exprhdl.Pdpplan(ulChild)->Pds())->Pdrgpexpr(), ulChild);

		// match mapped column positions of current child with outer child
		fSuccess = (NULL != pdrgpulChild) && FEqual(pdrgpulOuter, pdrgpulChild);
		CRefCount::SafeRelease(pdrgpulChild);
	}

	CDistributionSpecHashed *pdsOutput = NULL;
	if (fSuccess)
	{
		pdsOutput = PdsMatching(pmp, pdrgpulOuter);
	}

	pdrgpulOuter->Release();

	return pdsOutput;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSerialUnionAll::PdsMatching
//
//	@doc:
//		Compute output hashed distribution based on the outer child's
//		hashed distribution
//---------------------------------------------------------------------------
CDistributionSpecHashed *
CPhysicalSerialUnionAll::PdsMatching
	(
	IMemoryPool *pmp,
	const DrgPul *pdrgpulOuter
	)
	const
{
	GPOS_ASSERT(NULL != pdrgpulOuter);

	const ULONG ulCols = pdrgpulOuter->UlLength();

	GPOS_ASSERT(ulCols <= PdrgpcrOutput()->UlLength());

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ulCol = 0; ulCol < ulCols; ulCol++)
	{
		ULONG ulIdx = *(*pdrgpulOuter)[ulCol];
		CExpression *pexpr = CUtils::PexprScalarIdent(pmp, (*PdrgpcrOutput())[ulIdx]);
		pdrgpexpr->Append(pexpr);
	}

	GPOS_ASSERT(0 < pdrgpexpr->UlLength());

	return GPOS_NEW(pmp) CDistributionSpecHashed(pdrgpexpr, true /*fNullsColocated*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSerialUnionAll::PdshashedPassThru
//
//	@doc:
//		Compute required hashed distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpecHashed *
CPhysicalSerialUnionAll::PdshashedPassThru
	(
	IMemoryPool *pmp,
	CDistributionSpecHashed *pdshashedRequired,
	ULONG ulChildIndex
	)
	const
{
	DrgPexpr *pdrgpexprRequired = pdshashedRequired->Pdrgpexpr();
	DrgPcr *pdrgpcrChild = (*PdrgpdrgpcrInput())[ulChildIndex];
	const ULONG ulExprs = pdrgpexprRequired->UlLength();
	const ULONG ulOutputCols = PdrgpcrOutput()->UlLength();

	DrgPexpr *pdrgpexprChildRequired = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ulExpr = 0; ulExpr < ulExprs; ulExpr++)
	{
		CExpression *pexpr = (*pdrgpexprRequired)[ulExpr];
		if (COperator::EopScalarIdent != pexpr->Pop()->Eopid())
		{
			// skip expressions that are not in form of scalar identifiers
			continue;
		}
		const CColRef *pcrHashed = CScalarIdent::PopConvert(pexpr->Pop())->Pcr();
		const IMDType *pmdtype = pcrHashed->Pmdtype();
		if (!pmdtype->FHashable())
		{
			// skip non-hashable columns
			continue;
		}

		for (ULONG ulCol = 0; ulCol < ulOutputCols; ulCol++)
		{
			const CColRef *pcrOutput = (*PdrgpcrOutput())[ulCol];
			if (pcrOutput == pcrHashed)
			{
				const CColRef *pcrInput = (*pdrgpcrChild)[ulCol];
				pdrgpexprChildRequired->Append(CUtils::PexprScalarIdent(pmp, pcrInput));
			}
		}
	}

	if (0 < pdrgpexprChildRequired->UlLength())
	{
		return GPOS_NEW(pmp) CDistributionSpecHashed(pdrgpexprChildRequired, true /* fNullsCollocated */);
	}

	// failed to create a matching hashed distribution
	pdrgpexprChildRequired->Release();

	if (NULL != pdshashedRequired->PdshashedEquiv())
	{
		// try again with equivalent distribution
		return PdshashedPassThru(pmp, pdshashedRequired->PdshashedEquiv(), ulChildIndex);
	}

	// failed to create hashed distribution
	return NULL;
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

#ifdef GPOS_DEBUG
void
AssertValidChildDistributions
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CDistributionSpec::EDistributionType *pedt, // array of distribution types to check
	ULONG ulDistrs, // number of distribution types to check
	const CHAR *szAssertMsg
	)
{
	const ULONG ulArity = exprhdl.UlArity();
	for (ULONG ulChild = 0; ulChild < ulArity; ulChild++)
	{
		CDistributionSpec *pdsChild = exprhdl.Pdpplan(ulChild)->Pds();
		CDistributionSpec::EDistributionType edtChild = pdsChild->Edt();
		BOOL fMatch = false;
		for (ULONG ulDistr = 0; !fMatch && ulDistr < ulDistrs; ulDistr++)
		{
			fMatch = (pedt[ulDistr] == edtChild);
		}

		if (!fMatch)
		{
			CAutoTrace at(pmp);
			at.Os() << szAssertMsg;
		}
		GPOS_ASSERT(fMatch);
	}
}


void
CheckChildDistributions
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	BOOL fSingletonChild,
	BOOL fReplicatedChild,
	BOOL fUniversalOuterChild
	)
{
	CDistributionSpec::EDistributionType rgedt[4];
	rgedt[0] = CDistributionSpec::EdtSingleton;
	rgedt[1] = CDistributionSpec::EdtStrictSingleton;
	rgedt[2] = CDistributionSpec::EdtUniversal;
	rgedt[3] = CDistributionSpec::EdtReplicated;

	if (fReplicatedChild)
	{
		// assert all children have distribution Universal or Replicated
		AssertValidChildDistributions(pmp, exprhdl, rgedt + 2 /*start from Universal in rgedt*/, 2 /*ulDistrs*/, "expecting Replicated or Universal distribution in UnionAll children" /*szAssertMsg*/);
	}
	else if (fSingletonChild || fUniversalOuterChild)
	{
		// assert all children have distribution Singleton, StrictSingleton or Universal
		AssertValidChildDistributions(pmp, exprhdl, rgedt, 3  /*ulDistrs*/, "expecting Singleton or Universal distribution in UnionAll children" /*szAssertMsg*/);
	}
}
#endif // GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSerialUnionAll::PdsDeriveFromChildren
//
//	@doc:
//		Derive output distribution based on child distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalSerialUnionAll::PdsDeriveFromChildren
	(
	IMemoryPool *
#ifdef GPOS_DEBUG
		pmp
#endif // GPOS_DEBUG
	,
	CExpressionHandle &exprhdl
	)
	const
{
	const ULONG ulArity = exprhdl.UlArity();

	CDistributionSpec *pdsOuter = exprhdl.Pdpplan(0 /*ulChildIndex*/)->Pds();
	CDistributionSpec *pds = pdsOuter;
	BOOL fUniversalOuterChild =  (CDistributionSpec::EdtUniversal == pdsOuter->Edt());
	BOOL fSingletonChild = false;
	BOOL fReplicatedChild = false;
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CDistributionSpec *pdsChild = exprhdl.Pdpplan(ul /*ulChildIndex*/)->Pds();
		CDistributionSpec::EDistributionType edtChild = pdsChild->Edt();

		if (CDistributionSpec::EdtSingleton == edtChild ||
			CDistributionSpec::EdtStrictSingleton == edtChild)
		{
			fSingletonChild = true;
			pds = pdsChild;
			break;
		}

		if (CDistributionSpec::EdtReplicated == edtChild)
		{
			fReplicatedChild = true;
			pds = pdsChild;
			break;
		}
	}

#ifdef GPOS_DEBUG
	CheckChildDistributions(pmp, exprhdl, fSingletonChild, fReplicatedChild, fUniversalOuterChild);
#endif // GPOS_DEBUG

	if (!(fSingletonChild || fReplicatedChild || fUniversalOuterChild))
	{
		// failed to derive distribution from children
		pds = NULL;
	}

	return pds;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSerialUnionAll::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalSerialUnionAll::PdsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	CDistributionSpecHashed *pdshashed = PdshashedDerive(pmp, exprhdl);
	if (NULL != pdshashed)
	{
		return pdshashed;
	}

	CDistributionSpec *pds = PdsDeriveFromChildren(pmp, exprhdl);
	if (NULL != pds)
	{
		// succeeded in deriving output distribution from child distributions
		pds->AddRef();
		return pds;
	}

	// output has unknown distribution on all segments
	return GPOS_NEW(pmp) CDistributionSpecRandom();
}


// EOF
