#include "gpopt/base/CDrvdPropCtxtPlan.h"

#include "gpopt/operators/CPhysicalUnionAll.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CHashedDistributions.h"
#include "gpopt/operators/CScalarIdent.h"

using namespace gpopt;

static
BOOL
FEqual(DrgPul *pdrgpulFst, DrgPul *pdrgpulSnd);

#ifdef GPOS_DEBUG

// helper to assert distribution delivered by UnionAll children
static
void
AssertValidChildDistributions
	(
		IMemoryPool *pmp,
		CExpressionHandle &exprhdl,
		CDistributionSpec::EDistributionType *pedt, // array of distribution types to check
		ULONG ulDistrs, // number of distribution types to check
		const CHAR *szAssertMsg
	);

// helper to check if UnionAll children have valid distributions
static
void
CheckChildDistributions
	(
		IMemoryPool *pmp,
		CExpressionHandle &exprhdl,
		BOOL fSingletonChild,
		BOOL fReplicatedChild,
		BOOL fUniversalOuterChild
	);

#endif // GPOS_DEBUG

// helper to do value equality check of arrays of ULONG pointers
BOOL
FEqual
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

CColRefSet *
CPhysicalUnionAll::PcrsInput
	(
		ULONG ulChildIndex
	)
{
	GPOS_ASSERT(NULL != m_pdrgpcrsInput);
	CColRefSet *pcrs = (*m_pdrgpcrsInput)[ulChildIndex];
	return pcrs;
}

// sensitivity to order of inputs
BOOL
CPhysicalUnionAll::FInputOrderSensitive() const
{
	return false;
}

CPhysicalUnionAll::CPhysicalUnionAll
	(
		IMemoryPool *pmp,
		DrgPcr *pdrgpcrOutput,
		DrgDrgPcr *pdrgpdrgpcrInput,
		ULONG ulScanIdPartialIndex
	)
	:
	CPhysical(pmp),
	m_pdrgpcrOutput(pdrgpcrOutput),
	m_pdrgpdrgpcrInput(pdrgpdrgpcrInput),
	m_ulScanIdPartialIndex(ulScanIdPartialIndex),
	m_pdrgpcrsInput(NULL),
	m_pdrgpds(GPOS_NEW(pmp) CHashedDistributions(pmp, pdrgpcrOutput, pdrgpdrgpcrInput))
{
	GPOS_ASSERT(NULL != pdrgpcrOutput);
	GPOS_ASSERT(NULL != pdrgpdrgpcrInput);

	// build set representation of input columns
	m_pdrgpcrsInput = GPOS_NEW(pmp) DrgPcrs(pmp);
	const ULONG ulArity = m_pdrgpdrgpcrInput->UlLength();
	for (ULONG ulChild = 0; ulChild < ulArity; ulChild++)
	{
		DrgPcr *pdrgpcr = (*m_pdrgpdrgpcrInput)[ulChild];
		m_pdrgpcrsInput->Append(GPOS_NEW(pmp) CColRefSet(pmp, pdrgpcr));
	}
}

CPhysicalUnionAll::~CPhysicalUnionAll()
{
	m_pdrgpcrOutput->Release();
	m_pdrgpdrgpcrInput->Release();
	m_pdrgpcrsInput->Release();
	m_pdrgpds->Release();
}

// accessor of output column array
DrgPcr *
CPhysicalUnionAll::PdrgpcrOutput() const
{
	return m_pdrgpcrOutput;
}

// accessor of input column array
DrgDrgPcr *
CPhysicalUnionAll::PdrgpdrgpcrInput() const
{
	return m_pdrgpdrgpcrInput;
}

// if this unionall is needed for partial indexes then return the scan
// id, otherwise return gpos::ulong_max
ULONG
CPhysicalUnionAll::UlScanIdPartialIndex() const
{
	return m_ulScanIdPartialIndex;
}

// is this unionall needed for a partial index
BOOL
CPhysicalUnionAll::FPartialIndex() const
{
	return (gpos::ulong_max > m_ulScanIdPartialIndex);
}

CPhysicalUnionAll *
CPhysicalUnionAll::PopConvert
	(
		COperator *pop
	)
{
	GPOS_ASSERT(NULL != pop);

	CPhysicalUnionAll *popPhysicalUnionAll = dynamic_cast<CPhysicalUnionAll *>(pop);
	GPOS_ASSERT(NULL != popPhysicalUnionAll);

	return popPhysicalUnionAll;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::FMatch
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalUnionAll::FMatch
	(
		COperator *pop
	)
const
{
	if (Eopid() == pop->Eopid())
	{
		CPhysicalUnionAll *popUnionAll = CPhysicalUnionAll::PopConvert(pop);

		return PdrgpcrOutput()->FEqual(popUnionAll->PdrgpcrOutput()) &&
			   UlScanIdPartialIndex() == popUnionAll->UlScanIdPartialIndex();
	}

	return false;

}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalUnionAll::PcrsRequired
	(
		IMemoryPool *, // pmp
		CExpressionHandle &,//exprhdl,
		CColRefSet *, //pcrsRequired,
		ULONG ulChildIndex,
		DrgPdp *, // pdrgpdpCtxt
		ULONG // ulOptReq
	)
{
	CColRefSet *pcrs = PcrsInput(ulChildIndex);
	pcrs->AddRef();

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalUnionAll::PosRequired
	(
		IMemoryPool *pmp,
		CExpressionHandle &, //exprhdl,
		COrderSpec *, //posRequired,
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
	GPOS_ASSERT(PdrgpdrgpcrInput()->UlLength() > ulChildIndex);

	// no order required from child expression
	return GPOS_NEW(pmp) COrderSpec(pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalUnionAll::PrsRequired
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
	GPOS_ASSERT(PdrgpdrgpcrInput()->UlLength() > ulChildIndex);

	return PrsPassThru(pmp, exprhdl, prsRequired, ulChildIndex);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalUnionAll::PppsRequired
	(
		IMemoryPool *pmp,
		CExpressionHandle &exprhdl,
		CPartitionPropagationSpec *pppsRequired,
		ULONG ulChildIndex,
		DrgPdp *, //pdrgpdpCtxt,
		ULONG //ulOptReq
	)
{
	GPOS_ASSERT(NULL != pppsRequired);

	if (FPartialIndex())
	{
		// if this union came from the partial index xform, push an
		// empty partition request below
		return GPOS_NEW(pmp) CPartitionPropagationSpec
			(
				GPOS_NEW(pmp) CPartIndexMap(pmp),
				GPOS_NEW(pmp) CPartFilterMap(pmp)
			);
	}

	return CPhysical::PppsRequiredPushThruNAry(pmp, exprhdl, pppsRequired, ulChildIndex);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalUnionAll::PcteRequired
	(
		IMemoryPool *pmp,
		CExpressionHandle &exprhdl,
		CCTEReq *pcter,
		ULONG ulChildIndex,
		DrgPdp *pdrgpdpCtxt,
		ULONG //ulOptReq
	)
const
{
	return PcterNAry(pmp, exprhdl, pcter, ulChildIndex, pdrgpdpCtxt);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalUnionAll::FProvidesReqdCols
	(
		CExpressionHandle &
#ifdef GPOS_DEBUG
		exprhdl
#endif // GPOS_DEBUG
	,
		CColRefSet *pcrsRequired,
		ULONG // ulOptReq
)
const
{
	GPOS_ASSERT(NULL != pcrsRequired);
	GPOS_ASSERT(PdrgpdrgpcrInput()->UlLength() == exprhdl.UlArity());

	CColRefSet *pcrs = GPOS_NEW(m_pmp) CColRefSet(m_pmp);

	// include output columns
	pcrs->Include(PdrgpcrOutput());
	BOOL fProvidesCols = pcrs->FSubset(pcrsRequired);
	pcrs->Release();

	return fProvidesCols;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalUnionAll::PosDerive
	(
		IMemoryPool *pmp,
		CExpressionHandle &//exprhdl
	)
const
{
	// return empty sort order
	return GPOS_NEW(pmp) COrderSpec(pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalUnionAll::PrsDerive
	(
		IMemoryPool *, // pmp
		CExpressionHandle &exprhdl
	)
const
{
	return PrsDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalUnionAll::EpetOrder
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

	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalUnionAll::EpetRewindability
	(
		CExpressionHandle &exprhdl,
		const CEnfdRewindability *per
	)
const
{
	GPOS_ASSERT(NULL != per);

	// get rewindability delivered by the node
	CRewindabilitySpec *prs = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs();
	if (per->FCompatible(prs))
	{
		// required rewindability is already provided
		return CEnfdProp::EpetUnnecessary;
	}

	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::EpetPartitionPropagation
//
//	@doc:
//		Compute the enforcing type for the operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalUnionAll::EpetPartitionPropagation
	(
		CExpressionHandle &exprhdl,
		const CEnfdPartitionPropagation *pepp
	)
const
{
	CPartIndexMap *ppimReqd = pepp->PppsRequired()->Ppim();
	if (!ppimReqd->FContainsUnresolved())
	{
		// no unresolved partition consumers left
		return CEnfdProp::EpetUnnecessary;
	}

	CPartIndexMap *ppimDrvd = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Ppim();
	GPOS_ASSERT(NULL != ppimDrvd);

	BOOL fInScope = pepp->FInScope(m_pmp, ppimDrvd);
	BOOL fResolved = pepp->FResolved(m_pmp, ppimDrvd);

	if (fResolved)
	{
		// all required partition consumers are resolved
		return CEnfdProp::EpetUnnecessary;
	}

	if (!fInScope)
	{
		// some partition consumers are not covered downstream
		return CEnfdProp::EpetRequired;
	}


	DrgPul *pdrgpul = ppimReqd->PdrgpulScanIds(m_pmp);
	const ULONG ulScanIds = pdrgpul->UlLength();

	const ULONG ulArity = exprhdl.UlNonScalarChildren();
	for (ULONG ul = 0; ul < ulScanIds; ul++)
	{
		ULONG ulScanId = *((*pdrgpul)[ul]);

		ULONG ulChildrenWithConsumers = 0;
		for (ULONG ulChildIdx = 0; ulChildIdx < ulArity; ulChildIdx++)
		{
			if (exprhdl.Pdprel(ulChildIdx)->Ppartinfo()->FContainsScanId(ulScanId))
			{
				ulChildrenWithConsumers++;
			}
		}

		if (1 < ulChildrenWithConsumers)
		{
			// partition consumer exists in more than one child, so enforce it here
			pdrgpul->Release();

			return CEnfdProp::EpetRequired;
		}
	}

	pdrgpul->Release();

	// required part propagation can be enforced here or passed to the children
	return CEnfdProp::EpetOptional;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::PpimDerive
//
//	@doc:
//		Derive partition index map
//
//---------------------------------------------------------------------------
CPartIndexMap *
CPhysicalUnionAll::PpimDerive
	(
		IMemoryPool *pmp,
		CExpressionHandle &exprhdl,
		CDrvdPropCtxt *pdpctxt
	)
const
{
	CPartIndexMap *ppim = PpimDeriveCombineRelational(pmp, exprhdl);
	if (FPartialIndex())
	{
		GPOS_ASSERT(NULL != pdpctxt);
		ULONG ulExpectedPartitionSelectors = CDrvdPropCtxtPlan::PdpctxtplanConvert(pdpctxt)->UlExpectedPartitionSelectors();
		ppim->SetExpectedPropagators(UlScanIdPartialIndex(), ulExpectedPartitionSelectors);
	}

	return ppim;
}

// derive partition filter map
CPartFilterMap *
CPhysicalUnionAll::PpfmDerive
	(
		IMemoryPool *pmp,
		CExpressionHandle &exprhdl
	)
const
{
	// combine part filter maps from relational children
	return PpfmDeriveCombineRelational(pmp, exprhdl);
}

BOOL
CPhysicalUnionAll::FPassThruStats() const
{
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalUnionAll::PdsDerive
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

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalUnionAll::PdshashedDerive
//
//	@doc:
//		Derive hashed distribution from child hashed distributions
//
//---------------------------------------------------------------------------
CDistributionSpecHashed *
CPhysicalUnionAll::PdshashedDerive
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
		fSuccess = (CDistributionSpec::EdtHashed == edtChild || CDistributionSpec::EdtHashedNoOp == edtChild || CDistributionSpec::EdtStrictHashed == edtChild)
				&& pdsChild->FSatisfies((*m_pdrgpds)[ulChild]);
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
//		CPhysicalUnionAll::PdsMatching
//
//	@doc:
//		Compute output hashed distribution based on the outer child's
//		hashed distribution
//---------------------------------------------------------------------------
CDistributionSpecHashed *
CPhysicalUnionAll::PdsMatching
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
//		CPhysicalUnionAll::PdshashedPassThru
//
//	@doc:
//		Compute required hashed distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpecHashed *
CPhysicalUnionAll::PdshashedPassThru
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
//		CPhysicalUnionAll::PdsDeriveFromChildren
//
//	@doc:
//		Derive output distribution based on child distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalUnionAll::PdsDeriveFromChildren
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
	BOOL fUniversalOuterChild = (CDistributionSpec::EdtUniversal == pdsOuter->Edt());
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
//		CPhysicalUnionAll::PdrgpulMap
//
//	@doc:
//		Map given array of scalar identifier expressions to positions of
//		UnionAll input columns in the given child;
//		the function returns NULL if no mapping could be constructed
//
//---------------------------------------------------------------------------
DrgPul *
CPhysicalUnionAll::PdrgpulMap
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
		AssertValidChildDistributions(pmp, exprhdl, rgedt + 2 /*start from Universal in rgedt*/, 2 /*ulDistrs*/,
									  "expecting Replicated or Universal distribution in UnionAll children" /*szAssertMsg*/);
	} else if (fSingletonChild || fUniversalOuterChild)
	{
		// assert all children have distribution Singleton, StrictSingleton or Universal
		AssertValidChildDistributions(pmp, exprhdl, rgedt, 3  /*ulDistrs*/,
									  "expecting Singleton or Universal distribution in UnionAll children" /*szAssertMsg*/);
	}
}

#endif // GPOS_DEBUG
