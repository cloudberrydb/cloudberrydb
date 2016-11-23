#include "gpopt/operators/CPhysicalUnionAll.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/base/CDrvdPropCtxtPlan.h"

namespace gpopt
{
CColRefSet*
CPhysicalUnionAll::PcrsInput
	(
	ULONG ulChildIndex
	)
{
	GPOS_ASSERT(NULL != m_pdrgpcrsInput);
	CColRefSet *pcrs  = (*m_pdrgpcrsInput)[ulChildIndex];
	return pcrs;
}

// sensitivity to order of inputs
BOOL CPhysicalUnionAll::FInputOrderSensitive() const
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
	m_pdrgpcrsInput(NULL)
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
}

// accessor of output column array
DrgPcr* CPhysicalUnionAll::PdrgpcrOutput() const
{
	return m_pdrgpcrOutput;
}

// accessor of input column array
DrgDrgPcr *CPhysicalUnionAll::PdrgpdrgpcrInput() const
{
	return m_pdrgpdrgpcrInput;
}

// if this unionall is needed for partial indexes then return the scan
// id, otherwise return ULONG_MAX
ULONG CPhysicalUnionAll::UlScanIdPartialIndex() const
{
	return m_ulScanIdPartialIndex;
}

// is this unionall needed for a partial index
BOOL CPhysicalUnionAll::FPartialIndex() const
{
	return (ULONG_MAX > m_ulScanIdPartialIndex);
}

CPhysicalUnionAll *CPhysicalUnionAll::PopConvert
	(
	COperator *pop
	)
{
	GPOS_ASSERT(NULL != pop);

	CPhysicalUnionAll* popPhysicalUnionAll = dynamic_cast<CPhysicalUnionAll*>(pop);
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
		CColRefSet *pcrs  = PcrsInput(ulChildIndex);
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
//		CPhysicalUnionAll::EpetDistribution
//
//	@doc:
//		Return the enforcing type for distribution property based on this operator
//
//---------------------------------------------------------------------------
	CEnfdProp::EPropEnforcingType
	CPhysicalUnionAll::EpetDistribution
	(
		CExpressionHandle &exprhdl,
		const CEnfdDistribution *ped
	)
	const
	{
		GPOS_ASSERT(NULL != ped);

		// get distribution delivered by the node
		CDistributionSpec *pds = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pds();
		if (ped->FCompatible(pds))
		{
		 // required distribution is already provided
		 return CEnfdProp::EpetUnnecessary;
		}

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
	CPartFilterMap *CPhysicalUnionAll::PpfmDerive
		(
			IMemoryPool *pmp,
			CExpressionHandle &exprhdl
		)
	const
	{
		// combine part filter maps from relational children
		return PpfmDeriveCombineRelational(pmp, exprhdl);
	}

	BOOL CPhysicalUnionAll::FPassThruStats() const
	{
		return false;
	}
}
