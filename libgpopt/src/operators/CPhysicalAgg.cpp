//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalAgg.cpp
//
//	@doc:
//		Implementation of basic aggregate operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecRandom.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalAgg.h"
#include "gpopt/operators/CLogicalGbAgg.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::CPhysicalAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalAgg::CPhysicalAgg
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcr,
	DrgPcr *pdrgpcrMinimal, // minimal grouping columns based on FD's
	COperator::EGbAggType egbaggtype,
	BOOL fGeneratesDuplicates,
	DrgPcr *pdrgpcrArgDQA,
	BOOL fMultiStage
	)
	:
	CPhysical(pmp),
	m_pdrgpcr(pdrgpcr),
	m_egbaggtype(egbaggtype),
	m_pdrgpcrMinimal(NULL),
	m_fGeneratesDuplicates(fGeneratesDuplicates),
	m_pdrgpcrArgDQA(pdrgpcrArgDQA),
	m_fMultiStage(fMultiStage)
{
	GPOS_ASSERT(NULL != pdrgpcr);
	GPOS_ASSERT(COperator::EgbaggtypeSentinel > egbaggtype);
	GPOS_ASSERT_IMP(EgbaggtypeGlobal != egbaggtype, fMultiStage);

	ULONG ulDistrReqs = 1;
	if (pdrgpcrMinimal == NULL || 0 == pdrgpcrMinimal->UlLength())
	{
		pdrgpcr->AddRef();
		m_pdrgpcrMinimal = pdrgpcr;
	}
	else
	{
		pdrgpcrMinimal->AddRef();
		m_pdrgpcrMinimal = pdrgpcrMinimal;
	}

	if (COperator::EgbaggtypeLocal == egbaggtype)
	{
		// If the local aggregate has no distinct columns we generate
		// two optimization requests for its children:
		// (1) Any distribution requirement
		//
		// (2)	Random distribution requirement; this is needed to alleviate
		//		possible data skew

		ulDistrReqs = 2;
		if (pdrgpcrArgDQA != NULL && 0 != pdrgpcrArgDQA->UlLength())
		{
			// If the local aggregate has distinct columns we generate
			// one optimization requests for its children:
			// (1) hash distribution on the distinct columns
			ulDistrReqs = 1;
		}
	}
	else if (COperator::EgbaggtypeIntermediate == egbaggtype)
	{
		GPOS_ASSERT(NULL != pdrgpcrArgDQA);
		GPOS_ASSERT(pdrgpcrArgDQA->UlLength() <= pdrgpcr->UlLength());
		// Intermediate Agg generates two optimization requests for its children:
		// (1) Hash distribution on the group by columns + distinct column
		// (2) Hash distribution on the group by columns

		ulDistrReqs = 2;

		if (pdrgpcrArgDQA->UlLength() == pdrgpcr->UlLength())
		{
			// scalar aggregates so we only request the first case
			ulDistrReqs = 1;
		}
	}
	else if (COperator::EgbaggtypeGlobal == egbaggtype)
	{
		// Global Agg generates two optimization requests for its children:
		// (1) Singleton distribution, if child has volatile functions
		// (2) Hash distribution on the group by columns
		ulDistrReqs = 2;
	}

	SetDistrRequests(ulDistrReqs);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::~CPhysicalAgg
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalAgg::~CPhysicalAgg()
{
	m_pdrgpcr->Release();
	m_pdrgpcrMinimal->Release();
	CRefCount::SafeRelease(m_pdrgpcrArgDQA);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalAgg::PcrsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
{
	return PcrsRequiredAgg(pmp, exprhdl, pcrsRequired, ulChildIndex, m_pdrgpcr);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PcrsRequiredAgg
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalAgg::PcrsRequiredAgg
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG ulChildIndex,
	DrgPcr *pdrgpcrGrp
	)
{
	GPOS_ASSERT(NULL != pdrgpcrGrp);
	GPOS_ASSERT(0 == ulChildIndex &&
				"Required properties can only be computed on the relational child");

	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);

	// include grouping columns
	pcrs->Include(pdrgpcrGrp);
	pcrs->Union(pcrsRequired);

	CColRefSet *pcrsOutput = PcrsChildReqd(pmp, exprhdl, pcrs, ulChildIndex, 1 /*ulScalarIndex*/);
	pcrs->Release();

	return pcrsOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalAgg::PdsRequiredAgg
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CDistributionSpec *pdsInput,
	ULONG ulChildIndex,
	ULONG  ulOptReq,
	DrgPcr *pdrgpcgGrp,
	DrgPcr *pdrgpcrGrpMinimal
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);

	if (FGlobal())
	{
		return PdsRequiredGlobalAgg(pmp, exprhdl, pdsInput, ulChildIndex, pdrgpcgGrp, pdrgpcrGrpMinimal, ulOptReq);
	}

	if (COperator::EgbaggtypeIntermediate == m_egbaggtype)
	{
		return PdsRequiredIntermediateAgg(pmp, ulOptReq);
	}

	// if expression has to execute on master then we need a gather
	if (exprhdl.FMasterOnly())
	{
		return PdsEnforceMaster(pmp, exprhdl, pdsInput, ulChildIndex);
	}

	if (COperator::EgbaggtypeLocal == m_egbaggtype && m_pdrgpcrArgDQA != NULL && 0 != m_pdrgpcrArgDQA->UlLength())
	{
		GPOS_ASSERT(0 == ulOptReq);
		return PdsMaximalHashed(pmp, m_pdrgpcrArgDQA);
	}

	GPOS_ASSERT(0 == ulOptReq || 1 == ulOptReq);

	if (0 == ulOptReq)
	{
		return GPOS_NEW(pmp) CDistributionSpecAny(this->Eopid());
	}

	// we randomly distribute the input for skew-elimination
	return GPOS_NEW(pmp) CDistributionSpecRandom();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PdsMaximalHashed
//
//	@doc:
//		Compute a maximal hashed distribution using the given columns,
//		if no such distribution can be created, return a Singleton distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalAgg::PdsMaximalHashed
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcr
	)
{
	GPOS_ASSERT(NULL != pdrgpcr);

	CDistributionSpecHashed *pdshashedMaximal =
			CDistributionSpecHashed::PdshashedMaximal
				(
				pmp,
				pdrgpcr,
				true /*fNullsColocated*/
				);
	if (NULL != pdshashedMaximal)
	{
		return pdshashedMaximal;
	}

	// otherwise, require a singleton explicitly
	return GPOS_NEW(pmp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PdsRequiredGlobalAgg
//
//	@doc:
//		Compute required distribution of the n-th child of a global agg
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalAgg::PdsRequiredGlobalAgg
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CDistributionSpec *pdsInput,
	ULONG ulChildIndex,
	DrgPcr *pdrgpcrGrp,
	DrgPcr *pdrgpcrGrpMinimal,
	ULONG  ulOptReq
	)
	const
{
	GPOS_ASSERT(FGlobal());
	GPOS_ASSERT(2 > ulOptReq);

	// TODO:  - Mar 19, 2012; Cleanup: move this check to the caller
	if (exprhdl.FHasOuterRefs())
	{
		return PdsPassThru(pmp, exprhdl, pdsInput, ulChildIndex);
	}

	if (0 == pdrgpcrGrp->UlLength())
	{
		if (CDistributionSpec::EdtSingleton == pdsInput->Edt())
		{
			// pass through input distribution if it is a singleton
			pdsInput->AddRef();
			return pdsInput;
		}

		// otherwise, require a singleton explicitly
		return GPOS_NEW(pmp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
	}

	if (0 == ulOptReq &&
		(IMDFunction::EfsVolatile == exprhdl.Pdprel(0)->Pfp()->Efs()))
	{
		// request a singleton distribution if child has volatile functions
		return GPOS_NEW(pmp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
	}

	// if there are grouping columns, require a hash distribution explicitly
	return PdsMaximalHashed(pmp, pdrgpcrGrpMinimal);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PdsRequiredIntermediateAgg
//
//	@doc:
//		Compute required distribution of the n-th child of an intermediate 
//		aggregate operator
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalAgg::PdsRequiredIntermediateAgg
	(
	IMemoryPool *pmp,
	ULONG  ulOptReq
	)
	const
{
	GPOS_ASSERT(COperator::EgbaggtypeIntermediate == m_egbaggtype);

	if (0 == ulOptReq)
	{
		return PdsMaximalHashed(pmp, m_pdrgpcr);
	}

	DrgPcr *pdrgpcr = GPOS_NEW(pmp) DrgPcr(pmp);
	const ULONG ulLen = m_pdrgpcr->UlLength() - m_pdrgpcrArgDQA->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CColRef *pcr = (*m_pdrgpcr)[ul];
		pdrgpcr->Append(pcr);
	}

	CDistributionSpec *pds = PdsMaximalHashed(pmp, pdrgpcr);
	pdrgpcr->Release();

	return pds;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalAgg::PrsRequired
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
	GPOS_ASSERT(0 == ulChildIndex);

	// if there are outer refs that are not coming from the child, this means
	// that the grouping columns have outer refs, so we need a materialize
	if (exprhdl.FHasOuterRefs() && !exprhdl.FHasOuterRefs(0))
	{
		return GPOS_NEW(pmp) CRewindabilitySpec(CRewindabilitySpec::ErtGeneral);
	}

	return PrsPassThru(pmp, exprhdl, prsRequired, ulChildIndex);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalAgg::PppsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CPartitionPropagationSpec *pppsRequired,
	ULONG 
#ifdef GPOS_DEBUG
	ulChildIndex
#endif
	,
	DrgPdp *, //pdrgpdpCtxt,
	ULONG //ulOptReq
	)
{
	GPOS_ASSERT(0 == ulChildIndex);
	GPOS_ASSERT(NULL != pppsRequired);

	return CPhysical::PppsRequiredPushThruUnresolvedUnary(pmp, exprhdl, pppsRequired, CPhysical::EppcAllowed);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalAgg::PcteRequired
	(
	IMemoryPool *, //pmp,
	CExpressionHandle &, //exprhdl,
	CCTEReq *pcter,
	ULONG
#ifdef GPOS_DEBUG
	ulChildIndex
#endif
	,
	DrgPdp *, //pdrgpdpCtxt,
	ULONG //ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);
	return PcterPushThru(pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalAgg::FProvidesReqdCols
	(
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(NULL != pcrsRequired);
	GPOS_ASSERT(2 == exprhdl.UlArity());

	CColRefSet *pcrs = GPOS_NEW(m_pmp) CColRefSet(m_pmp);
	
	// include grouping columns
	pcrs->Include(PdrgpcrGroupingCols());
	
	// include defined columns by scalar child
	pcrs->Union(exprhdl.Pdpscalar(1 /*ulChildIndex*/)->PcrsDefined());
	BOOL fProvidesCols = pcrs->FSubset(pcrsRequired);
	pcrs->Release();

	return fProvidesCols;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalAgg::PdsDerive
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	return PdsDerivePassThruOuter(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalAgg::PrsDerive
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
//		CPhysicalAgg::UlHash
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CPhysicalAgg::UlHash() const
{
	ULONG ulHash = COperator::UlHash();
	const ULONG ulArity = m_pdrgpcr->UlLength();
	ULONG ulGbaggtype = (ULONG) m_egbaggtype;
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CColRef *pcr = (*m_pdrgpcr)[ul];
		ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHashPtr<CColRef>(pcr));
	}

	ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHash<ULONG>(&ulGbaggtype));

	return  gpos::UlCombineHashes(ulHash, gpos::UlHash<BOOL>(&m_fGeneratesDuplicates));
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::FMatch
//
//	@doc:
//		Match operator
//
//---------------------------------------------------------------------------
BOOL
CPhysicalAgg::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CPhysicalAgg *popAgg = reinterpret_cast<CPhysicalAgg*> (pop);

	if (FGeneratesDuplicates() != popAgg->FGeneratesDuplicates())
	{
		return false;
	}

	if (popAgg->Egbaggtype() == m_egbaggtype && m_pdrgpcr->FEqual(popAgg->m_pdrgpcr))
	{
		if (CColRef::FEqual(m_pdrgpcrMinimal, popAgg->m_pdrgpcrMinimal))
		{
			return (m_pdrgpcrArgDQA == NULL || 0 == m_pdrgpcrArgDQA->UlLength()) ||
				CColRef::FEqual(m_pdrgpcrArgDQA, popAgg->PdrgpcrArgDQA());
		}
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::EpetDistribution
//
//	@doc:
//		Return the enforcing type for distribution property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalAgg::EpetDistribution
	(
	CExpressionHandle &exprhdl,
	const CEnfdDistribution *ped
	)
	const
{
	GPOS_ASSERT(NULL != ped);

	// get distribution delivered by the aggregate node
	CDistributionSpec *pds = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pds();

	if (ped->FCompatible(pds))
	{
		if (COperator::EgbaggtypeLocal != Egbaggtype())
		{
			return CEnfdProp::EpetUnnecessary;
		}

		// prohibit the plan if local aggregate already delivers the enforced distribution, since
		// otherwise we would create two aggregates with no intermediate motion operators
		return CEnfdProp::EpetProhibited;

	}

	// required distribution will be enforced on Agg's output
	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalAgg::EpetRewindability
	(
	CExpressionHandle &exprhdl,
	const CEnfdRewindability *per
	)
	const
{
	// get rewindability delivered by the Agg node
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
//		CPhysicalAgg::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalAgg::OsPrint
	(
	IOstream &os
	)
	const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	os	<< SzId()
		<< "( ";
	CLogicalGbAgg::OsPrintGbAggType(os, m_egbaggtype);
	if (m_fMultiStage)
	{
		os << ", multi-stage";
	}
	os	<< " )";


	os	<< " Grp Cols: [";
	
	CUtils::OsPrintDrgPcr(os, m_pdrgpcr);
	os	<< "]"
		<< ", Minimal Grp Cols:[";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrMinimal);
	os	<< "]";
	
	if (COperator::EgbaggtypeIntermediate == m_egbaggtype)
	{
		os	<< ", Distinct Cols:[";
		CUtils::OsPrintDrgPcr(os, m_pdrgpcrArgDQA);
		os	<< "]";
	}
	os	<< ", Generates Duplicates :[ " << FGeneratesDuplicates() << " ] ";

	return os;
}

// EOF

