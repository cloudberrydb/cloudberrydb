//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CPartialPlan.cpp
//
//	@doc:
//		Implementation of partial plans created during optimization
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/engine/CPartialPlan.h"
#include "gpopt/search/CGroupExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalMotion.h"
#include "gpopt/search/CGroup.h"

#include "gpopt/exception.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPartialPlan::CPartialPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartialPlan::CPartialPlan
	(
	CGroupExpression *pgexpr,
	CReqdPropPlan *prpp,
	CCostContext *pccChild,
	ULONG ulChildIndex
	)
	:
	m_pgexpr(pgexpr),	// not owned
	m_prpp(prpp),
	m_pccChild(pccChild),	// cost context of an already optimized child
	m_ulChildIndex(ulChildIndex)
{
	GPOS_ASSERT(NULL != pgexpr);
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT_IMP(NULL != pccChild, ulChildIndex < pgexpr->UlArity());
}


//---------------------------------------------------------------------------
//	@function:
//		CPartialPlan::~CPartialPlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPartialPlan::~CPartialPlan()
{
	m_prpp->Release();
	CRefCount::SafeRelease(m_pccChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartialPlan::ExtractChildrenCostingInfo
//
//	@doc:
//		Extract costing info from children
//
//---------------------------------------------------------------------------
void
CPartialPlan::ExtractChildrenCostingInfo
	(
	IMemoryPool *pmp,
	ICostModel *pcm,
	CExpressionHandle &exprhdl,
	ICostModel::SCostingInfo *pci
	)
{
	GPOS_ASSERT(m_pgexpr == exprhdl.Pgexpr());
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT_IMP(NULL != m_pccChild, m_ulChildIndex < exprhdl.UlArity());

	const ULONG ulArity = m_pgexpr->UlArity();
	ULONG ulIndex = 0;
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CGroup *pgroupChild = (*m_pgexpr)[ul];
		if (pgroupChild->FScalar())
		{
			// skip scalar children
			continue;
		}

		CReqdPropPlan *prppChild = exprhdl.Prpp(ul);
		IStatistics *pstatsChild = pgroupChild->Pstats();
		RaiseExceptionIfStatsNull(pstatsChild);

		if (ul == m_ulChildIndex)
		{
			// we have reached a child with a known plan,
			// we have perfect costing information about this child

			// use stats in provided child context
			pstatsChild = m_pccChild->Pstats();

			// use provided child cost context to collect accurate costing info
			DOUBLE dRowsChild = pstatsChild->DRows().DVal();
			if (CDistributionSpec::EdptPartitioned == m_pccChild->Pdpplan()->Pds()->Edpt())
			{
				// scale statistics row estimate by number of segments
				dRowsChild = pcm->DRowsPerHost(CDouble(dRowsChild)).DVal();
			}

			pci->SetChildRows(ulIndex, dRowsChild);
			DOUBLE dWidthChild = pstatsChild->DWidth(pmp, prppChild->PcrsRequired()).DVal();
			pci->SetChildWidth(ulIndex, dWidthChild);
			pci->SetChildRebinds(ulIndex, pstatsChild->DRebinds().DVal());
			pci->SetChildCost(ulIndex, m_pccChild->Cost().DVal());

			// continue with next child
			ulIndex++;
			continue;
		}

		// otherwise, we do not know child plan yet,
		// we assume lower bounds on child row estimate and cost
		DOUBLE dRowsChild = pstatsChild->DRows().DVal();
		dRowsChild = pcm->DRowsPerHost(CDouble(dRowsChild)).DVal();
		pci->SetChildRows(ulIndex, dRowsChild);

		pci->SetChildRebinds(ulIndex, pstatsChild->DRebinds().DVal());

		DOUBLE dWidthChild =  pstatsChild->DWidth(pmp, prppChild->PcrsRequired()).DVal();
		pci->SetChildWidth(ulIndex, dWidthChild);

		// use child group's cost lower bound as the child cost
		DOUBLE dCostChild = pgroupChild->CostLowerBound(pmp, prppChild).DVal();
		pci->SetChildCost(ulIndex, dCostChild);

		// advance to next child
		ulIndex++;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CPartialPlan::RaiseExceptionIfStatsNull
//
//	@doc:
//		Raise exception if the stats object is NULL
//
//---------------------------------------------------------------------------
void
CPartialPlan::RaiseExceptionIfStatsNull
	(
	IStatistics *pstats
	)
{
	if (NULL == pstats)
	{
		GPOS_RAISE
			(
			gpopt::ExmaGPOPT,
			gpopt::ExmiNoPlanFound,
			GPOS_WSZ_LIT("Could not compute cost of partial plan since statistics for the group not derived")
			);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CPartialPlan::CostCompute
//
//	@doc:
//		Compute partial plan cost
//
//---------------------------------------------------------------------------
CCost
CPartialPlan::CostCompute
	(
	IMemoryPool *pmp
	)
{
	CExpressionHandle exprhdl(pmp);
	exprhdl.Attach(m_pgexpr);

	// init required properties of expression
	exprhdl.DeriveProps(NULL /*pdpdrvdCtxt*/);
	exprhdl.InitReqdProps(m_prpp);

	// create array of child derived properties
	DrgPdp *pdrgpdp = GPOS_NEW(pmp) DrgPdp(pmp);
	const ULONG ulArity =  m_pgexpr->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		// compute required columns of the n-th child
		exprhdl.ComputeChildReqdCols(ul, pdrgpdp);
	}
	pdrgpdp->Release();

	IStatistics *pstats = m_pgexpr->Pgroup()->Pstats();
	RaiseExceptionIfStatsNull(pstats);

	pstats->AddRef();
	ICostModel::SCostingInfo ci(pmp, exprhdl.UlNonScalarChildren(), GPOS_NEW(pmp) ICostModel::CCostingStats(pstats));

	ICostModel *pcm = COptCtxt::PoctxtFromTLS()->Pcm();
	ExtractChildrenCostingInfo(pmp, pcm, exprhdl, &ci);

	CDistributionSpec::EDistributionPartitioningType edpt = CDistributionSpec::EdptSentinel;
	if (NULL != m_prpp->Ped())
	{
		edpt = m_prpp->Ped()->PdsRequired()->Edpt();
	}

	COperator *pop = m_pgexpr->Pop();
	BOOL fDataPartitioningMotion =
		CUtils::FPhysicalMotion(pop) &&
		CDistributionSpec::EdptPartitioned == CPhysicalMotion::PopConvert(pop)->Pds()->Edpt();

	// extract rows from stats
	DOUBLE dRows = m_pgexpr->Pgroup()->Pstats()->DRows().DVal();
	if (
		fDataPartitioningMotion ||	// root operator is known to distribute data across segments
		NULL == m_prpp->Ped() ||	// required distribution not known yet, we assume data partitioning since we need a lower-bound on number of rows
		CDistributionSpec::EdptPartitioned == edpt || // required distribution is known to be partitioned, we assume data partitioning since we need a lower-bound on number of rows
		CDistributionSpec::EdptUnknown == edpt // required distribution is not known to be partitioned (e.g., ANY distribution), we assume data partitioning since we need a lower-bound on number of rows
		)
	{
		// use rows per host as a cardinality lower bound
		dRows = pcm->DRowsPerHost(CDouble(dRows)).DVal();
	}
	ci.SetRows(dRows);

	// extract width from stats
	DOUBLE dWidth = m_pgexpr->Pgroup()->Pstats()->DWidth(pmp, m_prpp->PcrsRequired()).DVal();
	ci.SetWidth(dWidth);

	// extract rebinds
	DOUBLE dRebinds = m_pgexpr->Pgroup()->Pstats()->DRebinds().DVal();
	ci.SetRebinds(dRebinds);

	// compute partial plan cost
	CCost cost = pcm->Cost(exprhdl, &ci);

	if (0 < ci.UlChildren() && 1.0 < cost.DVal())
	{
		// cost model implementation adds an artificial const (1.0) to
		// sum of children cost,
		// we subtract this 1.0 here since we compute a lower bound

		// TODO:  05/07/2014: remove artificial const 1.0 in CostSum() function
		cost = CCost(cost.DVal() - 1.0);
	}

	return cost;
}


//---------------------------------------------------------------------------
//	@function:
//		CPartialPlan::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CPartialPlan::UlHash
	(
	const CPartialPlan *ppp
	)
{
	GPOS_ASSERT(NULL != ppp);

	ULONG ulHash = ppp->Pgexpr()->UlHash();
	return UlCombineHashes(ulHash, CReqdPropPlan::UlHashForCostBounding(ppp->Prpp()));
}


//---------------------------------------------------------------------------
//	@function:
//		CPartialPlan::FEqual
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CPartialPlan::FEqual
	(
	const CPartialPlan *pppFst,
	const CPartialPlan *pppSnd
	)
{
	GPOS_ASSERT(NULL != pppFst);
	GPOS_ASSERT(NULL != pppSnd);

	BOOL fEqual = false;
	if (NULL == pppFst->PccChild() || NULL == pppSnd->PccChild())
	{
		fEqual = (NULL == pppFst->PccChild() && NULL == pppSnd->PccChild());
	}
	else
	{
		// use pointers for fast comparison
		fEqual = (pppFst->PccChild() ==  pppSnd->PccChild());
	}

	return
		fEqual &&
		pppFst->UlChildIndex() == pppSnd->UlChildIndex() &&
		pppFst->Pgexpr() == pppSnd->Pgexpr() &&   // use pointers for fast comparison
		CReqdPropPlan::FEqualForCostBounding(pppFst->Prpp(), pppSnd->Prpp());
}


// EOF
