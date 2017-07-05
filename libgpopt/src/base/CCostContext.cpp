//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CCostContext.cpp
//
//	@doc:
//		Implementation of cost context
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"
#include "gpopt/base/CCostContext.h"
#include "gpopt/base/CDistributionSpecHashed.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CDrvdPropCtxtPlan.h"
#include "gpopt/base/CDrvdPropPlan.h"
#include "gpopt/base/CDrvdPropCtxtRelational.h"
#include "gpopt/cost/ICostModel.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalDynamicTableScan.h"
#include "gpopt/operators/CPhysicalDynamicIndexScan.h"

#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/search/CGroupExpression.h"

#include "naucrates/statistics/CStatisticsUtils.h"

#include "gpopt/exception.h"

using namespace gpopt;
using namespace gpnaucrates;

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::CCostContext
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CCostContext::CCostContext
	(
	IMemoryPool *pmp,
	COptimizationContext *poc,
	ULONG ulOptReq,
	CGroupExpression *pgexpr
	)
	:
	m_pmp(pmp),
	m_cost(GPOPT_INVALID_COST),
	m_estate(estUncosted),
	m_pgexpr(pgexpr),
	m_pgexprForStats(NULL),
	m_pdrgpoc(NULL),
	m_pdpplan(NULL),
	m_ulOptReq(ulOptReq),
	m_fPruned(false),
	m_pstats(NULL),
	m_poc(poc)
{
	GPOS_ASSERT(NULL != poc);
	GPOS_ASSERT(NULL != pgexpr);
	GPOS_ASSERT_IMP
		(
		pgexpr->Pop()->FPhysical(),
		ulOptReq < CPhysical::PopConvert(pgexpr->Pop())->UlOptRequests()
		);

	if (!m_pgexpr->Pop()->FScalar() &&
		!CPhysical::PopConvert(m_pgexpr->Pop())->FPassThruStats())
	{
		CGroupExpression *pgexprForStats = m_pgexpr->Pgroup()->PgexprBestPromise(m_pmp, m_pgexpr);
		if (NULL != pgexprForStats)
		{
			pgexprForStats->AddRef();
			m_pgexprForStats = pgexprForStats;
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CCostContext::~CCostContext
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CCostContext::~CCostContext()
{
	CRefCount::SafeRelease(m_pgexpr);
	CRefCount::SafeRelease(m_pgexprForStats);
	CRefCount::SafeRelease(m_poc);
	CRefCount::SafeRelease(m_pdrgpoc);
	CRefCount::SafeRelease(m_pdpplan);
	CRefCount::SafeRelease(m_pstats);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostContext::FOwnsStats
//
//	@doc:
//		Check if new stats are owned by this context
//
//---------------------------------------------------------------------------
BOOL
CCostContext::FOwnsStats() const
{
	GPOS_ASSERT(NULL != m_pstats);

	// new stats are owned if context holds stats different from group stats
	return (m_pstats != m_pgexpr->Pgroup()->Pstats());
}


//---------------------------------------------------------------------------
//	@function:
//		CCostContext::FNeedsNewStats
//
//	@doc:
//		Check if we need to derive new stats for this context,
//		by default a cost context inherits stats from the owner group,
//		the only current exception is when part of the plan below cost
//		context is affected by partition elimination done by partition
//		selection in some other part of the plan
//
//---------------------------------------------------------------------------
BOOL
CCostContext::FNeedsNewStats() const
{
	COperator *pop = m_pgexpr->Pop();
	if (pop->FScalar())
	{
		// return false if scalar operator
		return false;
	}

	CEnfdPartitionPropagation *pepp = Poc()->Prpp()->Pepp();

	if (GPOS_FTRACE(EopttraceDeriveStatsForDPE) &&
		CUtils::FPhysicalScan(pop) &&
		CPhysicalScan::PopConvert(pop)->FDynamicScan() &&
		!pepp->PpfmDerived()->FEmpty())
	{
		// context is attached to a dynamic scan that went through
		// partition elimination in another part of the plan
		return true;
	}

	// we need to derive stats if any child has modified stats
	BOOL fDeriveStats = false;
	const ULONG ulArity = Pdrgpoc()->UlLength();
	for (ULONG ul = 0; !fDeriveStats && ul < ulArity; ul++)
	{
		COptimizationContext *pocChild = (*Pdrgpoc())[ul];
		CCostContext *pccChild = pocChild->PccBest();
		GPOS_ASSERT(NULL != pccChild);

		fDeriveStats = pccChild->FOwnsStats();
	}

	return fDeriveStats;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostContext::DeriveStats
//
//	@doc:
//		Derive stats of owner group expression
//
//---------------------------------------------------------------------------
void
CCostContext::DeriveStats()
{
	GPOS_ASSERT(NULL != m_pgexpr);
	GPOS_ASSERT(NULL != m_poc);

	if (NULL != m_pstats)
	{
		// stats are already derived
		return;
	}

	if (m_pgexpr->Pop()->FScalar())
	{
		// exit if scalar operator
		return;
	}

	CExpressionHandle exprhdl(m_pmp);
	exprhdl.Attach(this);
	exprhdl.DeriveCostContextStats();
	if (NULL == exprhdl.Pstats())
	{
		GPOS_RAISE
			(
			gpopt::ExmaGPOPT,
			gpopt::ExmiNoPlanFound,
			GPOS_WSZ_LIT("Could not compute cost since statistics for the group no derived")
			);
	}

	exprhdl.Pstats()->AddRef();
	m_pstats = exprhdl.Pstats();
}


//---------------------------------------------------------------------------
//	@function:
//		CCostContext::DerivePlanProps
//
//	@doc:
//		Derive properties of the plan carried by cost context
//
//---------------------------------------------------------------------------
void
CCostContext::DerivePlanProps
	(
	IMemoryPool *pmp
	)
{
	GPOS_ASSERT(NULL != m_pdrgpoc);

	if (NULL == m_pdpplan)
	{
		// derive properties of the plan carried by cost context
		CExpressionHandle exprhdl(pmp);
		exprhdl.Attach(this);
		exprhdl.DerivePlanProps();
		CDrvdPropPlan *pdpplan = CDrvdPropPlan::Pdpplan(exprhdl.Pdp());
		GPOS_ASSERT(NULL != pdpplan);

		// set derived plan properties
		pdpplan->AddRef();
		m_pdpplan = pdpplan;
		GPOS_ASSERT(NULL != m_pdpplan);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CCostContext::operator ==
//
//	@doc:
//		Comparison operator
//
//---------------------------------------------------------------------------
BOOL
CCostContext::operator ==
	(
	const CCostContext &cc
	)
	const
{
	return FEqual(cc, *this);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostContext::FValid
//
//	@doc:
//		Check validity by comparing derived and required properties
//
//---------------------------------------------------------------------------
BOOL
CCostContext::FValid
	(
	IMemoryPool *pmp
	)
{
	GPOS_ASSERT(NULL != m_poc);
	GPOS_ASSERT(NULL != m_pdrgpoc);

	// obtain relational properties from group
	CDrvdPropRelational *pdprel = CDrvdPropRelational::Pdprel(Pgexpr()->Pgroup()->Pdp());
	GPOS_ASSERT(NULL != pdprel);

	// derive plan properties
	DerivePlanProps(pmp);

	// checking for required properties satisfaction
	BOOL fValid = Poc()->Prpp()->FSatisfied(pdprel, m_pdpplan);

#ifdef GPOS_DEBUG
	if (COptCtxt::FAllEnforcersEnabled() && !fValid)
	{
		CAutoTrace at(pmp);
		IOstream &os = at.Os();

		os << std::endl << "PROPERTY MISMATCH:" << std::endl;
		os << std::endl << "GEXPR:" << std::endl << Pgexpr();
		os << std::endl << "REQUIRED PROPERTIES:" << std::endl << *(m_poc->Prpp());
		os << std::endl << "DERIVED PROPERTIES:" << std::endl << *pdprel << std::endl << *m_pdpplan;
	}
#endif //GPOS_DEBUG

	return fValid;
}



//---------------------------------------------------------------------------
//	@function:
//		CCostContext::FBreakCostTiesForJoinPlan
//
//	@doc:
//		For two cost contexts with join plans of the same cost, break the
//		tie in cost values based on join depth,
//		if tie-resolution succeeded, store a pointer to preferred cost
//		context in output argument
//
//---------------------------------------------------------------------------
void
CCostContext::BreakCostTiesForJoinPlans
	(
	const CCostContext *pccFst,
	const CCostContext *pccSnd,
	CONST_COSTCTXT_PTR *ppccPrefered, // output: preferred cost context
	BOOL *pfTiesResolved // output: if true, tie resolution has succeeded
	)
{
	GPOS_ASSERT(NULL != pccFst);
	GPOS_ASSERT(NULL != pccSnd);
	GPOS_ASSERT(NULL != ppccPrefered);
	GPOS_ASSERT(NULL != pfTiesResolved);
	GPOS_ASSERT(*(pccFst->Poc()) == *(pccSnd->Poc()));
	GPOS_ASSERT(estCosted == pccFst->Est());
	GPOS_ASSERT(estCosted == pccSnd->Est());
	GPOS_ASSERT(pccFst->Cost() == pccSnd->Cost());
	GPOS_ASSERT(CUtils::FPhysicalJoin(pccFst->Pgexpr()->Pop()));
	GPOS_ASSERT(CUtils::FPhysicalJoin(pccSnd->Pgexpr()->Pop()));

	// for two join plans with the same estimated rows in both children,
	// prefer the plan that has smaller tree depth on the inner side,
	// this is because a smaller tree depth means that row estimation
	// errors are not grossly amplified,
	// since we build a hash table/broadcast the inner side, we need
	// to have more reliable statistics on this side

	*pfTiesResolved = false;
	*ppccPrefered = NULL;
	CDouble dRowsOuterFst = (*pccFst->Pdrgpoc())[0]->PccBest()->Pstats()->DRows();
	CDouble dRowsInnerFst = (*pccFst->Pdrgpoc())[1]->PccBest()->Pstats()->DRows();
	if (dRowsOuterFst != dRowsInnerFst)
	{
		// two children of first plan have different row estimates
		return;
	}

	CDouble dRowsOuterSnd = (*pccSnd->Pdrgpoc())[0]->PccBest()->Pstats()->DRows();
	CDouble dRowsInnerSnd = (*pccSnd->Pdrgpoc())[1]->PccBest()->Pstats()->DRows();
	if (dRowsOuterSnd != dRowsInnerSnd)
	{
		// two children of second plan have different row estimates
		return;
	}

	if (dRowsInnerFst != dRowsInnerSnd)
	{
		// children of first plan have different row estimates compared to second plan
		return;
	}

	// both plans have equal estimated rows for both children, break tie based on join depth
	*pfTiesResolved = true;
	ULONG ulOuterJoinDepthFst = CDrvdPropRelational::Pdprel((*pccFst->Pgexpr())[0]->Pdp())->UlJoinDepth();
	ULONG ulInnerJoinDepthFst = CDrvdPropRelational::Pdprel((*pccFst->Pgexpr())[1]->Pdp())->UlJoinDepth();
	if (ulInnerJoinDepthFst < ulOuterJoinDepthFst)
	{
		*ppccPrefered = pccFst;
	}
	else
	{
		*ppccPrefered = pccSnd;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CCostContext::FBetterThan
//
//	@doc:
//		Is current context better than the given equivalent context
//		based on cost?
//
//---------------------------------------------------------------------------
BOOL
CCostContext::FBetterThan
	(
	const CCostContext *pcc
	)
	const
{
	GPOS_ASSERT(NULL != pcc);
	GPOS_ASSERT(*m_poc == *(pcc->Poc()));
	GPOS_ASSERT(estCosted == m_estate);
	GPOS_ASSERT(estCosted == pcc->Est());

	DOUBLE dCostDiff = (Cost().DVal() - pcc->Cost().DVal());
	if (dCostDiff < 0.0)
	{
		// if current context has a strictly smaller cost, then it is preferred
		return true;
	}

	if (dCostDiff > 0.0)
	{
		// if current context has a strictly larger cost, then it is not preferred
		return false;
	}

	// otherwise, we need to break tie in cost values

	// RULE 1: a partitioned plan is better than a non-partitioned
	// one to optimize CPU consumption
	if (CDistributionSpec::EdptPartitioned == Pdpplan()->Pds()->Edpt() &&
		CDistributionSpec::EdptPartitioned != pcc->Pdpplan()->Pds()->Edpt())
	{
		return true;
	}

	// RULE 2: hashed distribution is preferred to random distribution since
	// it preserves knowledge of hash key
	if (CDistributionSpec::EdtHashed == Pdpplan()->Pds()->Edt() &&
		CDistributionSpec::EdtRandom == pcc->Pdpplan()->Pds()->Edt())
	{
		return true;
	}

	// RULE 3: break ties in cost of join plans,
	// if both plans have the same estimated rows for both children, prefer
	// the plan with deeper outer child
	if (CUtils::FPhysicalJoin(Pgexpr()->Pop()) &&
		CUtils::FPhysicalJoin(pcc->Pgexpr()->Pop()))
	{
		CONST_COSTCTXT_PTR pccPrefered = NULL;
		BOOL fSuccess = false;
		BreakCostTiesForJoinPlans(this, pcc, &pccPrefered, &fSuccess);
		if (fSuccess)
		{
			return (this == pccPrefered);
		}
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostContext::ComputeCost
//
//	@doc:
//		Compute cost of current context,
//
//		the function extracts cardinality and row width of owner operator
//		and child operators, and then adjusts row estimate obtained from
//		statistics based on data distribution obtained from plan properties,
//
//		statistics row estimate is computed on logical expressions by
//		estimating the size of the whole relation regardless data
//		distribution, on the other hand, optimizer's cost model computes
//		the cost of a plan instance on some segment,
//
//		when a plan produces tuples distributed to multiple segments, we
//		need to divide statistics row estimate by the number segments to
//		provide a per-segment row estimate for cost computation,
//
//		Note that this scaling of row estimate cannot happen during
//		statistics derivation since plans are not created yet at this point
//
// 		this function also extracts number of rebinds of owner operator child
//		operators, if statistics are computed using predicates with external
//		parameters (outer references), number of rebinds is the total number
//		of external parameters' values
//
//---------------------------------------------------------------------------
CCost
CCostContext::CostCompute
	(
	IMemoryPool *pmp,
	DrgPcost *pdrgpcostChildren
	)
{
	// derive context stats
	DeriveStats();

	ULONG ulArity = 0;
	if (NULL != m_pdrgpoc)
	{
		ulArity = Pdrgpoc()->UlLength();
	}

	m_pstats->AddRef();
	ICostModel::SCostingInfo ci(pmp, ulArity, GPOS_NEW(pmp) ICostModel::CCostingStats(m_pstats));

	ICostModel *pcm = COptCtxt::PoctxtFromTLS()->Pcm();

	CExpressionHandle exprhdl(pmp);
	exprhdl.Attach(this);

	// extract local costing info
	DOUBLE dRows = m_pstats->DRows().DVal();
	if (CDistributionSpec::EdptPartitioned == Pdpplan()->Pds()->Edpt())
	{
		// scale statistics row estimate by number of segments
		dRows = DRowsPerHost().DVal();
	}
	ci.SetRows(dRows);

	DOUBLE dWidth = m_pstats->DWidth(pmp, m_poc->Prpp()->PcrsRequired()).DVal();
	ci.SetWidth(dWidth);

	DOUBLE dRebinds = m_pstats->DRebinds().DVal();
	ci.SetRebinds(dRebinds);
	GPOS_ASSERT_IMP(!exprhdl.FHasOuterRefs(), GPOPT_DEFAULT_REBINDS == (ULONG) (dRebinds) && "invalid number of rebinds when there are no outer references");

	// extract children costing info
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		COptimizationContext *pocChild = (*m_pdrgpoc)[ul];
		CCostContext *pccChild = pocChild->PccBest();
		GPOS_ASSERT(NULL != pccChild);

		IStatistics *pstatsChild = pccChild->Pstats();
		DOUBLE dRowsChild = pstatsChild->DRows().DVal();
		if (CDistributionSpec::EdptPartitioned == pccChild->Pdpplan()->Pds()->Edpt())
		{
			// scale statistics row estimate by number of segments
			dRowsChild = pccChild->DRowsPerHost().DVal();
		}
		ci.SetChildRows(ul, dRowsChild);

		DOUBLE dWidthChild = pstatsChild->DWidth(pmp, pocChild->Prpp()->PcrsRequired()).DVal();
		ci.SetChildWidth(ul, dWidthChild);

		DOUBLE dRebindsChild = pstatsChild->DRebinds().DVal();
		ci.SetChildRebinds(ul, dRebindsChild);
		GPOS_ASSERT_IMP(!exprhdl.FHasOuterRefs(ul), GPOPT_DEFAULT_REBINDS == (ULONG) (dRebindsChild) && "invalid number of rebinds when there are no outer references");

		DOUBLE dCostChild =  (*pdrgpcostChildren)[ul]->DVal();
		ci.SetChildCost(ul, dCostChild);
	}

	// compute cost using the underlying cost model
	return pcm->Cost(exprhdl, &ci);
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::DRowsPerHost
//
//	@doc:
//		Return the number of rows per host
//
//---------------------------------------------------------------------------
CDouble
CCostContext::DRowsPerHost() const
{
	DOUBLE dRows = Pstats()->DRows().DVal();
	COptCtxt *poptctxt = COptCtxt::PoctxtFromTLS();
	const ULONG ulHosts = poptctxt->Pcm()->UlHosts();

	CDistributionSpec *pds =  Pdpplan()->Pds();
	if (CDistributionSpec::EdtHashed == pds->Edt())
	{
		CDistributionSpecHashed *pdshashed = CDistributionSpecHashed::PdsConvert(pds);
		DrgPexpr *pdrgpexpr = pdshashed->Pdrgpexpr();
		CColRefSet *pcrsUsed = CUtils::PcrsExtractColumns(m_pmp, pdrgpexpr);

		const CColRefSet *pcrsReqdStats = this->Poc()->Prprel()->PcrsStat();
		if (!pcrsReqdStats->FSubset(pcrsUsed))
		{
			// statistics not available for distribution columns, therefore
			// assume uniform distribution across hosts
			// clean up
			pcrsUsed->Release();

			return CDouble(dRows / ulHosts);
		}

		DrgPul *pdrgpul = GPOS_NEW(m_pmp) DrgPul(m_pmp);
		pcrsUsed->ExtractColIds(m_pmp, pdrgpul);
		pcrsUsed->Release();

		CStatisticsConfig *pstatsconf = poptctxt->Poconf()->Pstatsconf();
		CDouble dNDVs = CStatisticsUtils::DGroups(m_pmp, Pstats(), pstatsconf, pdrgpul, NULL /*pbsKeys*/);
		pdrgpul->Release();

		if (dNDVs < ulHosts)
		{
			// estimated number of distinct values of distribution columns is smaller than number of hosts.
			// We assume data is distributed across a subset of hosts in this case. This results in a larger
			// number of rows per host compared to the uniform case, allowing us to capture data skew in
			// cost computation
			return CDouble(dRows / dNDVs.DVal());
		}
	}

	return CDouble(dRows / ulHosts);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostContext::OsPrint
//
//	@doc:
//		Print function;
//
//---------------------------------------------------------------------------
IOstream &
CCostContext::OsPrint
	(
	IOstream &os
	)
	const
{
	os << "main ctxt (stage " << m_poc->UlSearchStageIndex() << ")" << m_poc->UlId() << "." << m_ulOptReq;

	if (NULL != m_pdrgpoc)
	{
		os << ", child ctxts:[";
		ULONG ulArity = m_pdrgpoc->UlLength();
		if (0 < ulArity)
		{
			for (ULONG i = 0; i < ulArity - 1; i++)
			{
				os << (*m_pdrgpoc)[i]->UlId();
				os << ", ";
			}
			os << (*m_pdrgpoc)[ulArity - 1]->UlId();
		}
		os << "]";
	}

	if (NULL != m_pstats)
	{
		os <<", rows:" << m_pstats->DRows();
		if (FOwnsStats())
		{
			os <<" (owned)";
		}
		else
		{
			os <<" (group)";
		}
	}

	if (m_fPruned)
	{
		os << ", cost lower bound: " << this->Cost() << "\t PRUNED";
	}
	else
	{
		os << ", cost: " << this->Cost();
	}

	return os << std::endl;
}

#ifdef GPOS_DEBUG
void
CCostContext::DbgPrint()
{
	CAutoTrace at(m_pmp);
	(void) this->OsPrint(at.Os());
}
#endif // GPOS_DEBUG

// EOF

