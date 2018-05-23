//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CGroupExpression.cpp
//
//	@doc:
//		Implementation of group expressions
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/error/CAutoTrace.h"
#include "gpos/task/CAutoSuspendAbort.h"
#include "gpos/task/CWorker.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/COptimizationContext.h"
#include "gpopt/operators/ops.h"
#include "gpopt/search/CGroupExpression.h"
#include "gpopt/search/CGroupProxy.h"

#include "gpopt/xforms/CXformFactory.h"
#include "gpopt/xforms/CXformUtils.h"

#include "gpos/string/CWStringDynamic.h"
#include "gpos/io/COstreamString.h"

#include "naucrates/traceflags/traceflags.h"

using namespace gpopt;

#define GPOPT_COSTCTXT_HT_BUCKETS	100

// invalid group expression
const CGroupExpression CGroupExpression::m_gexprInvalid;


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::CGroupExpression
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CGroupExpression::CGroupExpression
	(
	IMemoryPool *pmp,
	COperator *pop,
	DrgPgroup *pdrgpgroup,
	CXform::EXformId exfid,
	CGroupExpression *pgexprOrigin,
	BOOL fIntermediate
	)
	:
	m_pmp(pmp),
	m_ulId(GPOPT_INVALID_GEXPR_ID),
	m_pgexprDuplicate(NULL),
	m_pop(pop),
	m_pdrgpgroup(pdrgpgroup),
	m_pdrgpgroupSorted(NULL),
	m_pgroup(NULL),
	m_exfidOrigin(exfid),
	m_pgexprOrigin(pgexprOrigin),
	m_fIntermediate(fIntermediate),
	m_estate(estUnexplored),
	m_eol(EolLow),
	m_ppartialplancostmap(NULL)
{
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(NULL != pdrgpgroup);
	GPOS_ASSERT_IMP(exfid != CXform::ExfInvalid, NULL != pgexprOrigin);
	
	// store sorted array of children for faster comparison
	if (1 < pdrgpgroup->UlLength() && !pop->FInputOrderSensitive())
	{
		m_pdrgpgroupSorted = GPOS_NEW(pmp) DrgPgroup(pmp, pdrgpgroup->UlLength());
		m_pdrgpgroupSorted->AppendArray(pdrgpgroup);
		m_pdrgpgroupSorted->Sort();
		
		GPOS_ASSERT(m_pdrgpgroupSorted->FSorted());
	}

	m_ppartialplancostmap = GPOS_NEW(pmp) PartialPlanCostMap(pmp);

	// initialize cost contexts hash table
	m_sht.Init
		(
		pmp,
		GPOPT_COSTCTXT_HT_BUCKETS,
		GPOS_OFFSET(CCostContext, m_link),
		GPOS_OFFSET(CCostContext, m_poc),
		&(COptimizationContext::m_pocInvalid),
		COptimizationContext::UlHash,
		COptimizationContext::FEqual
		);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::~CGroupExpression
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CGroupExpression::~CGroupExpression()
{
	if (this != &(CGroupExpression::m_gexprInvalid))
	{
		CleanupContexts();

		m_pop->Release();
		m_pdrgpgroup->Release();

		CRefCount::SafeRelease(m_pdrgpgroupSorted);
		m_ppartialplancostmap->Release();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::CleanupContexts
//
//	@doc:
//		 Destroy stored cost contexts in hash table
//
//---------------------------------------------------------------------------
void
CGroupExpression::CleanupContexts()
{
	// need to suspend cancellation while cleaning up
	{
		CAutoSuspendAbort asa;

		ShtIter shtit(m_sht);
		CCostContext *pcc = NULL;
		while (NULL != pcc || shtit.FAdvance())
		{
			if (NULL != pcc)
			{
				pcc->Release();
			}

			// iter's accessor scope
			{
				ShtAccIter shtitacc(shtit);
				if (NULL != (pcc = shtitacc.Pt()))
				{
					shtitacc.Remove(pcc);
				}
			}
		}
	}

#ifdef GPOS_DEBUG
	CWorker::PwrkrSelf()->ResetTimeSlice();
#endif // GPOS_DEBUG

}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::Init
//
//	@doc:
//		Init group expression
//
//
//---------------------------------------------------------------------------
void
CGroupExpression::Init
	(
	CGroup *pgroup,
	ULONG ulId
	)
{
	SetGroup(pgroup);
	SetId(ulId);
	SetOptimizationLevel();
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::SetOptimizationLevel
//
//	@doc:
//		Set optimization level of group expression
//
//
//---------------------------------------------------------------------------
void
CGroupExpression::SetOptimizationLevel()
{
	// a sequence expression with a first child group that contains a CTE
	// producer gets a higher optimization level. This is to be sure that the
	// producer gets optimized before its consumers
	if (COperator::EopPhysicalSequence == m_pop->Eopid())
	{
		CGroup *pgroupFirst = (*this)[0];
		if (pgroupFirst->FHasCTEProducer())
		{
			m_eol = EolHigh;
		}
	}
	else if (CUtils::FHashJoin(m_pop))
	{
		// optimize hash join first to minimize plan cost quickly
		m_eol = EolHigh;
	}
	else if (CUtils::FPhysicalAgg(m_pop))
	{
		BOOL fPreferMultiStageAgg = GPOS_FTRACE(EopttraceForceMultiStageAgg);
		if (!fPreferMultiStageAgg && COperator::EopPhysicalHashAgg == m_pop->Eopid())
		{
			// if we choose agg plans based on cost only (no preference for multi-stage agg), 
			// we optimize hash agg first to to minimize plan cost quickly
			m_eol = EolHigh;
			return;
		}

		// if we only want plans with multi-stage agg, we generate multi-stage agg
		// first to avoid later optimization of one stage agg if possible                                   
		BOOL fMultiStage = CPhysicalAgg::PopConvert(m_pop)->FMultiStage();
		if (fPreferMultiStageAgg && fMultiStage)
		{
			// optimize multi-stage agg first to allow avoiding one-stage agg if possible
			m_eol = EolHigh;
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::FValidContext
//
//	@doc:
//		Check if group expression is valid with respect to given child contexts
//
//		This is called during cost computation phase in group expression
//		optimization after enforcement is complete. Since it is called bottom-up,
//		for the given physical group expression, all the derived properties are
//		already computed.
//
//		Since property enforcement in CEngine::FCheckEnfdProps() only determines
//		whether or not an enforcer is added to the group, it is possible for the
//		enforcer group expression to select a child group expression that did not
//		create the enforcer. This could lead to invalid plans that could not have
//		been prevented earlier because derived physical properties weren't
//		available. For example, a Motion group expression may select as a child a
//		DynamicTableScan that has unresolved part propagators, instead of picking
//		the PartitionSelector enforcer which would resolve it.
//
//		This method can be used to reject such plans.
//
//---------------------------------------------------------------------------
BOOL
CGroupExpression::FValidContext
	(
	IMemoryPool *pmp,
	COptimizationContext *poc,
	DrgPoc *pdrgpocChild
	)
{
	GPOS_ASSERT(m_pop->FPhysical());

	return CPhysical::PopConvert(m_pop)->FValidContext(pmp, poc, pdrgpocChild);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::SetId
//
//	@doc:
//		Set id of expression
//
//---------------------------------------------------------------------------
void 
CGroupExpression::SetId
	(
	ULONG ulId
	)
{
	GPOS_ASSERT(GPOPT_INVALID_GEXPR_ID == m_ulId);

	m_ulId = ulId;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::SetGroup
//
//	@doc:
//		Set group pointer of expression
//
//---------------------------------------------------------------------------
void 
CGroupExpression::SetGroup
	(
	CGroup *pgroup
	)
{
	GPOS_ASSERT(NULL == m_pgroup);
	GPOS_ASSERT(NULL != pgroup);
	
	m_pgroup = pgroup;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::FCostContextExists
//
//	@doc:
//		Check if cost context already exists in group expression hash table
//
//---------------------------------------------------------------------------
BOOL
CGroupExpression::FCostContextExists
	(
	COptimizationContext *poc,
	DrgPoc *pdrgpoc
	)
{
	GPOS_ASSERT(NULL != poc);

	// lookup context based on required properties
	CCostContext *pccFound = NULL;
	{
		ShtAcc shta(Sht(), poc);
		pccFound = shta.PtLookup();
	}

	while (NULL != pccFound)
	{
		if (COptimizationContext::FEqualContextIds(pdrgpoc, pccFound->Pdrgpoc()))
		{
			// a cost context, matching required properties and child contexts, was already created
			return true;
		}

		{
			ShtAcc shta(Sht(), poc);
			pccFound = shta.PtNext(pccFound);
		}
	}

	return false;
}


//---------------------------------------------------------------------------
//     @function:
//			CGroupExpression::PccRemove
//
//     @doc:
//			Remove cost context in hash table;
//
//---------------------------------------------------------------------------
CCostContext *
CGroupExpression::PccRemove
	(
	COptimizationContext *poc,
	ULONG ulOptReq
	)
{
	GPOS_ASSERT(NULL != poc);
	ShtAcc shta(Sht(), poc);
	CCostContext *pccFound = shta.PtLookup();
	while (NULL != pccFound)
	{
		if (ulOptReq == pccFound->UlOptReq())
		{
			shta.Remove(pccFound);
			return pccFound;
		}

		pccFound = shta.PtNext(pccFound);
	}

	return NULL;
}


//---------------------------------------------------------------------------
//     @function:
//			CGroupExpression::PccInsertBest
//
//     @doc:
//			Insert given context in hash table only if a better context
//			does not already exist,
//			return the context that is kept in hash table
//
//---------------------------------------------------------------------------
CCostContext *
CGroupExpression::PccInsertBest
	(
	CCostContext *pcc
	)
{
	GPOS_ASSERT(NULL != pcc);

	COptimizationContext *poc = pcc->Poc();
	const ULONG ulOptReq = pcc->UlOptReq();

	// remove existing cost context, if any
	CCostContext *pccExisting = PccRemove(poc, ulOptReq);
	CCostContext *pccKept =  NULL;

	// compare existing context with given context
	if (NULL == pccExisting || pcc->FBetterThan(pccExisting))
	{
		// insert new context
		pccKept = PccInsert(pcc);
		GPOS_ASSERT(pccKept == pcc);

		if (NULL != pccExisting)
		{
			if (pccExisting == poc->PccBest())
			{
				// change best cost context of the corresponding optimization context
				poc->SetBest(pcc);
			}
			pccExisting->Release();
		}
	}
	else
	{
		// re-insert existing context
		pcc->Release();
		pccKept = PccInsert(pccExisting);
		GPOS_ASSERT(pccKept == pccExisting);
	}

	return pccKept;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::PccComputeCost
//
//	@doc:
//		Compute and store expression's cost under a given context;
//		the function returns the cost context containing the computed cost
//
//---------------------------------------------------------------------------
CCostContext *
CGroupExpression::PccComputeCost
	(
	IMemoryPool *pmp,
	COptimizationContext *poc,
	ULONG ulOptReq,
	DrgPoc *pdrgpoc, // array of child contexts
	BOOL fPruned,	// is created cost context pruned based on cost bound
	CCost costLowerBound	// lower bound on the cost of plan carried by cost context
	)
{
	GPOS_ASSERT(NULL != poc);
	GPOS_ASSERT_IMP(!fPruned, NULL != pdrgpoc);

	if (!fPruned && !FValidContext(pmp, poc, pdrgpoc))
	{
		return NULL;
	}
	
	// check if the same cost context is already created for current group expression
	if (FCostContextExists(poc, pdrgpoc))
	{
		return NULL;
	}

	poc->AddRef();
	this->AddRef();
	CCostContext *pcc = GPOS_NEW(pmp) CCostContext(pmp, poc, ulOptReq, this);
	BOOL fValid = true;

	// computing cost
	pcc->SetState(CCostContext::estCosting);

	if (!fPruned)
	{
		if (NULL != pdrgpoc)
		{
			pdrgpoc->AddRef();
		}
		pcc->SetChildContexts(pdrgpoc);

		fValid = pcc->FValid(pmp);
		if (fValid)
		{
			CCost cost = CostCompute(pmp, pcc);
			pcc->SetCost(cost);
		}
		GPOS_ASSERT_IMP(COptCtxt::FAllEnforcersEnabled(), fValid &&
				"Cost context carries an invalid plan");
	}
	else
	{
		pcc->SetPruned();
		pcc->SetCost(costLowerBound);
	}

	pcc->SetState(CCostContext::estCosted);
	if (fValid)
	{
		return PccInsertBest(pcc);
	}

	pcc->Release();

	// invalid cost context
	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::CostLowerBound
//
//	@doc:
//		Compute a lower bound on plans rooted by current group expression for
//		the given required properties
//
//---------------------------------------------------------------------------
CCost
CGroupExpression::CostLowerBound
	(
	IMemoryPool *pmp,
	CReqdPropPlan *prppInput,
	CCostContext *pccChild,
	ULONG ulChildIndex
	)
{
	GPOS_ASSERT(NULL != prppInput);
	GPOS_ASSERT(Pop()->FPhysical());

	prppInput->AddRef();
	if (NULL != pccChild)
	{
		pccChild->AddRef();
	}
	CPartialPlan *ppp = GPOS_NEW(pmp) CPartialPlan(this, prppInput, pccChild, ulChildIndex);
	CCost *pcostLowerBound = m_ppartialplancostmap->PtLookup(ppp);
	if (NULL != pcostLowerBound)
	{
		ppp->Release();
		return *pcostLowerBound;
	}

	// compute partial plan cost
	CCost cost = ppp->CostCompute(pmp);

#ifdef GPOS_DEBUG
	BOOL fSuccess =
#endif // GPOS_DEBUG
		m_ppartialplancostmap->FInsert(ppp, GPOS_NEW(pmp) CCost(cost.DVal()));
	GPOS_ASSERT(fSuccess);

	return cost;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::SetState
//
//	@doc:
//		Set group expression state;
//
//---------------------------------------------------------------------------
void
CGroupExpression::SetState
	(
	EState estNewState
	)
{
	GPOS_ASSERT(estNewState == (EState) (m_estate + 1));

	m_estate = estNewState;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::ResetState
//
//	@doc:
//		Reset group expression state;
//
//---------------------------------------------------------------------------
void
CGroupExpression::ResetState()
{
	m_estate = estUnexplored;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::CostCompute
//
//	@doc:
//		Costing scheme.
//
//---------------------------------------------------------------------------
CCost
CGroupExpression::CostCompute
	(
	IMemoryPool *pmp,
	CCostContext *pcc
	)
	const
{
	GPOS_ASSERT(NULL != pcc);

	// prepare cost array
	DrgPoc *pdrgpoc = pcc->Pdrgpoc();
	DrgPcost *pdrgpcostChildren = GPOS_NEW(pmp) DrgPcost(pmp);
	const ULONG ulLen = pdrgpoc->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		COptimizationContext *pocChild = (*pdrgpoc)[ul];
		pdrgpcostChildren->Append(GPOS_NEW(pmp) CCost(pocChild->PccBest()->Cost()));
	}

	CCost cost = pcc->CostCompute(pmp, pdrgpcostChildren);
	pdrgpcostChildren->Release();

	return cost;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::FTransitioned
//
//	@doc:
//		Check if transition to the given state is completed;
//
//---------------------------------------------------------------------------
BOOL
CGroupExpression::FTransitioned
	(
	EState estate
	)
	const
{
	GPOS_ASSERT(estate == estExplored || estate == estImplemented);

	return  !Pop()->FLogical() ||
			(estate == estExplored && FExplored()) ||
			(estate == estImplemented && FImplemented());
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::PccLookup
//
//	@doc:
//		Lookup cost context in hash table;
//
//---------------------------------------------------------------------------
CCostContext *
CGroupExpression::PccLookup
	(
	COptimizationContext *poc,
	ULONG ulOptReq
	)
{
	GPOS_ASSERT(NULL != poc);

	ShtAcc shta(Sht(), poc);
	CCostContext *pccFound = shta.PtLookup();
	while (NULL != pccFound)
	{
		if (ulOptReq == pccFound->UlOptReq())
		{
			return pccFound;
		}

		pccFound = shta.PtNext(pccFound);
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::PccLookupAll
//
//	@doc:
//		Lookup all valid cost contexts matching given optimization context
//
//---------------------------------------------------------------------------
DrgPcc *
CGroupExpression::PdrgpccLookupAll
	(
	IMemoryPool *pmp,
	COptimizationContext *poc
	)
{
	GPOS_ASSERT(NULL != poc);
	DrgPcc *pdrgpcc = GPOS_NEW(pmp) DrgPcc(pmp);

	CCostContext *pccFound = NULL;
	BOOL fValid = false;
	{
		ShtAcc shta(Sht(), poc);
		pccFound = shta.PtLookup();
		fValid = (NULL != pccFound && pccFound->Cost() != GPOPT_INVALID_COST && !pccFound->FPruned());
	}

	while (NULL != pccFound)
	{
		if (fValid)
		{
			pccFound->AddRef();
			pdrgpcc->Append(pccFound);
		}

		{
			ShtAcc shta(Sht(), poc);
			pccFound = shta.PtNext(pccFound);
			fValid = (NULL != pccFound && pccFound->Cost() != GPOPT_INVALID_COST && !pccFound->FPruned());
		}
	}

	return pdrgpcc;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::PccInsert
//
//	@doc:
//		Insert a cost context in hash table;
//
//---------------------------------------------------------------------------
CCostContext *
CGroupExpression::PccInsert
	(
	CCostContext *pcc
	)
{
	ShtAcc shta(Sht(), pcc->Poc());

	CCostContext *pccFound = shta.PtLookup();
	while (NULL != pccFound)
	{
		if (CCostContext::FEqual(*pcc, *pccFound))
		{
			return pccFound;
		}
		pccFound = shta.PtNext(pccFound);
	}
	GPOS_ASSERT(NULL == pccFound);

	shta.Insert(pcc);
	return pcc;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::PreprocessTransform
//
//	@doc:
//		Pre-processing before applying transformation
//
//---------------------------------------------------------------------------
void
CGroupExpression::PreprocessTransform
	(
	IMemoryPool *pmpLocal,
	IMemoryPool *pmpGlobal,
	CXform *pxform
	)
{
	if (CXformUtils::FDeriveStatsBeforeXform(pxform) && NULL == Pgroup()->Pstats())
	{
		GPOS_ASSERT(Pgroup()->FStatsDerivable(pmpGlobal));

		// derive stats on container group before applying xform
		CExpressionHandle exprhdl(pmpGlobal);
		exprhdl.Attach(this);
		exprhdl.DeriveStats(pmpLocal, pmpGlobal, NULL /*prprel*/, NULL /*pdrgpstatCtxt*/);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::PostprocessTransform
//
//	@doc:
//		Post-processing after applying transformation
//
//---------------------------------------------------------------------------
void
CGroupExpression::PostprocessTransform
	(
	IMemoryPool *, // pmpLocal
	IMemoryPool *, // pmpGlobal
	CXform *pxform
	)
{
	if (CXformUtils::FDeriveStatsBeforeXform(pxform))
	{
		(void) Pgroup()->FResetStats();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::Transform
//
//	@doc:
//		Transform group expression using the given xform
//
//---------------------------------------------------------------------------
void
CGroupExpression::Transform
	(
	IMemoryPool *pmp,
	IMemoryPool *pmpLocal,
	CXform *pxform,
	CXformResult *pxfres,
	ULONG *pulElapsedTime // output: elapsed time in millisecond
	)
{
	GPOS_ASSERT(NULL != pulElapsedTime);
	GPOS_CHECK_ABORT;

	BOOL fPrintOptStats = GPOS_FTRACE(EopttracePrintOptimizationStatistics);
	CTimerUser timer;
	if (fPrintOptStats)
	{
		timer.Restart();
	}

	*pulElapsedTime = 0;
	// check traceflag and compatibility with origin xform
	if (GPOPT_FDISABLED_XFORM(pxform->Exfid())|| !pxform->FCompatible(m_exfidOrigin))
	{
		if (fPrintOptStats)
		{
			*pulElapsedTime = timer.UlElapsedMS();
		}
		return;
	}

	// check xform promise
	CExpressionHandle exprhdl(pmp);
	exprhdl.Attach(this);
	exprhdl.DeriveProps(NULL /*pdpctxt*/);
	if (CXform::ExfpNone == pxform->Exfp(exprhdl))
	{
		if (GPOS_FTRACE(EopttracePrintOptimizationStatistics))
		{
			*pulElapsedTime = timer.UlElapsedMS();
		}
		return;
	}

	// pre-processing before applying xform to group expression
	PreprocessTransform(pmpLocal, pmp, pxform);

	// extract memo bindings to apply xform
	CBinding binding;
	CXformContext *pxfctxt = GPOS_NEW(pmp) CXformContext(pmp);

	CExpression *pexprPattern = pxform->PexprPattern();
	CExpression *pexpr = binding.PexprExtract(pmp, this, pexprPattern , NULL);
	while (NULL != pexpr)
	{
		ULONG ulNumResults = pxfres->Pdrgpexpr()->UlLength();
		pxform->Transform(pxfctxt, pxfres, pexpr);
		ulNumResults = pxfres->Pdrgpexpr()->UlLength() - ulNumResults;
		PrintXform(pmp, pxform, pexpr, pxfres, ulNumResults);

		if (CXformUtils::FApplyOnce(pxform->Exfid()) ||
			(0 < pxfres->Pdrgpexpr()->UlLength() &&
			!CXformUtils::FApplyToNextBinding(pxform, pexpr)))
		{
			// do not apply xform to other possible patterns
			pexpr->Release();
			break;
		}

		CExpression *pexprLast = pexpr;
		pexpr = binding.PexprExtract(pmp, this, pexprPattern, pexprLast);

		// release last extracted expression
		pexprLast->Release();

		GPOS_CHECK_ABORT;
	}
	pxfctxt->Release();

	// post-prcoessing before applying xform to group expression
	PostprocessTransform(pmpLocal, pmp, pxform);

	if (fPrintOptStats)
	{
		*pulElapsedTime = timer.UlElapsedMS();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::FMatchNonScalarChildren
//
//	@doc:
//		Match children of group expression against given children of
//		passed expression
//
//---------------------------------------------------------------------------
BOOL
CGroupExpression::FMatchNonScalarChildren
	(
	const CGroupExpression *pgexpr
	)
	const
{
	GPOS_ASSERT(NULL != pgexpr);

	if (0 == UlArity())
	{
		return (pgexpr->UlArity() == 0);
	}

	return CGroup::FMatchNonScalarGroups(m_pdrgpgroup, pgexpr->m_pdrgpgroup);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::FMatch
//
//	@doc:
//		Match group expression against given operator and its children
//
//---------------------------------------------------------------------------
BOOL
CGroupExpression::FMatch
	(
	const CGroupExpression *pgexpr
	)
	const
{
	GPOS_ASSERT(NULL != pgexpr);

	// make sure we are not comparing to invalid group expression
	if (NULL == this->Pop() || NULL == pgexpr->Pop())
	{
		return NULL == this->Pop() && NULL == pgexpr->Pop();
	}

	// have same arity
	if (UlArity() != pgexpr->UlArity())
	{
		return false;
	}

	// match operators
	if (!m_pop->FMatch(pgexpr->m_pop))
	{
		return false;
	}

	// compare inputs
	if (0 == UlArity())
	{
		return true;
	}
	else
	{
		if (1 == UlArity() || m_pop->FInputOrderSensitive())
		{
			return CGroup::FMatchGroups(m_pdrgpgroup, pgexpr->m_pdrgpgroup);
		}
		else
		{
			GPOS_ASSERT(NULL != m_pdrgpgroupSorted && NULL != pgexpr->m_pdrgpgroupSorted);

			return CGroup::FMatchGroups(m_pdrgpgroupSorted, pgexpr->m_pdrgpgroupSorted);
		}
	}
									
	GPOS_ASSERT(!"Unexpected exit from function");
	return false;
}
								

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::UlHash
//
//	@doc:
//		static hash function for operator and group references
//
//---------------------------------------------------------------------------
ULONG
CGroupExpression::UlHash
	(
	COperator *pop,
	DrgPgroup *pdrgpgroup
	)
{
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(NULL != pdrgpgroup);
	
	ULONG ulHash = pop->UlHash();
	
	ULONG ulArity = pdrgpgroup->UlLength();
	for (ULONG i = 0; i < ulArity; i++)
	{
		ulHash = UlCombineHashes(ulHash, (*pdrgpgroup)[i]->UlHash());
	}
	
	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::UlHash
//
//	@doc:
//		static hash function for group expressions
//
//---------------------------------------------------------------------------
ULONG
CGroupExpression::UlHash
	(
	const CGroupExpression &gexpr
	)
{
	return gexpr.UlHash();
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::PstatsRecursiveDerive
//
//	@doc:
//		Derive stats recursively on group expression
//
//---------------------------------------------------------------------------
IStatistics *
CGroupExpression::PstatsRecursiveDerive
	(
	IMemoryPool *, // pmpLocal
	IMemoryPool *pmpGlobal,
	CReqdPropRelational *prprel,
	DrgPstat *pdrgpstatCtxt,
	BOOL fComputeRootStats
	)
{
	GPOS_ASSERT(!Pgroup()->FScalar());
	GPOS_ASSERT(!Pgroup()->FImplemented());
	GPOS_ASSERT(NULL != pdrgpstatCtxt);
	GPOS_CHECK_ABORT;

	// trigger recursive property derivation
	CExpressionHandle exprhdl(pmpGlobal);
	exprhdl.Attach(this);
	exprhdl.DeriveProps(NULL /*pdpctxt*/);

	// compute required relational properties on child groups
	exprhdl.ComputeReqdProps(prprel, 0 /*ulOptReq*/);

	// trigger recursive stat derivation
	exprhdl.DeriveStats(pdrgpstatCtxt, fComputeRootStats);
	IStatistics *pstats = exprhdl.Pstats();
	if (NULL != pstats)
	{
		pstats->AddRef();
	}

	return pstats;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::PrintXform
//
//	@doc:
//		Print transformation
//
//---------------------------------------------------------------------------
void
CGroupExpression::PrintXform
	(
	IMemoryPool *pmp,
	CXform *pxform,
	CExpression *pexpr,
	CXformResult *pxfres,
	ULONG ulNumResults
	)
{
	if (NULL != pexpr && GPOS_FTRACE(EopttracePrintXform) && GPOS_FTRACE(EopttracePrintXformResults))
	{
		CAutoTrace at(pmp);
		IOstream &os(at.Os());

		os
			<< *pxform
			<< std::endl
			<< "Input:" << std::endl << *pexpr
			<< "Output:" << std::endl
			<< "Alternatives:" << std::endl;
		DrgPexpr *pdrgpexpr = pxfres->Pdrgpexpr();
		ULONG ulStart = pdrgpexpr->UlLength() - ulNumResults;
		ULONG ulEnd = pdrgpexpr->UlLength();

		for (ULONG i = ulStart; i < ulEnd; i++)
		{
			os << i-ulStart << ": " << std::endl;
			(*pdrgpexpr)[i]->OsPrint(os);
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::OsPrintCostContexts
//
//	@doc:
//		Print group expression cost contexts
//
//---------------------------------------------------------------------------
IOstream &
CGroupExpression::OsPrintCostContexts
	(
	IOstream &os,
	const CHAR *szPrefix
	)
{
	if (Pop()->FPhysical() && GPOS_FTRACE(EopttracePrintOptimizationContext))
	{
		// print cost contexts
		os << szPrefix << szPrefix << "Cost Ctxts:" << std::endl;
		CCostContext *pcc = NULL;
		ShtIter shtit(this->Sht());
		while (shtit.FAdvance())
		{
			{
				ShtAccIter shtitacc(shtit);
				pcc = shtitacc.Pt();
			}

			if (NULL != pcc)
			{
				os << szPrefix << szPrefix << szPrefix;
				(void) pcc->OsPrint(os);
			}
		}
	}

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CGroupExpression::OsPrint
	(
	IOstream &os,
	const CHAR *szPrefix
	)
{
	os << szPrefix << m_ulId << ": ";
	(void) m_pop->OsPrint(os);

	if (EolHigh == m_eol)
	{
		os << " (High)";
	}
	os << " [ ";
	
	ULONG ulArity = UlArity();
	for (ULONG i = 0; i < ulArity; i++)
	{
		os << (*m_pdrgpgroup)[i]->UlId() << " ";
	}
	os << "]";

	if (NULL != m_pgexprDuplicate)
	{
		os
			<< " Dup. of GrpExpr " << m_pgexprDuplicate->UlId()
			<< " in Grp " << m_pgexprDuplicate->Pgroup()->UlId();
	}

	if (GPOS_FTRACE(EopttracePrintXform) && ExfidOrigin() != CXform::ExfInvalid)
	{
		os << " Origin: ";
		if (m_fIntermediate)
		{
			os << "intermediate result of ";
		}
		os << "(xform: " << CXformFactory::Pxff()->Pxf(ExfidOrigin())->SzId();
		os << ", Grp: " << m_pgexprOrigin->Pgroup()->UlId() << ", GrpExpr: " << m_pgexprOrigin->UlId() << ")";
	}
	os << std::endl;

	(void) OsPrintCostContexts(os, szPrefix);

	return os;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::DbgPrint
//
//	@doc:
//		Print driving function for use in interactive debugging;
//		always prints to stderr;
//
//---------------------------------------------------------------------------
void
CGroupExpression::DbgPrint()
{
	CAutoTraceFlag atf(EopttracePrintGroupProperties, true);
	CAutoTrace at(m_pmp);
	(void) this->OsPrint(at.Os());
}
#endif // GPOS_DEBUG

// EOF
