//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2011 EMC Corp.
//
//	@filename:
//		CEngine.cpp
//
//	@doc:
//		Implementation of optimization engine
//---------------------------------------------------------------------------
#include "gpos/base.h"
#include "gpos/common/CAutoTimer.h"
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"
#include "gpos/task/CAutoTaskProxy.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/exception.h"

#include "gpopt/base/CDrvdPropCtxtPlan.h"
#include "gpopt/base/CCostContext.h"
#include "gpopt/base/COptimizationContext.h"
#include "gpopt/base/CReqdPropPlan.h"
#include "gpopt/base/CReqdPropRelational.h"
#include "gpopt/base/CQueryContext.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/minidump/CSerializableStackTrace.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogical.h"
#include "gpopt/operators/CPattern.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalMotionGather.h"
#include "gpopt/operators/CPhysicalAgg.h"
#include "gpopt/operators/CPhysicalSort.h"
#include "gpopt/optimizer/COptimizerConfig.h"

#include "gpopt/search/CGroup.h"
#include "gpopt/search/CGroupExpression.h"
#include "gpopt/search/CGroupProxy.h"
#include "gpopt/search/CJob.h"
#include "gpopt/search/CJobFactory.h"
#include "gpopt/search/CMemo.h"
#include "gpopt/search/CScheduler.h"
#include "gpopt/search/CSchedulerContext.h"
#include "gpopt/xforms/CXformFactory.h"

#include "naucrates/traceflags/traceflags.h"


#define GPOPT_SAMPLING_MAX_ITERS 30
#define GPOPT_JOBS_CAP 5000  // maximum number of initial optimization jobs
#define GPOPT_JOBS_PER_GROUP 20 // estimated number of needed optimization jobs per memo group

// memory consumption unit in bytes -- currently MB
#define GPOPT_MEM_UNIT (1024 * 1024)
#define GPOPT_MEM_UNIT_NAME "MB"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CEngine::CEngine
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CEngine::CEngine
	(
	IMemoryPool *pmp
	)
	:
	m_pmp(pmp),
	m_pqc(NULL),
	m_pdrgpss(NULL),
	m_ulCurrSearchStage(0),
	m_pmemo(NULL),
	m_pexprEnforcerPattern(NULL),
	m_pxfs(NULL),
	m_pdrgpulpXformCalls(NULL),
	m_pdrgpulpXformTimes(NULL)
{
	m_pmemo = GPOS_NEW(pmp) CMemo(pmp);
	m_pexprEnforcerPattern = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp));
	m_pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	m_pdrgpulpXformCalls = GPOS_NEW(pmp) DrgPulp(pmp);
	m_pdrgpulpXformTimes = GPOS_NEW(pmp) DrgPulp(pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::~CEngine
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CEngine::~CEngine()
{
#ifdef GPOS_DEBUG
	// in optimized build, we flush-down memory pools without leak checking,
	// we can save time in optimized build by skipping all de-allocations here,
	// we still have all de-llocations enabled in debug-build to detect any possible leaks
	GPOS_DELETE(m_pmemo);
	CRefCount::SafeRelease(m_pxfs);
	m_pdrgpulpXformCalls->Release();
	m_pdrgpulpXformTimes->Release();
	m_pexprEnforcerPattern->Release();
	CRefCount::SafeRelease(m_pdrgpss);
#endif // GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::InitLogicalExpression
//
//	@doc:
//		Initialize engine with a given expression
//
//---------------------------------------------------------------------------
void
CEngine::InitLogicalExpression
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL == m_pmemo->PgroupRoot() && "Root is already set");
	GPOS_ASSERT(pexpr->Pop()->FLogical());

	CGroup *pgroupRoot = PgroupInsert(NULL /*pgroupTarget*/, pexpr, CXform::ExfInvalid, NULL /*pgexprOrigin*/, false /*fIntermediate*/);
	m_pmemo->SetRoot(pgroupRoot);
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::Init
//
//	@doc:
//		Initialize engine using a given query context
//
//---------------------------------------------------------------------------
void 
CEngine::Init
	(
	CQueryContext *pqc,
	DrgPss *pdrgpss
	)
{
	GPOS_ASSERT(NULL == m_pqc);
	GPOS_ASSERT(NULL != pqc);
	GPOS_ASSERT_IMP
		(
		0 == CDrvdPropRelational::Pdprel(pqc->Pexpr()->PdpDerive())->PcrsOutput()->CElements(),
		0 == pqc->Prpp()->PcrsRequired()->CElements() &&
		"requiring columns from a zero column expression"
		);

	m_pdrgpss = pdrgpss;
	if (NULL == pdrgpss)
	{
		m_pdrgpss = CSearchStage::PdrgpssDefault(m_pmp);
	}
	GPOS_ASSERT(0 < m_pdrgpss->UlLength());

	if (GPOS_FTRACE(EopttracePrintOptimizationStatistics))
	{
		// initialize per-stage xform calls array
		const ULONG ulStages = m_pdrgpss->UlLength();
		for (ULONG ul = 0; ul < ulStages; ul++)
		{
			ULONG_PTR *pulpXformCalls = GPOS_NEW_ARRAY(m_pmp, ULONG_PTR, CXform::ExfSentinel);
			ULONG_PTR *pulpXformTimes = GPOS_NEW_ARRAY(m_pmp, ULONG_PTR, CXform::ExfSentinel);
			for (ULONG ulXform = 0; ulXform < CXform::ExfSentinel; ulXform++)
			{
				pulpXformCalls[ulXform] = 0;
				pulpXformTimes[ulXform] = 0;
			}
			m_pdrgpulpXformCalls->Append(pulpXformCalls);
			m_pdrgpulpXformTimes->Append(pulpXformTimes);
		}
	}

	m_pqc = pqc;
	InitLogicalExpression(m_pqc->Pexpr());

	m_pqc->PdrgpcrSystemCols()->AddRef();
	COptCtxt::PoctxtFromTLS()->SetReqdSystemCols(m_pqc->PdrgpcrSystemCols());
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::AddEnforcers
//
//	@doc:
//		Add enforcers to a memo group
//
//---------------------------------------------------------------------------
void
CEngine::AddEnforcers
	(
	CGroupExpression *pgexpr, // belongs to group where we need to add enforcers
	DrgPexpr *pdrgpexprEnforcers
	)
{
	GPOS_ASSERT(NULL != pdrgpexprEnforcers);
	GPOS_ASSERT(NULL != pgexpr);

	for (ULONG ul = 0; ul < pdrgpexprEnforcers->UlLength(); ul++)
	{
		// assemble an expression rooted by the enforcer operator
		CExpression *pexprEnforcer = (*pdrgpexprEnforcers)[ul];
#ifdef GPOS_DEBUG
		CGroup * pgroup =
#endif // GPOS_DEBUG
			PgroupInsert(pgexpr->Pgroup(), pexprEnforcer, CXform::ExfInvalid, NULL /*pgexprOrigin*/, false /*fIntermediate*/);
		GPOS_ASSERT(pgroup == pgexpr->Pgroup());
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::InsertExpressionChildren
//
//	@doc:
//		Insert children of the given expression to memo, and copy the groups
//		they end up at to the given group array
//
//---------------------------------------------------------------------------
void
CEngine::InsertExpressionChildren
	(
	CExpression *pexpr,
	DrgPgroup *pdrgpgroupChildren,
	CXform::EXformId exfidOrigin,
	CGroupExpression *pgexprOrigin
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pdrgpgroupChildren);

	ULONG ulArity = pexpr->UlArity();

	for (ULONG i = 0; i < ulArity; i++)
	{
		CGroup *pgroupChild = NULL;
		COperator *popChild = (*pexpr)[i]->Pop();
		if (popChild->FPattern() && CPattern::PopConvert(popChild)->FLeaf())
		{
			GPOS_ASSERT(NULL != (*pexpr)[i]->Pgexpr()->Pgroup());

			// group is already assigned during binding extraction;
			pgroupChild = (*pexpr)[i]->Pgexpr()->Pgroup();
		}
		else
		{
			// insert child expression recursively
			pgroupChild = PgroupInsert(NULL /*pgroupTarget*/, (*pexpr)[i], exfidOrigin, pgexprOrigin, true /*fIntermediate*/);
		}
		pdrgpgroupChildren->Append(pgroupChild);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::PgroupInsert
//
//	@doc:
//		Insert an expression tree into the memo, with explicit target group;
//		the function returns a pointer to the group that contains the given
//		group expression
//
//---------------------------------------------------------------------------
CGroup *
CEngine::PgroupInsert
	(
	CGroup *pgroupTarget,
	CExpression *pexpr,
	CXform::EXformId exfidOrigin,
	CGroupExpression *pgexprOrigin,
	BOOL fIntermediate
	)
{
	// recursive function - check stack
	GPOS_CHECK_STACK_SIZE;
	GPOS_CHECK_ABORT;
	GPOS_ASSERT_IMP(CXform::ExfInvalid != exfidOrigin, NULL != pgexprOrigin);

	CGroup *pgroupOrigin = NULL;
	
	// check if expression was produced by extracting
	// a binding from the memo
	if (NULL != pexpr->Pgexpr())
	{
		pgroupOrigin = pexpr->Pgexpr()->Pgroup();
		GPOS_ASSERT(NULL != pgroupOrigin && NULL == pgroupTarget &&
					"A valid group is expected");

		// if parent has group pointer, all children must have group pointers;
		// terminate recursive insertion here
		return pgroupOrigin;
	}
	
	// if we have a valid origin group, target group must be NULL
	GPOS_ASSERT_IMP(NULL != pgroupOrigin, NULL == pgroupTarget);

	// insert expression's children to memo by recursive call
	DrgPgroup *pdrgpgroupChildren = GPOS_NEW(m_pmp) DrgPgroup(m_pmp, pexpr->UlArity());
	InsertExpressionChildren(pexpr, pdrgpgroupChildren, exfidOrigin, pgexprOrigin);

	COperator *pop = pexpr->Pop();
	pop->AddRef();
	CGroupExpression *pgexpr =
		GPOS_NEW(m_pmp) CGroupExpression
					(
					m_pmp,
					pop,
					pdrgpgroupChildren,
					exfidOrigin,
					pgexprOrigin,
					fIntermediate
					);

	// find the group that contains created group expression
	CGroup *pgroupContainer = m_pmemo->PgroupInsert(pgroupTarget, pexpr, pgexpr);

	if (NULL == pgexpr->Pgroup())
	{
		// insertion failed, release created group expression
		pgexpr->Release();
	}

	return pgroupContainer;
}	


//---------------------------------------------------------------------------
//	@function:
//		CEngine::InsertXformResult
//
//	@doc:
//		Insert a set of transformation results to memo
//
//---------------------------------------------------------------------------
void
CEngine::InsertXformResult
	(
	CGroup *pgroupOrigin,
	CXformResult *pxfres,
	CXform::EXformId exfidOrigin,
	CGroupExpression *pgexprOrigin,
	ULONG ulXformTime // time consumed by transformation in msec
	)
{
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(NULL != pgroupOrigin);
	GPOS_ASSERT(CXform::ExfInvalid != exfidOrigin);
	GPOS_ASSERT(NULL != pgexprOrigin);

	if (GPOS_FTRACE(EopttracePrintOptimizationStatistics) && 0 < pxfres->Pdrgpexpr()->UlLength())
	{
		(void) m_pxfs->FExchangeSet(exfidOrigin);
		(void) UlpExchangeAdd(&(*m_pdrgpulpXformCalls)[m_ulCurrSearchStage][exfidOrigin], 1);

		{
			CAutoMutex am(m_mutexOptStats);
			am.Lock();
			(*m_pdrgpulpXformTimes)[m_ulCurrSearchStage][exfidOrigin] += ulXformTime;
		}
	}

	CExpression *pexpr = pxfres->PexprNext();
	while (NULL != pexpr)
	{
		CGroup *pgroupContainer = PgroupInsert(pgroupOrigin, pexpr, exfidOrigin, pgexprOrigin, false /*fIntermediate*/);
		if (pgroupContainer != pgroupOrigin && FPossibleDuplicateGroups(pgroupContainer, pgroupOrigin))
		{
			m_pmemo->MarkDuplicates(pgroupOrigin, pgroupContainer);
		}

		pexpr = pxfres->PexprNext();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FPossibleDuplicateGroups
//
//	@doc:
//		Check whether the given memo groups can be marked as duplicates. This is
//		true only if they have the same logical properties
//
//---------------------------------------------------------------------------
BOOL
CEngine::FPossibleDuplicateGroups
	(
	CGroup *pgroupFst,
	CGroup *pgroupSnd
	)
{
	GPOS_ASSERT(NULL != pgroupFst);
	GPOS_ASSERT(NULL != pgroupSnd);

	CDrvdPropRelational *pdprelFst = CDrvdPropRelational::Pdprel(pgroupFst->Pdp());
	CDrvdPropRelational *pdprelSnd = CDrvdPropRelational::Pdprel(pgroupSnd->Pdp());

	// right now we only check the output columns, but we may possibly need to
	// check other properties as well
	return pdprelFst->PcrsOutput()->FEqual(pdprelSnd->PcrsOutput());
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::DeriveStats
//
//	@doc:
//		Derive statistics on the root group
//
//---------------------------------------------------------------------------
void
CEngine::DeriveStats
	(
	IMemoryPool *pmpLocal
	)
{
	CWStringDynamic str(m_pmp);
	COstreamString oss (&str);
	oss << "\n[OPT]: Statistics Derivation Time (stage " << m_ulCurrSearchStage <<") ";
	CHAR *sz = CUtils::SzFromWsz(m_pmp, const_cast<WCHAR *>(str.Wsz()));

	{
		CAutoTimer at(sz, GPOS_FTRACE(EopttracePrintOptimizationStatistics));
		// derive stats on root group
		CEngine::DeriveStats(pmpLocal, m_pmp, PgroupRoot(), NULL /*prprel*/);
	}

	GPOS_DELETE_ARRAY(sz);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::DeriveStats
//
//	@doc:
//		Derive statistics on the group
//
//---------------------------------------------------------------------------
void
CEngine::DeriveStats
	(
	IMemoryPool *pmpLocal,
	IMemoryPool *pmpGlobal,
	CGroup *pgroup,
	CReqdPropRelational *prprel
	)
{
	CGroupExpression *pgexprFirst = CEngine::PgexprFirst(pgroup);
	CExpressionHandle exprhdl(pmpGlobal);
	exprhdl.Attach(pgexprFirst);
	exprhdl.DeriveStats(pmpLocal, pmpGlobal, prprel, NULL /*pdrgpstatCtxt*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::PgexprFirst
//
//	@doc:
//		Return the first group expression in a given group
//
//---------------------------------------------------------------------------
CGroupExpression *
CEngine::PgexprFirst
	(
	CGroup *pgroup
	)
{
	CGroupExpression *pgexprFirst = NULL;
	{
		// group proxy scope
		CGroupProxy gp(pgroup);
		pgexprFirst = gp.PgexprFirst();
	}
	GPOS_ASSERT(NULL != pgexprFirst);

	return pgexprFirst;
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::EolDamp
//
//	@doc:
//		Damp optimization level
//
//---------------------------------------------------------------------------
EOptimizationLevel
CEngine::EolDamp
	(
	EOptimizationLevel eol
	)
{
	if (EolHigh == eol)
	{
		return EolLow;
	}

	return EolSentinel;
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::FOptimizeChild
//
//	@doc:
//		Check if parent group expression needs to optimize child group expression.
//		This method is called right before a group optimization job is about to
//		schedule a group expression optimization job.
//
//		Relation properties as well the optimizing parent group expression is
//		available to make the decision. So, operators can reject being optimized
//		under specific parent operators. For example, a GatherMerge under a Sort
//		can be prevented here since it destroys the order from a GatherMerge.
//---------------------------------------------------------------------------
BOOL
CEngine::FOptimizeChild
	(
	CGroupExpression *pgexprParent,
	CGroupExpression *pgexprChild,
	COptimizationContext *pocChild,
	EOptimizationLevel eolCurrent // current optimization level in child group
	)
{
	GPOS_ASSERT(NULL != PgroupRoot());
	GPOS_ASSERT(PgroupRoot()->FImplemented());
	GPOS_ASSERT(NULL != pgexprChild);
	GPOS_ASSERT_IMP(NULL == pgexprParent, pgexprChild->Pgroup() == PgroupRoot());

	if (pgexprParent == pgexprChild)
	{
		// a group expression cannot optimize itself
		return false;
	}

	if (pgexprChild->Eol() != eolCurrent)
	{
		// child group expression does not match current optimization level
		return false;
	}

	COperator *popChild = pgexprChild->Pop();

	if (NULL != pgexprParent &&
		COperator::EopPhysicalSort == pgexprParent->Pop()->Eopid() &&
		COperator::EopPhysicalMotionGather == popChild->Eopid())
	{
		// prevent (Sort --> GatherMerge), since Sort destroys order maintained by GatherMerge
		return !CPhysicalMotionGather::PopConvert(popChild)->FOrderPreserving();
	}


	return COptimizationContext::FOptimize(m_pmp, pgexprParent, pgexprChild, pocChild, UlSearchStages());
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FSafeToPruneWithDPEStats
//
//	@doc:
//		Determine if a plan rooted by given group expression can be safely
//		pruned during optimization when stats for Dynamic Partition Elimination
//		are derived
//
//---------------------------------------------------------------------------
BOOL
CEngine::FSafeToPruneWithDPEStats
	(
	CGroupExpression *pgexpr,
	CReqdPropPlan *, // prpp
	CCostContext *pccChild,
	ULONG  ulChildIndex
	)
{
	GPOS_ASSERT(GPOS_FTRACE(EopttraceDeriveStatsForDPE));
	GPOS_ASSERT(GPOS_FTRACE(EopttraceEnableSpacePruning));

	if (NULL == pccChild)
	{
		// group expression has not been optimized yet

		CDrvdPropRelational *pdprel = CDrvdPropRelational::Pdprel(pgexpr->Pgroup()->Pdp());
		if (0 < pdprel->Ppartinfo()->UlConsumers())
		{
			// we cannot bound cost here because of possible DPE that can happen below the operator
			return false;
		}

		return true;
	}

	// first child has been optimized
	CExpressionHandle exprhdl(m_pmp);
	exprhdl.Attach(pgexpr);
	ULONG ulNextChild = exprhdl.UlNextOptimizedChildIndex(ulChildIndex);
	CDrvdPropRelational *pdprelChild = CDrvdPropRelational::Pdprel((*pgexpr)[ulNextChild]->Pdp());
	if (0 < pdprelChild->Ppartinfo()->UlConsumers())
	{
		// we cannot bound cost here because of possible DPE that can happen for the unoptimized child
		return false;
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::FSafeToPrune
//
//	@doc:
//		Determine if a plan rooted by given group expression can be safely
//		pruned during optimization
//
//---------------------------------------------------------------------------
BOOL
CEngine::FSafeToPrune
	(
	CGroupExpression *pgexpr,
	CReqdPropPlan *prpp,
	CCostContext *pccChild,
	ULONG ulChildIndex,
	CCost *pcostLowerBound // output: a lower bound on plan's cost
	)
{
	GPOS_ASSERT(NULL != pcostLowerBound);
	*pcostLowerBound  = GPOPT_INVALID_COST;

	if (!GPOS_FTRACE(EopttraceEnableSpacePruning))
	{
		// space pruning is disabled
		return false;
	}

	if (GPOS_FTRACE(EopttraceDeriveStatsForDPE) &&
		!FSafeToPruneWithDPEStats(pgexpr, prpp, pccChild, ulChildIndex))
	{
		// stat derivation for Dynamic Partition Elimination may not allow non-trivial cost bounds

		return false;
	}

	// check if container group has a plan for given properties
	CGroup *pgroup = pgexpr->Pgroup();
	COptimizationContext *pocGroup = pgroup->PocLookupBest(m_pmp, UlSearchStages(), prpp);
	if (NULL != pocGroup && NULL != pocGroup->PccBest())
	{
		// compute a cost lower bound for the equivalent plan rooted by given group expression
		CCost costLowerBound = pgexpr->CostLowerBound(m_pmp, prpp, pccChild, ulChildIndex);
		*pcostLowerBound = costLowerBound;
		if (costLowerBound > pocGroup->PccBest()->Cost())
		{
			// group expression cannot deliver a better plan for given properties and can be safely pruned
			return true;
		}
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::Pmemotmap
//
//	@doc:
//		Build tree map on memo
//
//---------------------------------------------------------------------------
MemoTreeMap *
CEngine::Pmemotmap()
{
	COptimizerConfig *poconf = COptCtxt::PoctxtFromTLS()->Poconf();

	if (NULL == m_pmemo->Pmemotmap())
	{
		m_pqc->Prpp()->AddRef();
		COptimizationContext *poc = GPOS_NEW(m_pmp) COptimizationContext
						(
						m_pmp,
						PgroupRoot(),
						m_pqc->Prpp(),
						GPOS_NEW(m_pmp) CReqdPropRelational(GPOS_NEW(m_pmp) CColRefSet(m_pmp)), // pass empty required relational properties initially
						GPOS_NEW(m_pmp) DrgPstat(m_pmp), // pass empty stats context initially
						0 // ulSearchStageIndex
						);

		m_pmemo->BuildTreeMap(poc);
		poconf->Pec()->SetPlanSpaceSize(m_pmemo->Pmemotmap()->UllCount());

		poc->Release();
	}

	return m_pmemo->Pmemotmap();
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CEngine::ApplyTransformations
//
//	@doc:
//		Applies given set of xforms to group expression and insert
//		results to memo
//
//---------------------------------------------------------------------------
void
CEngine::ApplyTransformations
	(
	IMemoryPool *pmpLocal,
	CXformSet *pxfs,
	CGroupExpression *pgexpr
	)
{
	// iterate over xforms
	CXformSetIter xsi(*pxfs);
	while (xsi.FAdvance())
	{
		GPOS_CHECK_ABORT;
		CXform *pxform = CXformFactory::Pxff()->Pxf(xsi.TBit());

		// transform group expression, and insert results to memo
		CXformResult *pxfres = GPOS_NEW(m_pmp) CXformResult(m_pmp);
		ULONG ulElapsedTime = 0;
		pgexpr->Transform(m_pmp, pmpLocal, pxform, pxfres, &ulElapsedTime);
		InsertXformResult(pgexpr->Pgroup(), pxfres, pxform->Exfid(), pgexpr, ulElapsedTime);
		pxfres->Release();

		if (PssCurrent()->FTimedOut())
		{
			break;
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::TransitionGroupExpression
//
//	@doc:
//		Transition group expression to a given target state
//
//---------------------------------------------------------------------------
void
CEngine::TransitionGroupExpression
	(
	IMemoryPool *pmpLocal,
	CGroupExpression *pgexpr,
	CGroupExpression::EState estTarget
	)
{
	GPOS_ASSERT(CGroupExpression::estExplored == estTarget ||
				CGroupExpression::estImplemented == estTarget);

	if (PssCurrent()->FTimedOut())
	{
		return;
	}

	CGroupExpression::EState estInitial = CGroupExpression::estExploring;
	CGroup::EState estGroupTargetState = CGroup::estExplored;
	if (CGroupExpression::estImplemented == estTarget)
	{
		estInitial = CGroupExpression::estImplementing;
		estGroupTargetState = CGroup::estImplemented;
	}

	pgexpr->SetState(estInitial);

	// transition all child groups
	ULONG ulArity = pgexpr->UlArity();
	for (ULONG i = 0; i < ulArity; i++)
	{
		TransitionGroup(pmpLocal, (*pgexpr)[i], estGroupTargetState);

		GPOS_CHECK_ABORT;
	}

	// find which set of xforms should be used
	CXformSet *pxfs = CXformFactory::Pxff()->PxfsExploration();
	if (CGroupExpression::estImplemented == estTarget)
	{
		pxfs = CXformFactory::Pxff()->PxfsImplementation();
	}

	// get all applicable xforms
	COperator *pop = pgexpr->Pop();
	CXformSet *pxfsCandidates = CLogical::PopConvert(pop)->PxfsCandidates(m_pmp);

	// intersect them with the required set of xforms, then apply transformations
	pxfsCandidates->Intersection(pxfs);
	pxfsCandidates->Intersection(PxfsCurrentStage());
	ApplyTransformations(pmpLocal, pxfsCandidates, pgexpr);
	pxfsCandidates->Release();

	pgexpr->SetState(estTarget);
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::TransitionGroup
//
//	@doc:
//		Transition group to a target state
//
//---------------------------------------------------------------------------
void
CEngine::TransitionGroup
	(
	IMemoryPool *pmpLocal,
	CGroup *pgroup,
	CGroup::EState estTarget
	)
{
	// check stack size
	GPOS_CHECK_STACK_SIZE;

	if (PssCurrent()->FTimedOut())
	{
		return;
	}

	GPOS_ASSERT(CGroup::estExplored == estTarget ||
				CGroup::estImplemented == estTarget);

	BOOL fTransitioned = false;
	{
		CGroupProxy gp(pgroup);
		fTransitioned = gp.FTransitioned(estTarget);
	}

	// check if we can end recursion early
	if (!fTransitioned)
	{

		CGroup::EState estInitial = CGroup::estExploring;
		CGroupExpression::EState estGExprTargetState = CGroupExpression::estExplored;
		if (CGroup::estImplemented == estTarget)
		{
			estInitial = CGroup::estImplementing;
			estGExprTargetState = CGroupExpression::estImplemented;
		}

		CGroupExpression *pgexprCurrent = NULL;

		// transition group's state to initial state
		{
			CGroupProxy gp(pgroup);
			gp.SetState(estInitial);
		}

		// get first group expression
		{
			CGroupProxy gp(pgroup);
			pgexprCurrent = gp.PgexprFirst();
		}

		while (NULL != pgexprCurrent)
		{
			if (!pgexprCurrent->FTransitioned(estGExprTargetState))
			{
				TransitionGroupExpression
					(
					pmpLocal,
					pgexprCurrent,
					estGExprTargetState
					);
			}

			if (PssCurrent()->FTimedOut())
			{
				break;
			}

			// get next group expression
			{
				CGroupProxy gp(pgroup);
				pgexprCurrent = gp.PgexprNext(pgexprCurrent);
			}

			GPOS_CHECK_ABORT;
		}

		// transition group to target state
		{
			CGroupProxy gp(pgroup);
			gp.SetState(estTarget);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::PocChild
//
//	@doc:
//		Create optimization context for child group
//
//---------------------------------------------------------------------------
COptimizationContext *
CEngine::PocChild
	(
	CGroupExpression *pgexpr, // parent expression
	COptimizationContext *pocOrigin, // optimization context of parent operator
	CExpressionHandle &exprhdlPlan, // handle to compute required plan properties
	CExpressionHandle &exprhdlRel, // handle to compute required relational properties
	DrgPdp *pdrgpdpChildren, // derived plan properties of optimized children
	DrgPstat *pdrgpstatCurrentCtxt,
	ULONG ulChildIndex,
	ULONG ulOptReq
	)
{
	GPOS_ASSERT(exprhdlPlan.Pgexpr() == pgexpr);
	GPOS_ASSERT(NULL != pocOrigin);
	GPOS_ASSERT(NULL != pdrgpdpChildren);
	GPOS_ASSERT(NULL != pdrgpstatCurrentCtxt);

	CGroup *pgroupChild = (*pgexpr)[ulChildIndex];

	// compute required properties of the n-th child
	exprhdlPlan.ComputeChildReqdProps(ulChildIndex, pdrgpdpChildren, ulOptReq);
	exprhdlPlan.Prpp(ulChildIndex)->AddRef();

	// use current stats for optimizing current child
	DrgPstat *pdrgpstatCtxt = GPOS_NEW(m_pmp) DrgPstat(m_pmp);
	CUtils::AddRefAppend<IStatistics, CleanupStats>(pdrgpstatCtxt, pdrgpstatCurrentCtxt);

	// compute required relational properties
	CReqdPropRelational *prprel = NULL;
	if (CPhysical::PopConvert(pgexpr->Pop())->FPassThruStats())
	{
		// copy requirements from origin context
		prprel = pocOrigin->Prprel();
	}
	else
	{
		// retrieve requirements from handle
		prprel = exprhdlRel.Prprel(ulChildIndex);
	}
	GPOS_ASSERT(NULL != prprel);
	prprel->AddRef();

	COptimizationContext *pocChild =
			GPOS_NEW(m_pmp) COptimizationContext
				(
				m_pmp,
				pgroupChild,
				exprhdlPlan.Prpp(ulChildIndex),
				prprel,
				pdrgpstatCtxt,
				m_ulCurrSearchStage
				);

	return pocChild;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::PccOptimizeChild
//
//	@doc:
//		Optimize child group and return best cost context satisfying
//		required properties
//
//---------------------------------------------------------------------------
CCostContext *
CEngine::PccOptimizeChild
	(
	CExpressionHandle &exprhdl, // initialized with required properties
	CExpressionHandle &exprhdlRel,
	COptimizationContext *pocOrigin, // optimization context of parent operator
	DrgPdp *pdrgpdp,
	DrgPstat *pdrgpstatCurrentCtxt,
	ULONG ulChildIndex,
	ULONG ulOptReq
	)
{
	CGroupExpression *pgexpr = exprhdl.Pgexpr();
	CGroup *pgroupChild = (*exprhdl.Pgexpr())[ulChildIndex];

	// create optimization context for child group
	COptimizationContext *pocChild =
			PocChild(pgexpr, pocOrigin, exprhdl, exprhdlRel, pdrgpdp, pdrgpstatCurrentCtxt, ulChildIndex, ulOptReq);

	if (pgroupChild == pgexpr->Pgroup() && pocChild->FMatch(pocOrigin))
	{
		// child context is the same as origin context, this is a deadlock
		pocChild->Release();
		return NULL;
	}

	// optimize child group
	CGroupExpression *pgexprChildBest = PgexprOptimize(pgroupChild, pocChild, pgexpr);
	pocChild->Release();
	if (NULL == pgexprChildBest || PssCurrent()->FTimedOut())
	{
		// failed to generate a plan for the child, or search stage is timed-out
		return NULL;
	}

	// derive plan properties of child group optimal implementation
	COptimizationContext *pocFound = pgroupChild->PocLookupBest(m_pmp, m_pdrgpss->UlLength(), exprhdl.Prpp(ulChildIndex));
	GPOS_ASSERT(NULL != pocFound);

	CCostContext *pccChildBest = pocFound->PccBest();
	GPOS_ASSERT(NULL != pccChildBest);

	// check if optimization can be early terminated after first child has been optimized
	CCost costLowerBound(GPOPT_INVALID_COST);
	if (exprhdl.UlFirstOptimizedChildIndex() == ulChildIndex &&
		FSafeToPrune(pgexpr, pocOrigin->Prpp(), pccChildBest, ulChildIndex, &costLowerBound))
	{
		// failed to optimize child due to cost bounding
		(void) pgexpr->PccComputeCost(m_pmp, pocOrigin, ulOptReq, NULL /*pdrgpoc*/, true /*fPruned*/, costLowerBound);
		return NULL;
	}

	return pccChildBest;
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::PdrgpocOptimizeChildren
//
//	@doc:
//		Optimize child groups of a given group expression;
//
//---------------------------------------------------------------------------
DrgPoc *
CEngine::PdrgpocOptimizeChildren
	(
	CExpressionHandle &exprhdl, // initialized with required properties
	COptimizationContext *pocOrigin, // optimization context of parent operator
	ULONG ulOptReq
	)
{
	GPOS_ASSERT(NULL != exprhdl.Pgexpr());

	CGroupExpression *pgexpr = exprhdl.Pgexpr();
	const ULONG ulArity = exprhdl.UlArity();
	if (0 == ulArity)
	{
		// return empty array if no children
		return GPOS_NEW(m_pmp) DrgPoc(m_pmp);
	}

	// create array of child derived properties
	DrgPdp *pdrgpdp = GPOS_NEW(m_pmp) DrgPdp(m_pmp);

	// initialize current stats context with input stats context
	DrgPstat *pdrgpstatCurrentCtxt = GPOS_NEW(m_pmp) DrgPstat(m_pmp);
	CUtils::AddRefAppend<IStatistics, CleanupStats>(pdrgpstatCurrentCtxt, pocOrigin->Pdrgpstat());

	// initialize required relational properties computation
	CExpressionHandle exprhdlRel(m_pmp);
	CGroupExpression *pgexprForStats = pgexpr->Pgroup()->PgexprBestPromise(m_pmp, pgexpr);
	if (NULL != pgexprForStats)
	{
		exprhdlRel.Attach(pgexprForStats);
		exprhdlRel.DeriveProps(NULL /*pdpctxt*/);
		exprhdlRel.ComputeReqdProps(pocOrigin->Prprel(), 0 /*ulOptReq*/);
	}

	// iterate over child groups and optimize them
	BOOL fSuccess = true;
	ULONG ulChildIndex = exprhdl.UlFirstOptimizedChildIndex();
	do
	{
		CGroup *pgroupChild = (*exprhdl.Pgexpr())[ulChildIndex];
		if (pgroupChild->FScalar())
		{
			// skip scalar groups from optimization
			continue;
		}

		CCostContext *pccChildBest = PccOptimizeChild(exprhdl, exprhdlRel, pocOrigin, pdrgpdp, pdrgpstatCurrentCtxt, ulChildIndex, ulOptReq);
		if (NULL == pccChildBest)
		{
			fSuccess = false;
			break;
		}

		CExpressionHandle exprhdlChild(m_pmp);
		exprhdlChild.Attach(pccChildBest);
		exprhdlChild.DerivePlanProps();
		exprhdlChild.Pdp()->AddRef();
		pdrgpdp->Append(exprhdlChild.Pdp());

		// copy stats of child's best cost context to current stats context
		IStatistics *pstat = pccChildBest->Pstats();
		pstat->AddRef();
		pdrgpstatCurrentCtxt->Append(pstat);

		GPOS_CHECK_ABORT;
	}
	while (exprhdl.FNextChildIndex(&ulChildIndex));
	pdrgpdp->Release();
	pdrgpstatCurrentCtxt->Release();

	if (!fSuccess)
	{
		return NULL;
	}

	// return child optimization contexts array
	return PdrgpocChildren(m_pmp, exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::OptimizeGroupExpression
//
//	@doc:
//		Optimize group expression under a given context
//
//---------------------------------------------------------------------------
void
CEngine::OptimizeGroupExpression
	(
	CGroupExpression *pgexpr,
	COptimizationContext *poc
	)
{
	CGroup *pgroup = pgexpr->Pgroup();
	const ULONG ulOptRequests = CPhysical::PopConvert(pgexpr->Pop())->UlOptRequests();
	for (ULONG ul = 0; ul < ulOptRequests; ul++)
	{
		CExpressionHandle exprhdl(m_pmp);
		exprhdl.Attach(pgexpr);
		exprhdl.DeriveProps(NULL /*pdpctxt*/);

		// check if group expression optimization can be early terminated without optimizing any child
		CCost costLowerBound(GPOPT_INVALID_COST);
		if (FSafeToPrune(pgexpr, poc->Prpp(), NULL /*pccChild*/,
						 gpos::ulong_max /*ulChildIndex*/, &costLowerBound))
		{
			(void) pgexpr->PccComputeCost(m_pmp, poc, ul, NULL /*pdrgpoc*/, true /*fPruned*/, costLowerBound);
			continue;
		}

		if (FCheckReqdProps(exprhdl, poc->Prpp(), ul))
		{
			// load required properties on the handle
			exprhdl.InitReqdProps(poc->Prpp());

			// optimize child groups
			DrgPoc *pdrgpoc = PdrgpocOptimizeChildren(exprhdl, poc, ul);

			if (NULL != pdrgpoc && FCheckEnfdProps(m_pmp, pgexpr, poc, ul, pdrgpoc))
			{
				// compute group expression cost under the current optimization context
				CCostContext *pccComputed = pgexpr->PccComputeCost(m_pmp, poc, ul, pdrgpoc, false /*fPruned*/, CCost(0.0));

				if (NULL != pccComputed)
				{
					// update best group expression under the current optimization context
					pgroup->UpdateBestCost(poc, pccComputed);
				}
			}

			CRefCount::SafeRelease(pdrgpoc);
		}

		GPOS_CHECK_ABORT;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::PgexprOptimize
//
//	@doc:
//		Optimize group under a given context;
//
//---------------------------------------------------------------------------
CGroupExpression *
CEngine::PgexprOptimize
	(
	CGroup *pgroup,
	COptimizationContext *poc,
	CGroupExpression *pgexprOrigin
	)
{
	// recursive function - check stack
	GPOS_CHECK_STACK_SIZE;

	COptimizationContext *pocFound = pgroup->PocInsert(poc);
	if (poc != pocFound)
	{

		GPOS_ASSERT(COptimizationContext::estOptimized == pocFound->Est());
		return pocFound->PgexprBest();
	}

	// add-ref context to pin it in hash table
	poc->AddRef();
	poc->SetState(COptimizationContext::estOptimizing);

	EOptimizationLevel eolCurrent = pgroup->EolMax();
	while (EolSentinel != eolCurrent)
	{
		CGroupExpression *pgexprCurrent = NULL;
		{
			CGroupProxy gp(pgroup);
			pgexprCurrent = gp.PgexprSkipLogical(NULL /*pgexpr*/);
		}

		while (NULL != pgexprCurrent)
		{
			if (FOptimizeChild(pgexprOrigin, pgexprCurrent, poc, eolCurrent))
			{
				OptimizeGroupExpression(pgexprCurrent, poc);
			}

			if (PssCurrent()->FTimedOut())
			{
				break;
			}

			// move to next group expression
			{
				CGroupProxy gp(pgroup);
				pgexprCurrent = gp.PgexprSkipLogical(pgexprCurrent);
			}
		}

		// damp optimization level
		eolCurrent = EolDamp(eolCurrent);

		GPOS_CHECK_ABORT;
	}
	poc->SetState(COptimizationContext::estOptimized);

	return poc->PgexprBest();
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::Explore
//
//	@doc:
//		Apply all exploration xforms
//
//---------------------------------------------------------------------------
void
CEngine::Explore()
{
	GPOS_ASSERT(NULL != m_pqc);
	GPOS_ASSERT(NULL != PgroupRoot());

	// explore root group
	GPOS_ASSERT(!PgroupRoot()->FExplored());

	TransitionGroup(m_pmp, PgroupRoot(), CGroup::estExplored /*estTarget*/);
	GPOS_ASSERT_IMP
		(
		!PssCurrent()->FTimedOut(),
		PgroupRoot()->FExplored()
		);
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::Implement
//
//	@doc:
//		Apply all implementation xforms
//
//---------------------------------------------------------------------------
void
CEngine::Implement()
{
	GPOS_ASSERT(NULL != m_pqc);
	GPOS_ASSERT(NULL != PgroupRoot());

	// implement root group
	GPOS_ASSERT(!PgroupRoot()->FImplemented());

	TransitionGroup(m_pmp, PgroupRoot(), CGroup::estImplemented /*estTarget*/);
	GPOS_ASSERT_IMP
		(
		!PssCurrent()->FTimedOut(),
		PgroupRoot()->FImplemented()
		);
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::RecursiveOptimize
//
//	@doc:
//		Recursive optimization
//
//---------------------------------------------------------------------------
void
CEngine::RecursiveOptimize()
{
	COptimizerConfig *poconf = COptCtxt::PoctxtFromTLS()->Poconf();

	CAutoTimer at("\n[OPT]: Total Optimization Time", GPOS_FTRACE(EopttracePrintOptimizationStatistics));

	const ULONG ulSearchStages = m_pdrgpss->UlLength();
	for (ULONG ul = 0; !FSearchTerminated() && ul < ulSearchStages; ul++)
	{
		PssCurrent()->RestartTimer();

		// apply exploration xforms
		Explore();

		// run exploration completion operations
		FinalizeExploration();

		// apply implementation xforms
		Implement();

		// run implementation completion operations
		FinalizeImplementation();

		// optimize root group
		m_pqc->Prpp()->AddRef();
		COptimizationContext *poc =
			GPOS_NEW(m_pmp) COptimizationContext
				(
				m_pmp,
				PgroupRoot(),
				m_pqc->Prpp(),
				GPOS_NEW(m_pmp) CReqdPropRelational(GPOS_NEW(m_pmp) CColRefSet(m_pmp)), // pass empty required relational properties initially
				GPOS_NEW(m_pmp) DrgPstat(m_pmp), // pass an empty stats context initially
				m_ulCurrSearchStage
				);
		(void) PgexprOptimize(PgroupRoot(), poc, NULL /*pgexprOrigin*/);
		poc->Release();

		// extract best plan found at the end of current search stage
		CExpression *pexprPlan =
			m_pmemo->PexprExtractPlan
								(
								m_pmp,
								m_pmemo->PgroupRoot(),
								m_pqc->Prpp(),
								m_pdrgpss->UlLength()
								);
		PssCurrent()->SetBestExpr(pexprPlan);

		FinalizeSearchStage();
	}

	{
		CAutoTrace atSearch(m_pmp);
		atSearch.Os() << "[OPT]: Search terminated at stage " << m_ulCurrSearchStage << "/" << m_pdrgpss->UlLength();
	}

	if (poconf->Pec()->FSample())
	{
		SamplePlans();
	}
}

#endif // GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CEngine::PdrgpocChildren
//
//	@doc:
//		Return array of child optimization contexts corresponding
//		to handle requirements
//
//---------------------------------------------------------------------------
DrgPoc *
CEngine::PdrgpocChildren
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
{
	GPOS_ASSERT(NULL != exprhdl.Pgexpr());

	DrgPoc *pdrgpoc = GPOS_NEW(pmp) DrgPoc(pmp);
	const ULONG ulArity = exprhdl.UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CGroup *pgroupChild = (*exprhdl.Pgexpr())[ul];
		if (!pgroupChild->FScalar())
		{
			COptimizationContext *poc =
				pgroupChild->PocLookupBest(pmp, m_pdrgpss->UlLength(), exprhdl.Prpp(ul));
			GPOS_ASSERT(NULL != poc);

			poc->AddRef();
			pdrgpoc->Append(poc);
		}
	}

	return pdrgpoc;
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::ScheduleMainJob
//
//	@doc:
//		Create and schedule the main optimization job
//
//---------------------------------------------------------------------------
void
CEngine::ScheduleMainJob
	(
	CSchedulerContext *psc,
	COptimizationContext *poc
	)
{
	GPOS_ASSERT(NULL != PgroupRoot());

	CJobGroupOptimization::ScheduleJob(psc, PgroupRoot(), NULL /*pgexprOrigin*/, poc, NULL /*pjParent*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::FinalizeExploration
//
//	@doc:
//		Execute operations after exploration completes
//
//---------------------------------------------------------------------------
void
CEngine::FinalizeExploration()
{
	GroupMerge();

	if (m_pqc->FDeriveStats())
	{
		// derive statistics
		m_pmemo->ResetStats();
		DeriveStats(m_pmp);
	}

	if (!GPOS_FTRACE(EopttraceDonotDeriveStatsForAllGroups))
	{
		// derive stats for every group without stats
		m_pmemo->DeriveStatsIfAbsent(m_pmp);
	}

	if (GPOS_FTRACE(EopttracePrintMemoAfterExploration))
	{
		{
			CAutoTrace at(m_pmp);
			at.Os() << "MEMO after exploration (stage " << m_ulCurrSearchStage << ")" << std::endl;
		}

		{
			CAutoTrace at(m_pmp);
			at.Os() << *this;
		}
	}

	if (GPOS_FTRACE(EopttracePrintOptimizationStatistics))
	{
		CAutoTrace at(m_pmp);
		(void) OsPrintMemoryConsumption(at.Os(), "Memory consumption after exploration ");
	}

}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::FinalizeImplementation
//
//	@doc:
//		Execute operations after implementation completes
//
//---------------------------------------------------------------------------
void
CEngine::FinalizeImplementation()
{
	if (GPOS_FTRACE(EopttracePrintMemoAfterImplementation))
	{
		{
			CAutoTrace at(m_pmp);
			at.Os() << "MEMO after implementation (stage " << m_ulCurrSearchStage << ")" << std::endl;
		}

		{
			CAutoTrace at(m_pmp);
			at.Os() << *this;
		}
	}

	if (GPOS_FTRACE(EopttracePrintOptimizationStatistics))
	{
		CAutoTrace at(m_pmp);
		(void) OsPrintMemoryConsumption(at.Os(), "Memory consumption after implementation ");
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FinalizeSearchStage
//
//	@doc:
//		Execute operations after search stage completes
//
//---------------------------------------------------------------------------
void
CEngine::FinalizeSearchStage()
{
	ProcessTraceFlags();

	m_pxfs->Release();
	m_pxfs = NULL;
	m_pxfs = GPOS_NEW(m_pmp) CXformSet(m_pmp);

	m_ulCurrSearchStage++;
	m_pmemo->ResetGroupStates();
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::PrintActivatedXforms
//
//	@doc:
//		Print activated xform
//
//---------------------------------------------------------------------------
void
CEngine::PrintActivatedXforms
	(
	IOstream &os
	)
	const
{
	if (GPOS_FTRACE(EopttracePrintOptimizationStatistics))
	{
		os << std::endl << "[OPT]: <Begin Xforms - stage " << m_ulCurrSearchStage << ">" << std::endl;
		CXformSetIter xsi(*m_pxfs);
		while (xsi.FAdvance())
		{
			CXform *pxform = CXformFactory::Pxff()->Pxf(xsi.TBit());
			ULONG ulCalls = (ULONG) (*m_pdrgpulpXformCalls)[m_ulCurrSearchStage][pxform->Exfid()];
			ULONG ulTime = (ULONG) (*m_pdrgpulpXformTimes)[m_ulCurrSearchStage][pxform->Exfid()];
			os
				<< pxform->SzId() << ": "
				<< ulCalls << " calls, "
				<< ulTime << "ms"<< std::endl;
		}
		os << "[OPT]: <End Xforms - stage " << m_ulCurrSearchStage << ">" << std::endl;
	}
}



//---------------------------------------------------------------------------
//	@function:
//		CEngine::PrintMemoryConsumption
//
//	@doc:
//		Print current memory consumption
//
//---------------------------------------------------------------------------
IOstream &
CEngine::OsPrintMemoryConsumption
	(
	IOstream &os,
	const CHAR *szHeader
	)
	const
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	CMDAccessor::MDCache *pcache = pmda->Pcache();

	os << std::endl << szHeader
		<<  "Engine: [" << (DOUBLE) m_pmp->UllTotalAllocatedSize() / GPOPT_MEM_UNIT << "] " << GPOPT_MEM_UNIT_NAME
		<< ", MD Cache: [" << (DOUBLE) (pcache->UllTotalAllocatedSize()) / GPOPT_MEM_UNIT << "] " << GPOPT_MEM_UNIT_NAME
		<< ", Total: [" << (DOUBLE) (CMemoryPoolManager::Pmpm()->UllTotalAllocatedSize()) / GPOPT_MEM_UNIT << "] " << GPOPT_MEM_UNIT_NAME;

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::ProcessTraceFlags
//
//	@doc:
//		Process trace flags after optimization is complete
//
//---------------------------------------------------------------------------
void
CEngine::ProcessTraceFlags()
{
	if (GPOS_FTRACE(EopttracePrintMemoAfterOptimization))
	{
		{
			CAutoTrace at(m_pmp);
			at.Os() << "MEMO after optimization (stage "<< m_ulCurrSearchStage << "):" << std::endl;
		}

		{
			CAutoTrace at(m_pmp);
			at.Os() << *this;
		}
	}

	if (GPOS_FTRACE(EopttracePrintOptimizationStatistics))
	{
		CAutoTrace at(m_pmp);

		// print optimization stats
		at.Os()
			<< std::endl << "[OPT]: Memo (stage "<< m_ulCurrSearchStage << "): ["
			<< (ULONG) (m_pmemo->UlpGroups()) << " groups"
			<< ", " << m_pmemo->UlDuplicateGroups() << " duplicate groups"
			<< ", " << m_pmemo->UlGrpExprs() << " group expressions"
			<< ", " << m_pxfs->CElements() << " activated xforms]";

		at.Os()
			<< std::endl << "[OPT]: stage "<< m_ulCurrSearchStage << " completed in "
			<< PssCurrent()->UlElapsedTime() << " msec, ";
			if (NULL == PssCurrent()->PexprBest())
			{
				at.Os() << " no plan was found";
			}
			else
			{
				at.Os() << " plan with cost "<< PssCurrent()->CostBest() <<" was found";
			}

		PrintActivatedXforms(at.Os());

		(void) OsPrintMemoryConsumption(at.Os(), "Memory consumption after optimization ");
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::Optimize
//
//	@doc:
//		Main driver of optimization engine
//
//---------------------------------------------------------------------------
void
CEngine::Optimize()
{
	COptimizerConfig *poconf = COptCtxt::PoctxtFromTLS()->Poconf();

	CAutoTimer at("\n[OPT]: Total Optimization Time", GPOS_FTRACE(EopttracePrintOptimizationStatistics));

	if (GPOS_FTRACE(EopttraceParallel))
	{
		MultiThreadedOptimize(2 /*ulWorkers*/);
	}
	else
	{
		MainThreadOptimize();
	}

	{
		if (GPOS_FTRACE(EopttracePrintOptimizationStatistics))
		{
			CAutoTrace atSearch(m_pmp);
			atSearch.Os() << "[OPT]: Search terminated at stage " << m_ulCurrSearchStage << "/" << m_pdrgpss->UlLength();
		}
	}

	if (poconf->Pec()->FSample())
	{
		SamplePlans();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::MainThreadOptimize
//
//	@doc:
//		Run optimizer on the main thread
//
//---------------------------------------------------------------------------
void
CEngine::MainThreadOptimize()
{
	GPOS_ASSERT(NULL != PgroupRoot());
	GPOS_ASSERT(NULL != COptCtxt::PoctxtFromTLS());

	const ULONG ulJobs = std::min((ULONG) GPOPT_JOBS_CAP, (ULONG) (m_pmemo->UlpGroups() * GPOPT_JOBS_PER_GROUP));
	CJobFactory jf(m_pmp, ulJobs);
	CScheduler sched(m_pmp, ulJobs, 1 /*ulWorkers*/);

	CSchedulerContext sc;
	sc.Init(m_pmp, &jf, &sched, this);

	const ULONG ulSearchStages = m_pdrgpss->UlLength();
	for (ULONG ul = 0; !FSearchTerminated() && ul < ulSearchStages; ul++)
	{
		PssCurrent()->RestartTimer();

		// optimize root group
		m_pqc->Prpp()->AddRef();
		COptimizationContext *poc = GPOS_NEW(m_pmp) COptimizationContext
							(
							m_pmp,
							PgroupRoot(),
							m_pqc->Prpp(),
							GPOS_NEW(m_pmp) CReqdPropRelational(GPOS_NEW(m_pmp) CColRefSet(m_pmp)), // pass empty required relational properties initially
							GPOS_NEW(m_pmp) DrgPstat(m_pmp), // pass empty stats context initially
							m_ulCurrSearchStage
							);

		// schedule main optimization job
		ScheduleMainJob(&sc, poc);

		// run optimization job
		CScheduler::Run(&sc);

		poc->Release();

		// extract best plan found at the end of current search stage
		CExpression *pexprPlan =
			m_pmemo->PexprExtractPlan
								(
								m_pmp,
								m_pmemo->PgroupRoot(),
								m_pqc->Prpp(),
								m_pdrgpss->UlLength()
								);
		PssCurrent()->SetBestExpr(pexprPlan);

		FinalizeSearchStage();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::MultiThreadedOptimize
//
//	@doc:
//		Multi-threaded optimization
//
//---------------------------------------------------------------------------
void
CEngine::MultiThreadedOptimize
	(
	ULONG  ulWorkers
	)
{
	GPOS_ASSERT(NULL != PgroupRoot());
	GPOS_ASSERT(NULL != COptCtxt::PoctxtFromTLS());

	const ULONG ulJobs = std::min((ULONG) GPOPT_JOBS_CAP, (ULONG) (m_pmemo->UlpGroups() * GPOPT_JOBS_PER_GROUP));
	CJobFactory jf(m_pmp, ulJobs);
	CScheduler sched(m_pmp, ulJobs, ulWorkers);

	CSchedulerContext sc;
	sc.Init(m_pmp, &jf, &sched, this);

	const ULONG ulSearchStages = m_pdrgpss->UlLength();
	for (ULONG ul = 0; !FSearchTerminated() && ul < ulSearchStages; ul++)
	{
		PssCurrent()->RestartTimer();

		// optimize root group
		m_pqc->Prpp()->AddRef();
		COptimizationContext *poc = GPOS_NEW(m_pmp) COptimizationContext
								(
								m_pmp,
								PgroupRoot(),
								m_pqc->Prpp(),
								GPOS_NEW(m_pmp) CReqdPropRelational(GPOS_NEW(m_pmp) CColRefSet(m_pmp)), // pass empty required relational properties initially
								GPOS_NEW(m_pmp) DrgPstat(m_pmp), // pass empty stats context initially
								m_ulCurrSearchStage
								);

		// schedule main optimization job
		ScheduleMainJob(&sc, poc);

		// create task array
		CAutoRg<CTask*> a_rgptsk;
		a_rgptsk = GPOS_NEW_ARRAY(m_pmp, CTask*, ulWorkers);

		// create scheduling contexts
		CAutoRg<CSchedulerContext> a_rgsc;
		a_rgsc = GPOS_NEW_ARRAY(m_pmp, CSchedulerContext, ulWorkers);

		// scope for ATP
		{
			CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();
			CAutoTaskProxy atp(m_pmp, pwpm);

			for (ULONG i = 0; i < ulWorkers; i++)
			{
				// initialize scheduling context
				a_rgsc[i].Init(m_pmp, &jf, &sched, this);

				// create scheduling task
				a_rgptsk[i] = atp.PtskCreate(CScheduler::Run, &a_rgsc[i]);

				// store a pointer to optimizer's context in current task local storage
				a_rgptsk[i]->Tls().Reset(m_pmp);
				a_rgptsk[i]->Tls().Store(COptCtxt::PoctxtFromTLS());
			}

			// start tasks
			for (ULONG i = 0; i < ulWorkers; i++)
			{
				atp.Schedule(a_rgptsk[i]);
			}

			// wait for tasks to complete
			for (ULONG i = 0; i < ulWorkers; i++)
			{
				CTask *ptsk;
				atp.WaitAny(&ptsk);
			}
		}

		poc->Release();

		// extract best plan found at the end of current search stage
		CExpression *pexprPlan =
			m_pmemo->PexprExtractPlan
								(
								m_pmp,
								m_pmemo->PgroupRoot(),
								m_pqc->Prpp(),
								m_pdrgpss->UlLength()
								);
		PssCurrent()->SetBestExpr(pexprPlan);

		FinalizeSearchStage();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::CEngine
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CExpression *
CEngine::PexprUnrank
	(
	ULLONG ullPlanId
	)
{
	// The CTE map will be updated by the Producer instead of the Sequence operator
	// because we are doing a DFS traversal of the TreeMap.
	CDrvdPropCtxtPlan *pdpctxtplan = GPOS_NEW(m_pmp) CDrvdPropCtxtPlan(m_pmp, false /*fUpdateCTEMap*/);
	CExpression *pexpr = Pmemotmap()->PrUnrank(m_pmp, pdpctxtplan, ullPlanId);
	pdpctxtplan->Release();

#ifdef GPOS_DEBUG
	// check plan using configured plan checker, if any
	COptimizerConfig *poconf = COptCtxt::PoctxtFromTLS()->Poconf();
	CEnumeratorConfig *pec = poconf->Pec();
	BOOL fCheck = pec->FCheckPlan(pexpr);
	if (!fCheck)
	{
		CAutoTrace at(m_pmp);
		at.Os() << "\nextracted plan failed PlanChecker function: " << std::endl << *pexpr;
	}
	GPOS_ASSERT(fCheck);
#endif // GPOS_DEBUG

	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::PexprExtractPlan
//
//	@doc:
//		Extract a physical plan from the memo
//
//---------------------------------------------------------------------------
CExpression *
CEngine::PexprExtractPlan()
{
	GPOS_ASSERT(NULL != m_pqc);
	GPOS_ASSERT(NULL != m_pmemo);
	GPOS_ASSERT(NULL != m_pmemo->PgroupRoot());

	BOOL fGenerateAlt = false;
	COptimizerConfig *poconf = COptCtxt::PoctxtFromTLS()->Poconf();
	CEnumeratorConfig *pec = poconf->Pec();
	if (pec->FEnumerate())
	{
		CAutoTrace at(m_pmp);
		ULLONG ullCount = Pmemotmap()->UllCount();
		at.Os() << "[OPT]: Number of plan alternatives: " << ullCount << std::endl;

		if (0 < pec->UllPlanId())
		{

			if (pec->UllPlanId() > ullCount)
			{
				// an invalid plan number is chosen
				GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiInvalidPlanAlternative, pec->UllPlanId(), ullCount);
			}

			// a valid plan number was chosen
			fGenerateAlt = true;
		}
	}

	CExpression *pexpr = NULL;
	if (fGenerateAlt)
	{
		pexpr = PexprUnrank(pec->UllPlanId() - 1 /*rank of plan alternative is zero-based*/);
		CAutoTrace at(m_pmp);
		at.Os() << "[OPT]: Successfully generated plan: " << pec->UllPlanId() << std::endl;
	}
	else
	{
		pexpr = m_pmemo->PexprExtractPlan
						(
						m_pmp,
						m_pmemo->PgroupRoot(),
						m_pqc->Prpp(),
						m_pdrgpss->UlLength()
						);
	}

	if (NULL == pexpr)
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiNoPlanFound);
	}

	return pexpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::UllRandomPlanId
//
//	@doc:
//		Generate random plan id
//
//---------------------------------------------------------------------------
ULLONG
CEngine::UllRandomPlanId
	(
	ULONG *pulSeed
	)
{
	ULLONG ullCount = Pmemotmap()->UllCount();
	ULLONG ullPlanId = 0;
	do
	{
		ullPlanId = clib::UlRandR(pulSeed);
	}
	while (ullPlanId >= ullCount);

	return ullPlanId;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FValidPlanSample
//
//	@doc:
//		Extract a plan sample and handle exceptions according to enumerator
//		configurations
//
//---------------------------------------------------------------------------
BOOL
CEngine::FValidPlanSample
	(
	CEnumeratorConfig *pec,
	ULLONG ullPlanId,
	CExpression **ppexpr // output: extracted plan
	)
{
	GPOS_ASSERT(NULL != pec);
	GPOS_ASSERT(NULL != ppexpr);

	BOOL fValidPlan = true;
	if (pec->FSampleValidPlans())
	{
		// if enumerator is configured to extract valid plans only,
		// we extract plan and catch invalid plan exception here
		GPOS_TRY
		{
			*ppexpr = PexprUnrank(ullPlanId);
		}
		GPOS_CATCH_EX(ex)
		{
			if (GPOS_MATCH_EX(ex, gpopt::ExmaGPOPT, gpopt::ExmiUnsatisfiedRequiredProperties))
			{
				GPOS_RESET_EX;
				fValidPlan = false;
			}
			else
			{
				// for all other exceptions, we bail out
				GPOS_RETHROW(ex);
			}
		}
		GPOS_CATCH_END;
	}
	else
	{
		// otherwise, we extract plan and leave exception handling to the caller
		*ppexpr = PexprUnrank(ullPlanId);
	}

	return fValidPlan;
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::SamplePlans
//
//	@doc:
//		Sample distribution of possible plans uniformly;
//
//---------------------------------------------------------------------------
void
CEngine::SamplePlans()
{
	COptimizerConfig *poconf = COptCtxt::PoctxtFromTLS()->Poconf();
	GPOS_ASSERT(NULL != poconf);

	CEnumeratorConfig *pec = poconf->Pec();

	ULLONG ullSamples = pec->UllInputSamples();
	GPOS_ASSERT(0 < ullSamples);

	pec->ClearSamples();

	ULLONG ullCount = Pmemotmap()->UllCount();
	if (0 == ullCount)
	{
		// optimizer generated no plans
		return;
	}

	// generate full plan space when space size is less than or equal to
	// the required number of samples
	BOOL fGenerateAll = (ullSamples >= ullCount);

	ULLONG ullTargetSamples = ullSamples;
	if (fGenerateAll)
	{
		ullTargetSamples = ullCount;
	}

	// find cost of best plan
	CExpression *pexpr =
			m_pmemo->PexprExtractPlan
				(
				m_pmp,
				m_pmemo->PgroupRoot(),
				m_pqc->Prpp(),
				m_pdrgpss->UlLength()
				);
	CCost costBest = pexpr->Cost();
	pec->SetBestCost(costBest);
	pexpr->Release();

	// generate randomized seed using local time
	TIMEVAL tv;
	syslib::GetTimeOfDay(&tv, NULL/*timezone*/);
	ULONG ulSeed = UlCombineHashes((ULONG) tv.tv_sec, (ULONG)tv.tv_usec);

	// set maximum number of iterations based number of samples
	// we use maximum iteration to prevent infinite looping below
	const ULLONG ullMaxIters = ullTargetSamples * GPOPT_SAMPLING_MAX_ITERS;
	ULLONG ullIters = 0;
	ULLONG ull = 0;
	while (ullIters < ullMaxIters && ull < ullTargetSamples)
	{
		// generate id of plan to be extracted
		ULLONG ullPlanId = ull;
		if (!fGenerateAll)
		{
			ullPlanId = UllRandomPlanId(&ulSeed);
		}

		pexpr = NULL;
		BOOL fAccept = false;
		if (FValidPlanSample(pec, ullPlanId, &pexpr))
		{
			// add plan to the sample if it is below cost threshold
			CCost cost = pexpr->Cost();
			fAccept = pec->FAddSample(ullPlanId, cost);
			pexpr->Release();
		}

		if (fGenerateAll || fAccept)
		{
			ull++;
		}

		ullIters++;
	}

	pec->PrintPlanSample();
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::FCheckEnfdProps
//
//	@doc:
//		Check enforceable properties and append enforcers to the current group if
//		required.
//
//		This check is done in two steps:
//
//		First, it determines if any particular property needs to be enforced at
//		all. For example, the EopttraceDisableSort traceflag can disable order
//		enforcement. Also, if there are no partitioned tables referenced in the
//		subtree, partition propagation enforcement can be skipped.
//
//		Second, EPET methods are called for each property to determine if an
//		enforcer needs to be added. These methods in turn call into virtual
//		methods in the different operators. For example, CPhysical::EpetOrder()
//		is used to determine a Sort node needs to be added to the group. These
//		methods are passed an expression handle (to access derived properties of
//		the subtree) and the required properties as a object of a subclass of
//		CEnfdProp.
//
//		Finally, based on return values of the EPET methods,
//		CEnfdProp::AppendEnforcers() is called for each of the enforced
//		properties.
//
//		Returns true if no enforcers were created because they were deemed
//		unnecessary or optional i.e all enforced properties were satisfied for
//		the group expression under the current optimization context.  Returns
//		false otherwise.
//
//		NB: This method is only concerned with a certain enforcer needs to be
//		added into the group. Once added, there is no connection between the
//		enforcer and the operator that created it. That is although some group
//		expression X created the enforcer E, later, during costing, E can still
//		decide to pick some other group expression Y for its child, since
//		theoretically, all group expressions in a group are equivalent.
//
//---------------------------------------------------------------------------
BOOL
CEngine::FCheckEnfdProps
	(
	IMemoryPool *pmp,
	CGroupExpression *pgexpr,
	COptimizationContext *poc,
	ULONG ulOptReq,
	DrgPoc *pdrgpoc
	)
{
	GPOS_CHECK_ABORT;

	if (GPOS_FTRACE(EopttracePrintMemoEnforcement))
	{
		CAutoTrace at(m_pmp);
		at.Os() << "CEngine::FCheckEnfdProps (Group ID: " << pgexpr->Pgroup()->UlId() <<
				" Expression ID: " <<  pgexpr->UlId() << ")"<< std::endl;
		m_pmemo->OsPrint(at.Os());
	}

	// check if all children could be successfully optimized
	if (!FChildrenOptimized(pdrgpoc))
	{
		return false;
	}

	// load a handle with derived plan properties
	poc->AddRef();
	pgexpr->AddRef();
	pdrgpoc->AddRef();
	CCostContext *pcc= GPOS_NEW(pmp) CCostContext(pmp, poc, ulOptReq, pgexpr);
	pcc->SetChildContexts(pdrgpoc);
	CExpressionHandle exprhdl(pmp);
	exprhdl.Attach(pcc);
	exprhdl.DerivePlanProps();
	pcc->Release();

	CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
	CReqdPropPlan *prpp = poc->Prpp();

	// check whether the current physical operator satisfies the CTE requirements
	// and whether it is a motion over unresolved part consumers
	if (!FValidCTEAndPartitionProperties(pmp, exprhdl, prpp))
	{
		return false;
	}

	// Determine if any property enforcement is disable or unnecessary
	BOOL fOrderReqd =
		!GPOS_FTRACE(EopttraceDisableSort) &&
		!prpp->Peo()->PosRequired()->FEmpty();

	BOOL fDistributionReqd =
		!GPOS_FTRACE(EopttraceDisableMotions) &&
		(CDistributionSpec::EdtAny != prpp->Ped()->PdsRequired()->Edt());

	BOOL fRewindabilityReqd =
		!GPOS_FTRACE(EopttraceDisableSpool) &&
		(CRewindabilitySpec::ErtNone != prpp->Per()->PrsRequired()->Ert());

	BOOL fPartPropagationReqd =
		!GPOS_FTRACE(EopttraceDisablePartPropagation) &&
		prpp->Pepp()->PppsRequired()->FPartPropagationReqd();

	// Determine if adding an enforcer to the group is required, optional,
	// unnecessary or prohibited over the group expression and given the current
	// optimization context (required properties)

	// get order enforcing type
	CEnfdProp::EPropEnforcingType epetOrder =
			prpp->Peo()->Epet(exprhdl, popPhysical, fOrderReqd);

	// get distribution enforcing type
	CEnfdProp::EPropEnforcingType epetDistribution = prpp->Ped()->Epet(exprhdl, popPhysical, prpp->Pepp()->PppsRequired(), fDistributionReqd);

	// get rewindability enforcing type
	CEnfdProp::EPropEnforcingType epetRewindability =
			prpp->Per()->Epet(exprhdl, popPhysical, fRewindabilityReqd);

	// get partition propagation enforcing type
	CEnfdProp::EPropEnforcingType epetPartitionPropagation =
			prpp->Pepp()->Epet(exprhdl, popPhysical, fPartPropagationReqd);

	// Skip adding enforcers entirely if any property determines it to be
	// 'prohibited'. In this way, a property may veto out the creation of an
	// enforcer for the current group expression and optimization context.
	//
	// NB: Even though an enforcer E is not added because of some group
	// expression G because it was prohibited, some other group expression H may
	// decide to add it. And if E is added, it is possible for E to consider both
	// G and H as its child.
	if (FProhibited(epetOrder, epetDistribution, epetRewindability, epetPartitionPropagation))
	{
		return false;
	}

	DrgPexpr *pdrgpexprEnforcers = GPOS_NEW(pmp) DrgPexpr(pmp);

	// extract a leaf pattern from target group
	CBinding binding;
	CExpression *pexpr =
		binding.PexprExtract(m_pmp, exprhdl.Pgexpr(), m_pexprEnforcerPattern, NULL /* pexprLast */);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(pexpr->Pgexpr()->Pgroup() == pgexpr->Pgroup());
		
	prpp->Peo()->AppendEnforcers(pmp, prpp, pdrgpexprEnforcers, pexpr, epetOrder, exprhdl);
	prpp->Ped()->AppendEnforcers(pmp, prpp, pdrgpexprEnforcers, pexpr, epetDistribution, exprhdl);
	prpp->Per()->AppendEnforcers(pmp, prpp, pdrgpexprEnforcers, pexpr, epetRewindability, exprhdl);
	prpp->Pepp()->AppendEnforcers(pmp, prpp, pdrgpexprEnforcers, pexpr, epetPartitionPropagation, exprhdl);

	if (0 < pdrgpexprEnforcers->UlLength())
	{
		AddEnforcers(exprhdl.Pgexpr(), pdrgpexprEnforcers);
	}
	pdrgpexprEnforcers->Release();
	pexpr->Release();
	
	return FOptimize(epetOrder, epetDistribution, epetRewindability, epetPartitionPropagation);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FValidCTEAndPartitionProperties
//
//	@doc:
//		Check if the given expression has valid cte and partition properties
//		with respect to the given requirements. This function returns true iff
//		ALL the following conditions are met:
//		1. The expression satisfies the CTE requirements
//		2. The root of the expression is not a motion over an unresolved part consumer
//		3. The expression does not have an unneeded part propagator
//
//---------------------------------------------------------------------------
BOOL
CEngine::FValidCTEAndPartitionProperties
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CReqdPropPlan *prpp
	)
{
	CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
	CPartIndexMap *ppimDrvd = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Ppim();

	return popPhysical->FProvidesReqdCTEs(exprhdl, prpp->Pcter()) &&
			!CUtils::FMotionOverUnresolvedPartConsumers(pmp, exprhdl, prpp->Pepp()->PppsRequired()->Ppim()) &&
			!ppimDrvd->FContainsRedundantPartitionSelectors(prpp->Pepp()->PppsRequired()->Ppim());
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FChildrenOptimized
//
//	@doc:
//		Check if all children were successfully optimized
//
//---------------------------------------------------------------------------
BOOL
CEngine::FChildrenOptimized
	(
	DrgPoc *pdrgpoc
	)
{
	GPOS_ASSERT(NULL != pdrgpoc);

	const ULONG ulLen = pdrgpoc->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		if (NULL == (*pdrgpoc)[ul]->PgexprBest())
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FOptimize
//
//	@doc:
//		Check if optimization is possible under the given property enforcing 
//		types
//
//---------------------------------------------------------------------------
BOOL 
CEngine::FOptimize
	(
	CEnfdProp::EPropEnforcingType epetOrder, 
	CEnfdProp::EPropEnforcingType epetDistribution, 
	CEnfdProp::EPropEnforcingType epetRewindability, 
	CEnfdProp::EPropEnforcingType epetPropagation
	)
{
	return CEnfdProp::FOptimize(epetOrder) &&
	       CEnfdProp::FOptimize(epetDistribution) &&
	       CEnfdProp::FOptimize(epetRewindability) && 
	       CEnfdProp::FOptimize(epetPropagation);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FProhibited
//
//	@doc:
//		Check if any of the given property enforcing types prohibits enforcement
//
//---------------------------------------------------------------------------
BOOL 
CEngine::FProhibited
	(
	CEnfdProp::EPropEnforcingType epetOrder, 
	CEnfdProp::EPropEnforcingType epetDistribution, 
	CEnfdProp::EPropEnforcingType epetRewindability, 
	CEnfdProp::EPropEnforcingType epetPropagation
	)
{
	return (CEnfdProp::EpetProhibited == epetOrder ||
		    CEnfdProp::EpetProhibited == epetDistribution ||
		    CEnfdProp::EpetProhibited == epetRewindability ||
		    CEnfdProp::EpetProhibited == epetPropagation);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FCheckReqdPartPropagation
//
//	@doc:
//		Check if partition propagation resolver is passed an empty part propagation
// 		spec
//
//---------------------------------------------------------------------------
BOOL
CEngine::FCheckReqdPartPropagation
	(
	CPhysical *pop,
	CEnfdPartitionPropagation *pepp
	)
{
	BOOL fPartPropagationReqd = (NULL != pepp && pepp->PppsRequired()->Ppim()->FContainsUnresolvedZeroPropagators());
	
	return fPartPropagationReqd || COperator::EopPhysicalPartitionSelector != pop->Eopid();
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FCheckReqdProps
//
//	@doc:
//		Determine if checking required properties is needed.
//		This method is called after a group expression optimization job has
//		started executing and can be used to cancel the job early.
//
//		This is useful to prevent deadlocks when an enforcer optimizes same
//		group with the same optimization context. Also, in case the subtree
//		doesn't provide the required columns we can save optimization time by
//		skipping this optimization request.
//
//		NB: Only relational properties are available at this stage to make this
//		decision.
//---------------------------------------------------------------------------
BOOL
CEngine::FCheckReqdProps
	(
	CExpressionHandle &exprhdl,
	CReqdPropPlan *prpp,
	ULONG ulOptReq
	)
{
	GPOS_CHECK_ABORT;

	if (GPOS_FTRACE(EopttracePrintMemoEnforcement))
	{
		CAutoTrace at(m_pmp);
		at.Os() << "CEngine::FCheckReqdProps (Group ID: " << exprhdl.Pgexpr()->Pgroup()->UlId() <<
				" Expression ID: " <<  exprhdl.Pgexpr()->UlId() << ")" << std::endl;
		m_pmemo->OsPrint(at.Os());
	}

	// check if operator provides required columns
	if (!prpp->FProvidesReqdCols(m_pmp, exprhdl, ulOptReq))
	{
		return false;
	}

	CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
	COperator::EOperatorId eopid = popPhysical->Eopid();

	// check if sort operator is passed an empty order spec;
	// this check is required to avoid self-deadlocks, i.e.
	// sort optimizing same group with the same optimization context;
	BOOL fOrderReqd = !prpp->Peo()->PosRequired()->FEmpty();
	if (!fOrderReqd && COperator::EopPhysicalSort == eopid)
	{
		return false;
	}

	// check if motion operator is passed an ANY distribution spec;
	// this check is required to avoid self-deadlocks, i.e.
	// motion optimizing same group with the same optimization context;
	BOOL fDistributionReqd =
			(CDistributionSpec::EdtAny != prpp->Ped()->PdsRequired()->Edt());
	if (!fDistributionReqd && CUtils::FPhysicalMotion(popPhysical))
	{
		return false;
	}

	// check if spool operator is passed a non-rewindable spec;
	// this check is required to avoid self-deadlocks, i.e.
	// spool optimizing same group with the same optimization context;
	BOOL fRewindabilityReqd =
			(CRewindabilitySpec::ErtNone != prpp->Per()->PrsRequired()->Ert());
	if (!fRewindabilityReqd && COperator::EopPhysicalSpool == eopid)
	{
		return false;
	}
	
	return FCheckReqdPartPropagation(popPhysical, prpp->Pepp());
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CEngine::PrintRoot
//
//	@doc:
//		Print root group
//
//---------------------------------------------------------------------------
void
CEngine::PrintRoot()
{
	CAutoTrace at(m_pmp);
	at.Os() << "Root Group:" << std::endl;
	m_pmemo->Pgroup(m_pmemo->PgroupRoot()->UlId())->OsPrint(at.Os());
	at.Os() << std::endl;
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::PrintOptCtxts
//
//	@doc:
//		Print main optimization context and optimal cost context
//
//---------------------------------------------------------------------------
void
CEngine::PrintOptCtxts()
{
	CAutoTrace at(m_pmp);
	COptimizationContext *poc =
		m_pmemo->PgroupRoot()->PocLookupBest(m_pmp, m_pdrgpss->UlLength(), m_pqc->Prpp());
	GPOS_ASSERT(NULL != poc);

	at.Os() << std::endl << "Main Opt Ctxt:" << std::endl;
	(void) poc->OsPrint(at.Os(), " ");
	at.Os() << std::endl;
	at.Os() << "Optimal Cost Ctxt:" << std::endl;
	(void) poc->PccBest()->OsPrint(at.Os());
	at.Os() << std::endl;
}
#endif // GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CEngine::OsPrint
//
//	@doc:
//		print function
//
//---------------------------------------------------------------------------
IOstream &
CEngine::OsPrint
	(
	IOstream &os
	)
	const
{
	return m_pmemo->OsPrint(os);
}

// EOF

