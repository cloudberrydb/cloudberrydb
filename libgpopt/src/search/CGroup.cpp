//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 -2011 EMC Corp.
//
//	@filename:
//		CGroup.cpp
//
//	@doc:
//		Implementation of Memo groups; database agnostic
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/task/CAutoSuspendAbort.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/task/CWorker.h"

#include "gpopt/base/CDrvdProp.h"
#include "gpopt/base/CDrvdPropCtxtPlan.h"
#include "gpopt/base/CDrvdPropCtxtRelational.h"
#include "gpopt/base/COptimizationContext.h"
#include "gpopt/search/CGroup.h"
#include "gpopt/search/CGroupProxy.h"
#include "gpopt/search/CJobGroup.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalCTEProducer.h"
#include "gpopt/operators/CLogicalCTEConsumer.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CPhysicalMotionGather.h"

#include "gpopt/exception.h"

#include "naucrates/traceflags/traceflags.h"

using namespace gpnaucrates;
using namespace gpopt;

#define GPOPT_OPTCTXT_HT_BUCKETS	100


//---------------------------------------------------------------------------
//	@function:
//		CGroup::SContextLink::SContextLink
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CGroup::SContextLink::SContextLink
	(
	CCostContext *pccParent,
	ULONG ulChildIndex,
	COptimizationContext *poc
	)
	:
	m_pccParent(pccParent),
	m_ulChildIndex(ulChildIndex),
	m_poc(poc)
{}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::SContextLink::~SContextLink
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CGroup::SContextLink::~SContextLink()
{
	CRefCount::SafeRelease(m_pccParent);
	CRefCount::SafeRelease(m_poc);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::SContextLink::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CGroup::SContextLink::UlHash
	(
	const SContextLink *pclink
	)
{
	ULONG ulHashPcc = 0;
	if (NULL != pclink->m_pccParent)
	{
		ulHashPcc = CCostContext::UlHash(*pclink->m_pccParent);
	}

	ULONG ulHashPoc = 0;
	if (NULL != pclink->m_poc)
	{
		ulHashPoc = COptimizationContext::UlHash(*pclink->m_poc);
	}

	return UlCombineHashes
			(
			pclink->m_ulChildIndex,
			UlCombineHashes(ulHashPcc, ulHashPoc)
			);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::SContextLink::FEqual
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CGroup::SContextLink::FEqual
	(
	const SContextLink *pclink1,
	const SContextLink *pclink2
	)
{
	BOOL fEqualChildIndexes = (pclink1->m_ulChildIndex == pclink2->m_ulChildIndex);
	BOOL fEqual = false;
	if (fEqualChildIndexes)
	{
		if (NULL == pclink1->m_pccParent || NULL == pclink2->m_pccParent)
		{
			fEqual = (NULL == pclink1->m_pccParent && NULL == pclink2->m_pccParent);
		}
		else
		{
			fEqual = (*pclink1->m_pccParent == *pclink2->m_pccParent);
		}
	}

	if (fEqual)
	{
		if (NULL == pclink1->m_poc || NULL == pclink2->m_poc)
		{
			return  (NULL == pclink1->m_poc && NULL == pclink2->m_poc);
		}

		return COptimizationContext::FEqual(*pclink1->m_poc, *pclink2->m_poc);
	}

	return fEqual;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::CGroup
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CGroup::CGroup
	(
	IMemoryPool *pmp,
	BOOL fScalar
	)
	:
	m_pmp(pmp),
	m_ulId(GPOPT_INVALID_GROUP_ID),
	m_fScalar(fScalar),
	m_pdrgpexprHashJoinKeysOuter(NULL),
	m_pdrgpexprHashJoinKeysInner(NULL),
	m_pdp(NULL),
	m_pstats(NULL),
	m_pexprScalar(NULL),
	m_pccDummy(NULL),
	m_pgroupDuplicate(NULL),
	m_plinkmap(NULL),
	m_pstatsmap(NULL),
	m_ulGExprs(0),
	m_pcostmap(NULL),
	m_ulpOptCtxts(0),
	m_estate(estUnexplored),
	m_eolMax(EolLow),
	m_fHasNewLogicalOperators(false),
	m_ulCTEProducerId(gpos::ulong_max),
	m_fCTEConsumer(false)
{
	GPOS_ASSERT(NULL != pmp);

	m_listGExprs.Init(GPOS_OFFSET(CGroupExpression, m_linkGroup));
	m_listDupGExprs.Init(GPOS_OFFSET(CGroupExpression, m_linkGroup));

	m_sht.Init
			(
			pmp,
			GPOPT_OPTCTXT_HT_BUCKETS,
			GPOS_OFFSET(COptimizationContext, m_link),
			0, /*cKeyOffset (0 because we use COptimizationContext class as key)*/
			&(COptimizationContext::m_ocInvalid),
			COptimizationContext::UlHash,
			COptimizationContext::FEqual
			);
	m_plinkmap = GPOS_NEW(pmp) LinkMap(pmp);
	m_pstatsmap = GPOS_NEW(pmp) StatsMap(pmp);
	m_pcostmap = GPOS_NEW(pmp) CostMap(pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::~CGroup
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CGroup::~CGroup()
{
	CRefCount::SafeRelease(m_pdrgpexprHashJoinKeysOuter);
	CRefCount::SafeRelease(m_pdrgpexprHashJoinKeysInner);
	CRefCount::SafeRelease(m_pdp);
	CRefCount::SafeRelease(m_pexprScalar);
	CRefCount::SafeRelease(m_pccDummy);
	CRefCount::SafeRelease(m_pstats);
	m_plinkmap->Release();
	m_pstatsmap->Release();
	m_pcostmap->Release();
	
	// cleaning-up group expressions
	CGroupExpression *pgexpr = m_listGExprs.PtFirst();
	while (NULL != pgexpr)
	{
		CGroupExpression *pgexprNext = m_listGExprs.PtNext(pgexpr);
		pgexpr->CleanupContexts();
		pgexpr->Release();
		
		pgexpr = pgexprNext;
	}

	// cleaning-up duplicate expressions
	pgexpr = m_listDupGExprs.PtFirst();
	while (NULL != pgexpr)
	{
		CGroupExpression *pgexprNext = m_listDupGExprs.PtNext(pgexpr);
		pgexpr->CleanupContexts();
		pgexpr->Release();

		pgexpr = pgexprNext;
	}

	// cleanup optimization contexts
	CleanupContexts();
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::CleanupContexts
//
//	@doc:
//		 Destroy stored contexts in hash table
//
//---------------------------------------------------------------------------
void
CGroup::CleanupContexts()
{
	// need to suspend cancellation while cleaning up
	{
		CAutoSuspendAbort asa;

		COptimizationContext *poc = NULL;
		ShtIter shtit(m_sht);

		while (NULL != poc || shtit.FAdvance())
		{
			CRefCount::SafeRelease(poc);

			// iter's accessor scope
			{
				ShtAccIter shtitacc(shtit);
				if (NULL != (poc = shtitacc.Pt()))
				{
					shtitacc.Remove(poc);
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
//		CGroup::UpdateBestCost
//
//	@doc:
//		 Update the group expression with best cost under the given
//		 optimization context
//
//---------------------------------------------------------------------------
void
CGroup::UpdateBestCost
	(
	COptimizationContext *poc,
	CCostContext *pcc
	)
{
	GPOS_ASSERT(CCostContext::estCosted == pcc->Est());

	COptimizationContext *pocFound  = NULL;
	
	{
		// scope for accessor
		ShtAcc shta(Sht(), *poc);
		pocFound = shta.PtLookup();
	}

	GPOS_ASSERT(NULL != pocFound);

	// update best cost context
	CCostContext *pccBest = pocFound->PccBest();
	if
		(
		GPOPT_INVALID_COST != pcc->Cost()
		&& (NULL == pccBest || pcc->FBetterThan(pccBest))
		)
	{
		pocFound->SetBest(pcc);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::PocLookup
//
//	@doc:
//		Lookup a given context in contexts hash table
//
//---------------------------------------------------------------------------
COptimizationContext *
CGroup::PocLookup
	(
	IMemoryPool *pmp,
	CReqdPropPlan *prpp,
	ULONG ulSearchStageIndex
	)
{
	prpp->AddRef();
	COptimizationContext *poc = GPOS_NEW(pmp) COptimizationContext
								(
								pmp,
								this,
								prpp,
								GPOS_NEW(pmp) CReqdPropRelational(GPOS_NEW(pmp) CColRefSet(pmp)), // required relational props is not used when looking up contexts
								GPOS_NEW(pmp) DrgPstat(pmp), // stats context is not used when looking up contexts
								ulSearchStageIndex
								);

	COptimizationContext *pocFound = NULL;
	{
		ShtAcc shta(Sht(), *poc);
		pocFound =  shta.PtLookup();
	}
	poc->Release();

	return pocFound;
}



//---------------------------------------------------------------------------
//	@function:
//		CGroup::PocLookupBest
//
//	@doc:
//		Lookup the best context across all stages for the given required
//		properties
//
//---------------------------------------------------------------------------
COptimizationContext *
CGroup::PocLookupBest
	(
	IMemoryPool *pmp,
	ULONG ulSearchStages,
	CReqdPropPlan *prpp
	)
{
	COptimizationContext *pocBest = NULL;
	CCostContext *pccBest = NULL;
	for (ULONG ul = 0; ul < ulSearchStages; ul++)
	{
		COptimizationContext *pocCurrent = PocLookup(pmp, prpp, ul);
		if (NULL == pocCurrent)
		{
			continue;
		}

		CCostContext *pccCurrent = pocCurrent->PccBest();
		if (NULL == pccBest || (NULL != pccCurrent && pccCurrent->FBetterThan(pccBest)))
		{
			pocBest = pocCurrent;
			pccBest = pccCurrent;
		}
	}

	return pocBest;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::PocInsert
//
//	@doc:
//		Insert a given context into contexts hash table only if a matching
//		context does not already exist;
//		return either the inserted or the existing matching context
//
//---------------------------------------------------------------------------
COptimizationContext *
CGroup::PocInsert
	(
	COptimizationContext *poc
	)
{
	ShtAcc shta(Sht(), *poc);

	COptimizationContext *pocFound = shta.PtLookup();
	if (NULL == pocFound)
	{
		poc->SetId((ULONG) UlpIncOptCtxts());
		shta.Insert(poc);
		return poc;
	}

	return pocFound;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::PgexprBest
//
//	@doc:
//		Lookup best group expression under optimization context
//
//---------------------------------------------------------------------------
CGroupExpression *
CGroup::PgexprBest
	(
	COptimizationContext *poc
	)
{
	ShtAcc shta(Sht(), *poc);
	COptimizationContext *pocFound = shta.PtLookup();
	if (NULL != pocFound)
	{
		return pocFound->PgexprBest();
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::SetId
//
//	@doc:
//		Set group id;
//		separated from constructor to avoid synchronization issues
//
//---------------------------------------------------------------------------
void
CGroup::SetId
	(
	ULONG ulId
	)
{
	GPOS_ASSERT(m_slock.FOwned());
	GPOS_ASSERT(GPOPT_INVALID_GROUP_ID == m_ulId &&
				"Overwriting previously assigned group id");

	m_ulId = ulId;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::InitProperties
//
//	@doc:
//		Initialize group's properties
//
//---------------------------------------------------------------------------
void
CGroup::InitProperties
	(
	CDrvdProp *pdp
	)
{
	GPOS_ASSERT(m_slock.FOwned());
	GPOS_ASSERT(NULL == m_pdp);
	GPOS_ASSERT(NULL != pdp);
	GPOS_ASSERT_IMP(FScalar(), CDrvdProp::EptScalar == pdp->Ept());
	GPOS_ASSERT_IMP(!FScalar(), CDrvdProp::EptRelational == pdp->Ept());

	m_pdp = pdp;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::InitStats
//
//	@doc:
//		Initialize group's stats
//
//---------------------------------------------------------------------------
void
CGroup::InitStats
	(
	IStatistics *pstats
	)
{
	GPOS_ASSERT(m_slock.FOwned());
	GPOS_ASSERT(NULL == m_pstats);
	GPOS_ASSERT(NULL != pstats);

	m_pstats = pstats;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::SetState
//
//	@doc:
//		Set group state;
//
//---------------------------------------------------------------------------
void
CGroup::SetState
	(
	EState estNewState
	)
{
	GPOS_ASSERT(m_slock.FOwned());
	GPOS_ASSERT(estNewState == (EState) (m_estate + 1));

	m_estate = estNewState;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::SetHashJoinKeys
//
//	@doc:
//		Set group hash join keys;
//
//---------------------------------------------------------------------------
void
CGroup::SetHashJoinKeys
	(
	DrgPexpr *pdrgpexprOuter,
	DrgPexpr *pdrgpexprInner
	)
{
	GPOS_ASSERT(m_fScalar);
	GPOS_ASSERT(NULL != pdrgpexprOuter);
	GPOS_ASSERT(NULL != pdrgpexprInner);
	GPOS_ASSERT(m_slock.FOwned());

	if (NULL != m_pdrgpexprHashJoinKeysOuter)
	{
		GPOS_ASSERT(NULL != m_pdrgpexprHashJoinKeysInner);

		// hash join keys have been already set, exit here
		return;
	}

	pdrgpexprOuter->AddRef();
	m_pdrgpexprHashJoinKeysOuter = pdrgpexprOuter;

	pdrgpexprInner->AddRef();
	m_pdrgpexprHashJoinKeysInner = pdrgpexprInner;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::UlHash
//
//	@doc:
//		Hash function for group identification
//
//---------------------------------------------------------------------------
ULONG
CGroup::UlHash() const
{
	ULONG ulId = m_ulId;
	if (FDuplicateGroup() && 0 == m_ulGExprs)
	{
		// group has been merged into another group
	 	ulId = PgroupDuplicate()->UlId();
	}

	return gpos::UlHash<ULONG>(&ulId);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::Insert
//
//	@doc:
//		Insert group expression
//
//---------------------------------------------------------------------------
void
CGroup::Insert
	(
	CGroupExpression *pgexpr
	)
{
	GPOS_ASSERT(m_slock.FOwned());

	m_listGExprs.Append(pgexpr);
	COperator *pop = pgexpr->Pop();
	if (pop->FLogical())
	{
		m_fHasNewLogicalOperators = true;
		if (COperator::EopLogicalCTEConsumer == pop->Eopid())
		{
			m_fCTEConsumer = true;
		}

		if (COperator::EopLogicalCTEProducer == pop->Eopid())
		{
			GPOS_ASSERT(gpos::ulong_max == m_ulCTEProducerId);
			m_ulCTEProducerId = CLogicalCTEProducer::PopConvert(pop)->UlCTEId();;
		}
	}

	if (pgexpr->Eol() > m_eolMax)
	{
		m_eolMax = pgexpr->Eol();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::MoveDuplicateGExpr
//
//	@doc:
//		Move duplicate group expression to duplicates list
//
//---------------------------------------------------------------------------
void
CGroup::MoveDuplicateGExpr
	(
	CGroupExpression *pgexpr
	)
{
	GPOS_ASSERT(m_slock.FOwned());

	m_listGExprs.Remove(pgexpr);
	m_ulGExprs--;

	m_listDupGExprs.Append(pgexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::PgexprAnyCTEConsumer
//
//	@doc:
//		Retrieve the group expression containing a CTE Consumer operator
//
//---------------------------------------------------------------------------
CGroupExpression *
CGroup::PgexprAnyCTEConsumer()
{
	BOOL fFoundCTEConsumer = false;
	// get first logical group expression
	CGroupExpression *pgexprCurrent = NULL;
	{
		CGroupProxy gp(this);
		pgexprCurrent = gp.PgexprNextLogical(NULL /*pgexpr*/);
		fFoundCTEConsumer = (COperator::EopLogicalCTEConsumer == pgexprCurrent->Pop()->Eopid());
	}

	while (NULL != pgexprCurrent && !fFoundCTEConsumer)
	{
		GPOS_CHECK_ABORT;
		// move to next logical group expression
		{
			CGroupProxy gp(this);
			pgexprCurrent = gp.PgexprNextLogical(pgexprCurrent);
		}

		if (NULL != pgexprCurrent)
		{
			COperator *popCurrent = pgexprCurrent->Pop();
			fFoundCTEConsumer = (COperator::EopLogicalCTEConsumer == popCurrent->Eopid());
		}
	}

	if (fFoundCTEConsumer)
	{
		return pgexprCurrent;
	}

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::PgexprFirst
//
//	@doc:
//		Retrieve first expression in group
//
//---------------------------------------------------------------------------
CGroupExpression *
CGroup::PgexprFirst()
{
	GPOS_ASSERT(m_slock.FOwned());

	return m_listGExprs.PtFirst();
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::PgexprNext
//
//	@doc:
//		Retrieve next expression in group
//
//---------------------------------------------------------------------------
CGroupExpression *
CGroup::PgexprNext
	(
	CGroupExpression *pgexpr
	) 
{
	GPOS_ASSERT(m_slock.FOwned());

	return m_listGExprs.PtNext(pgexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::FMatchGroups
//
//	@doc:
//		Determine whether two arrays of groups are equivalent
//
//---------------------------------------------------------------------------
BOOL
CGroup::FMatchGroups
	(
	DrgPgroup *pdrgpgroupFst, 
	DrgPgroup *pdrgpgroupSnd
	)
{
	ULONG ulArity = pdrgpgroupFst->UlLength();
	GPOS_ASSERT(pdrgpgroupSnd->UlLength() == ulArity);
	
	for (ULONG i = 0; i < ulArity; i++)
	{
		CGroup *pgroupFst = (*pdrgpgroupFst)[i];
		CGroup *pgroupSnd = (*pdrgpgroupSnd)[i];
		if (pgroupFst != pgroupSnd && !FDuplicateGroups(pgroupFst, pgroupSnd))
		{
			return false;
		}
	}
	
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::FMatchNonScalarGroups
//
//	@doc:
//		 Matching of pairs of arrays of groups while skipping scalar groups
//
//---------------------------------------------------------------------------
BOOL
CGroup::FMatchNonScalarGroups
	(
	DrgPgroup *pdrgpgroupFst,
	DrgPgroup *pdrgpgroupSnd
	)
{
	GPOS_ASSERT(NULL != pdrgpgroupFst);
	GPOS_ASSERT(NULL != pdrgpgroupSnd);

	if (pdrgpgroupFst->UlLength() != pdrgpgroupSnd->UlLength())
	{
		return false;
	}

	ULONG ulArity = pdrgpgroupFst->UlLength();
	GPOS_ASSERT(pdrgpgroupSnd->UlLength() == ulArity);

	for (ULONG i = 0; i < ulArity; i++)
	{
		CGroup *pgroupFst = (*pdrgpgroupFst)[i];
		CGroup *pgroupSnd = (*pdrgpgroupSnd)[i];
		if (pgroupFst->FScalar())
		{
			// skip scalar groups
			continue;
		}

		if (pgroupFst != pgroupSnd && !FDuplicateGroups(pgroupFst, pgroupSnd))
		{
			return false;
		}
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::FDuplicateGroups
//
//	@doc:
//		Determine whether two groups are equivalent
//
//---------------------------------------------------------------------------
BOOL
CGroup::FDuplicateGroups
	(
	CGroup *pgroupFst,
	CGroup *pgroupSnd
	)
{
 	GPOS_ASSERT(NULL != pgroupFst);
 	GPOS_ASSERT(NULL != pgroupSnd);

 	CGroup *pgroupFstDup = pgroupFst->PgroupDuplicate();
 	CGroup *pgroupSndDup = pgroupSnd->PgroupDuplicate();

 	return (pgroupFst == pgroupSnd) ||		// pointer equality
 			(pgroupFst == pgroupSndDup) ||	// first group is duplicate of second group
 			(pgroupSnd == pgroupFstDup) ||	// second group is duplicate of first group
 			// both groups have the same duplicate group
 			(NULL != pgroupFstDup && NULL != pgroupSndDup && pgroupFstDup == pgroupSndDup);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::FInitStats
//
//	@doc:
//		Attempt initializing stats with the given stat object
//
//---------------------------------------------------------------------------
BOOL
CGroup::FInitStats
	(
	IStatistics *pstats
	)
{
	GPOS_ASSERT(NULL != pstats);

	CGroupProxy gp(this);
	if (NULL == Pstats())
	{
		gp.InitStats(pstats);
		return true;
	}

	// mark group as having no new logical operators to disallow
	// resetting stats until a new logical operator is inserted
	ResetHasNewLogicalOperators();

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::AppendStats
//
//	@doc:
//		Append given stats to group stats
//
//---------------------------------------------------------------------------
void
CGroup::AppendStats
	(
	IMemoryPool *pmp,
	IStatistics *pstats
	)
{
	GPOS_ASSERT(NULL != pstats);
	GPOS_ASSERT(NULL != Pstats());

	IStatistics *pstatsCopy = Pstats()->PstatsCopy(pmp);
	pstatsCopy->AppendStats(pmp, pstats);

	IStatistics *pstatsCurrent = NULL;
	{
		CGroupProxy gp(this);
		pstatsCurrent = m_pstats;
		m_pstats = NULL;
		gp.InitStats(pstatsCopy);
	}

	pstatsCurrent->Release();
	pstatsCurrent = NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::AddDuplicateGrp
//
//	@doc:
//		Add duplicate group
//
//---------------------------------------------------------------------------
void
CGroup::AddDuplicateGrp
	(
	CGroup *pgroup
	)
{
	GPOS_ASSERT(NULL != pgroup);

	// add link following monotonic ordering of group IDs
	CGroup *pgroupSrc = this;
	CGroup *pgroupDest = pgroup;
	if (pgroupSrc->UlId() > pgroupDest->UlId())
	{
		std::swap(pgroupSrc, pgroupDest);
	}

	// keep looping until we add link
	while (pgroupSrc->m_pgroupDuplicate != pgroupDest &&
	       !FCompareSwap<CGroup>
				(
				(volatile CGroup**)&pgroupSrc->m_pgroupDuplicate,
				NULL,
				pgroupDest
				))
	{
		pgroupSrc = pgroupSrc->m_pgroupDuplicate;
		if (pgroupSrc->UlId() > pgroupDest->UlId())
		{
			std::swap(pgroupSrc, pgroupDest);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::ResolveDuplicateMaster
//
//	@doc:
//		Resolve master duplicate group
//
//---------------------------------------------------------------------------
void
CGroup::ResolveDuplicateMaster()
{
	if (!FDuplicateGroup())
	{
		return;
	}
	CGroup *pgroupTarget = m_pgroupDuplicate;
	while (NULL != pgroupTarget->m_pgroupDuplicate)
	{
		GPOS_ASSERT(pgroupTarget->UlId() < pgroupTarget->m_pgroupDuplicate->UlId());
		pgroupTarget = pgroupTarget->m_pgroupDuplicate;
	}

	// update reference to target group
	m_pgroupDuplicate = pgroupTarget;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::MergeGroup
//
//	@doc:
//		Merge group with its duplicate - not thread-safe
//
//---------------------------------------------------------------------------
void
CGroup::MergeGroup()
{
	if (!FDuplicateGroup())
	{
		return;
	}
	GPOS_ASSERT(FExplored());
	GPOS_ASSERT(!FImplemented());

	// resolve target group
	ResolveDuplicateMaster();
	CGroup *pgroupTarget = m_pgroupDuplicate;

	// move group expressions from this group to target
	while (!m_listGExprs.FEmpty())
	{
		CGroupExpression *pgexpr = m_listGExprs.RemoveHead();
		m_ulGExprs--;

		pgexpr->Reset(pgroupTarget, pgroupTarget->m_ulGExprs++);
		pgroupTarget->m_listGExprs.Append(pgexpr);

		GPOS_CHECK_ABORT;
	}

	GPOS_ASSERT(0 == m_ulGExprs);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::CreateScalarExpression
//
//	@doc:
//		Materialize a scalar expression for stat derivation only if
//		this is a scalar group
//
//---------------------------------------------------------------------------
void
CGroup::CreateScalarExpression()
{
	GPOS_ASSERT(FScalar());
	GPOS_ASSERT(NULL == m_pexprScalar);

	CGroupExpression *pgexprFirst = NULL;
	{
		CGroupProxy gp(this);
		pgexprFirst = gp.PgexprFirst();
	}
	GPOS_ASSERT(NULL != pgexprFirst);

	// if group has subquery, cache only the root operator
	// since the underlying tree have relational operator
	if (CDrvdPropScalar::Pdpscalar(Pdp())->FHasSubquery())
	{
		COperator *pop = pgexprFirst->Pop();
		pop->AddRef();
		m_pexprScalar = GPOS_NEW(m_pmp) CExpression (m_pmp, pop);

		return;
	}

	DrgPexpr *pdrgpexpr = GPOS_NEW(m_pmp) DrgPexpr(m_pmp);
	const ULONG ulArity = pgexprFirst->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CGroup *pgroupChild = (*pgexprFirst)[ul];
		GPOS_ASSERT(pgroupChild->FScalar());

		CExpression *pexprChild = pgroupChild->PexprScalar();
		pexprChild->AddRef();
		pdrgpexpr->Append(pexprChild);
	}

	COperator *pop = pgexprFirst->Pop();
	pop->AddRef();
	m_pexprScalar = GPOS_NEW(m_pmp) CExpression(m_pmp, pop, pdrgpexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::CreateDummyCostContext
//
//	@doc:
//		Create a dummy cost context attached to the first group expression,
//		used for plan enumeration for scalar groups
//
//
//---------------------------------------------------------------------------
void
CGroup::CreateDummyCostContext()
{
	GPOS_ASSERT(FScalar());
	GPOS_ASSERT(NULL == m_pccDummy);

	CGroupExpression *pgexprFirst = NULL;
	{
		CGroupProxy gp(this);
		pgexprFirst = gp.PgexprFirst();
	}
	GPOS_ASSERT(NULL != pgexprFirst);

	COptimizationContext *poc = GPOS_NEW(m_pmp) COptimizationContext
						(
						m_pmp,
						this,
						CReqdPropPlan::PrppEmpty(m_pmp),
						GPOS_NEW(m_pmp) CReqdPropRelational(GPOS_NEW(m_pmp) CColRefSet(m_pmp)),
						GPOS_NEW(m_pmp) DrgPstat(m_pmp),
						0 // ulSearchStageIndex
						);

	pgexprFirst->AddRef();
	m_pccDummy = GPOS_NEW(m_pmp) CCostContext(m_pmp, poc, 0 /*ulOptReq*/, pgexprFirst);
	m_pccDummy->SetState(CCostContext::estCosting);
	m_pccDummy->SetCost(CCost(0.0));
	m_pccDummy->SetState(CCostContext::estCosted);

#ifdef GPOS_DEBUG
	CGroupExpression *pgexprNext = NULL;
	{
		CGroupProxy gp(this);
		pgexprNext = gp.PgexprNext(pgexprFirst);
	}
	GPOS_ASSERT(NULL == pgexprNext && "scalar group can only have one group expression");
#endif // GPOS_DEBUG

}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::RecursiveBuildTreeMap
//
//	@doc:
//		Find all cost contexts of current group expression that carry valid
//		implementation of the given optimization context,
//		for all such cost contexts, introduce a link from parent cost context
//		to child cost context and then process child groups recursively
//
//
//---------------------------------------------------------------------------
void
CGroup::RecursiveBuildTreeMap
	(
	IMemoryPool *pmp,
	COptimizationContext *poc,
	CCostContext *pccParent,
	CGroupExpression *pgexprCurrent,
	ULONG ulChildIndex,
	CTreeMap<CCostContext, CExpression, CDrvdPropCtxtPlan, CCostContext::UlHash, CCostContext::FEqual> *ptmap
	)
{
	GPOS_ASSERT(pgexprCurrent->Pop()->FPhysical());
	GPOS_ASSERT(NULL != ptmap);
	GPOS_ASSERT_IMP(NULL != pccParent, ulChildIndex < pccParent->Pgexpr()->UlArity());

	DrgPcc *pdrgpcc = pgexprCurrent->PdrgpccLookupAll(pmp, poc);
	const ULONG ulCCSize = pdrgpcc->UlLength();

	if (0 == ulCCSize)
	{
		// current group expression has no valid implementations of optimization context
		pdrgpcc->Release();
		return;
	}

	// iterate over all valid implementations of given optimization context
	for (ULONG ulCC = 0; ulCC < ulCCSize; ulCC++)
	{
		GPOS_CHECK_ABORT;

		CCostContext *pccCurrent = (*pdrgpcc)[ulCC];
		if (NULL != pccParent)
		{
			// link parent cost context to child cost context
			ptmap->Insert(pccParent, ulChildIndex, pccCurrent);
		}

		DrgPoc *pdrgpoc = pccCurrent->Pdrgpoc();
		if (NULL != pdrgpoc)
		{
			// process children recursively
			const ULONG ulArity = pgexprCurrent->UlArity();
			for (ULONG ul = 0; ul < ulArity; ul++)
			{
				GPOS_CHECK_ABORT;

				CGroup *pgroupChild = (*pgexprCurrent)[ul];
				COptimizationContext *pocChild = NULL;
				if (!pgroupChild->FScalar())
				{
					pocChild = (*pdrgpoc)[ul];
					GPOS_ASSERT(NULL != pocChild);
				}
				pgroupChild->BuildTreeMap(pmp, pocChild, pccCurrent, ul, ptmap);
			}
		}
	}

	pdrgpcc->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::BuildTreeMap
//
//	@doc:
//		Given a parent cost context and an optimization context,
//		link parent cost context to all cost contexts in current group
//		that carry valid implementation of the given optimization context
//
//
//---------------------------------------------------------------------------
void
CGroup::BuildTreeMap
	(
	IMemoryPool *pmp,
	COptimizationContext *poc, // NULL if we are in a Scalar group
	CCostContext *pccParent, // NULL if we are in the Root group
	ULONG ulChildIndex, // index used for treating group as child of parent context
	CTreeMap<CCostContext, CExpression, CDrvdPropCtxtPlan, CCostContext::UlHash, CCostContext::FEqual> *ptmap // map structure
	)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != ptmap);
	GPOS_ASSERT_IMP(NULL == poc, FScalar());

#ifdef GPOS_DEBUG
	CGroupExpression *pgexprParent = NULL;
#endif  // GPOS_DEBUG
	if (NULL != pccParent)
	{
		pccParent->AddRef();
#ifdef GPOS_DEBUG
		pgexprParent = pccParent->Pgexpr();
#endif  // GPOS_DEBUG
	}
	if (NULL != poc)
	{
		poc->AddRef();
	}

	// check if link has been processed before,
	// this is crucial to eliminate unnecessary recursive calls
	SContextLink *pclink = GPOS_NEW(m_pmp) SContextLink(pccParent, ulChildIndex, poc);
	if (m_plinkmap->PtLookup(pclink))
	{
		// link is already processed
		GPOS_DELETE(pclink);
		return;
	}

	// start with first non-logical group expression
	CGroupExpression *pgexprCurrent = NULL;
	{
		CGroupProxy gp(this);
		pgexprCurrent = gp.PgexprSkipLogical(NULL /*pgexpr*/);
	}

	while (NULL != pgexprCurrent)
	{
		COperator *pop = pgexprCurrent->Pop();
		if (pop->FPhysical())
		{
			// create links recursively
			RecursiveBuildTreeMap(pmp, poc, pccParent, pgexprCurrent, ulChildIndex, ptmap);
		}
		else
		{
			GPOS_ASSERT(pop->FScalar());
			GPOS_ASSERT(NULL == poc);
			GPOS_ASSERT(ulChildIndex < pgexprParent->UlArity());

			// this is a scalar group, link parent cost context to group's dummy context
			ptmap->Insert(pccParent, ulChildIndex, PccDummy());

			// recursively link group's dummy context to child contexts
			const ULONG ulArity = pgexprCurrent->UlArity();
			for (ULONG ul = 0; ul < ulArity; ul++)
			{
				CGroup *pgroupChild = (*pgexprCurrent)[ul];
				GPOS_ASSERT(pgroupChild->FScalar());

				pgroupChild->BuildTreeMap(pmp, NULL /*poc*/, PccDummy(), ul, ptmap);
			}
		}

		// move to next non-logical group expression
		{
			CGroupProxy gp(this);
			pgexprCurrent = gp.PgexprSkipLogical(pgexprCurrent);
		}
		GPOS_ASSERT_IMP(FScalar(), NULL == pgexprCurrent &&
				"a scalar group can only have one group expression");

		GPOS_CHECK_ABORT;
	}

	// remember processed links to avoid re-processing them later
#ifdef GPOS_DEBUG
	BOOL fInserted =
#endif  // GPOS_DEBUG
		m_plinkmap->FInsert(pclink, GPOS_NEW(m_pmp) BOOL(true));
	GPOS_ASSERT(fInserted);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::FStatsDerivable
//
//	@doc:
//		Returns true if stats can be derived on this group
//
//---------------------------------------------------------------------------
BOOL
CGroup::FStatsDerivable
	(
	IMemoryPool *pmp
	)
{
	GPOS_CHECK_STACK_SIZE;

	if (NULL != m_pstats)
	{
		return true;
	}

	CGroupExpression *pgexprBest = NULL;
	CLogical::EStatPromise espBest = CLogical::EspNone;

	CGroupExpression *pgexprCurrent = NULL;
	// get first logical group expression
	{
		CGroupProxy gp(this);
		pgexprCurrent = gp.PgexprNextLogical(NULL /*pgexpr*/);
	}

	while (NULL != pgexprCurrent)
	{
		CExpressionHandle exprhdl(pmp);
		exprhdl.Attach(pgexprCurrent);
		CDrvdPropCtxtRelational *pdpctxtrel = GPOS_NEW(pmp) CDrvdPropCtxtRelational(pmp);
		exprhdl.DeriveProps(pdpctxtrel);
		pdpctxtrel->Release();

		CLogical *popLogical = CLogical::PopConvert(pgexprCurrent->Pop());
		CLogical::EStatPromise esp = popLogical->Esp(exprhdl);

		if (esp > espBest)
		{
			pgexprBest = pgexprCurrent;
			espBest = esp;
		}

		// move to next logical group expression
		{
			CGroupProxy gp(this);
			pgexprCurrent = gp.PgexprNextLogical(pgexprCurrent);
		}

		GPOS_CHECK_ABORT;
	}

	if (NULL == pgexprBest)
	{
		return false;
	}

	BOOL fStatsDerivable = true;
	const ULONG ulArity = pgexprBest->UlArity();
	for (ULONG ul = 0; fStatsDerivable && ul < ulArity; ul++)
	{
		CGroup *pgroupChild = (*pgexprBest)[ul];
		fStatsDerivable = (pgroupChild->FScalar() || pgroupChild->FStatsDerivable(pmp));
	}

	return fStatsDerivable;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::FBetterPromise
//
//	@doc:
//		Return true if first promise is better than second promise
//
//---------------------------------------------------------------------------
BOOL
CGroup::FBetterPromise
	(
	IMemoryPool *pmp,
	CLogical::EStatPromise espFst,
	CGroupExpression *pgexprFst,
	CLogical::EStatPromise espSnd,
	CGroupExpression *pgexprSnd
	)
	const
{
	// if there is a tie and both group expressions are inner join, we prioritize
	// the inner join having less predicates
	return
		espFst > espSnd ||
		(espFst == espSnd &&
			CLogicalInnerJoin::FFewerConj(pmp, pgexprFst, pgexprSnd));
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::EspDerive
//
//	@doc:
//		Derive statistics recursively on group expression
//
//---------------------------------------------------------------------------
CLogical::EStatPromise
CGroup::EspDerive
	(
	IMemoryPool *pmpLocal,
	IMemoryPool *pmpGlobal,
	CGroupExpression *pgexpr,
	CReqdPropRelational *prprel,
	DrgPstat *pdrgpstatCtxt,
	BOOL fDeriveChildStats
	)
{
	GPOS_ASSERT(pgexpr->Pop()->FLogical());

	CExpressionHandle exprhdl(pmpGlobal);
	exprhdl.Attach(pgexpr);
	CDrvdPropCtxtRelational *pdpctxtrel = GPOS_NEW(pmpGlobal) CDrvdPropCtxtRelational(pmpGlobal);
	exprhdl.DeriveProps(pdpctxtrel);
	pdpctxtrel->Release();

	// compute stat promise for gexpr
	CLogical *popLogical = CLogical::PopConvert(pgexpr->Pop());
	CLogical::EStatPromise esp = popLogical->Esp(exprhdl);

	// override promise if outer child references columns of inner children
	if (2 < exprhdl.UlArity() &&
		!exprhdl.FScalarChild(0 /*ulChildIndex*/) &&
		exprhdl.FHasOuterRefs(0 /*ulChildIndex*/) &&
		!exprhdl.FHasOuterRefs())
	{
		// stat derivation always starts by outer child,
		// any outer references in outer child cannot be resolved for stats derivation purposes
		esp = CLogical::EspLow;
	}

	if (fDeriveChildStats && esp > CLogical::EspNone)
	{
		// we only aim here at triggering stat derivation on child groups recursively,
		// there is no need to compute stats for group expression's root operator
		IStatistics *pstats = pgexpr->PstatsRecursiveDerive(pmpLocal, pmpGlobal, prprel, pdrgpstatCtxt, false /*fComputeRootStats*/);
		CRefCount::SafeRelease(pstats);
	}

	return esp;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::PstatsRecursiveDerive
//
//	@doc:
//		Derive statistics recursively on group
//
//---------------------------------------------------------------------------
IStatistics *
CGroup::PstatsRecursiveDerive
	(
	IMemoryPool *pmpLocal,
	IMemoryPool *pmpGlobal,
	CReqdPropRelational *prprel,
	DrgPstat *pdrgpstatCtxt
	)
{
	GPOS_CHECK_STACK_SIZE;

	GPOS_ASSERT(!FImplemented());
	GPOS_ASSERT(NULL != pdrgpstatCtxt);
	GPOS_CHECK_ABORT;

	// create empty stats if a scalar group
	if (FScalar())
	{
		return PstatsInitEmpty(pmpGlobal);
	}

	IStatistics *pstats = NULL;
	// if this is a duplicate group, return stats from the duplicate
	if (FDuplicateGroup())
	{
		// call stat derivation on the duplicate group
		pstats = PgroupDuplicate()->PstatsRecursiveDerive
				(
				pmpLocal,
				pmpGlobal,
				prprel,
				pdrgpstatCtxt
				);
	}
	GPOS_ASSERT(0 < m_ulGExprs);

	prprel->AddRef();
	CReqdPropRelational *prprelInput = prprel;

	if (NULL == pstats)
	{
		pstats = Pstats();
	}
	// if group has derived stats, check if requirements are covered
	// by what's already derived

	if (NULL != pstats)
	{
		prprelInput->Release();
		CReqdPropRelational *prprelExisting = pstats->Prprel(pmpGlobal);
		prprelInput = prprel->PrprelDifference(pmpGlobal, prprelExisting);
		prprelExisting->Release();
		if (prprelInput->FEmpty())
		{
			// required stat columns are already covered by existing stats
			prprelInput->Release();
			return pstats;
		}
	}

	// required stat columns are not covered by existing stats, we need to
	// derive the missing ones

	// find the best group expression to derive stats on
	CGroupExpression *pgexprBest = PgexprBestPromise(pmpLocal, pmpGlobal, prprelInput, pdrgpstatCtxt);

	if (NULL == pgexprBest)
	{
		GPOS_RAISE
			(
			gpopt::ExmaGPOPT,
			gpopt::ExmiNoPlanFound,
			GPOS_WSZ_LIT("Could not choose a group expression for statistics derivation")
			);
	}

	// derive stats on group expression and copy them to group
	pstats = pgexprBest->PstatsRecursiveDerive(pmpLocal, pmpGlobal, prprelInput, pdrgpstatCtxt);
	if (!FInitStats(pstats))
	{
		// a group stat object already exists, we append derived stats to that object
		AppendStats(pmpGlobal, pstats);
		pstats->Release();
	}
	GPOS_ASSERT(NULL != Pstats());

	prprelInput->Release();

	return Pstats();
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::PgexprBestPromise
//
//	@doc:
//		Find group expression with best stats promise and the
//		same children as given expression
//
//---------------------------------------------------------------------------
CGroupExpression *
CGroup::PgexprBestPromise
	(
	IMemoryPool *pmp,
	CGroupExpression *pgexprToMatch
	)
{
	GPOS_ASSERT(NULL != pgexprToMatch);

	CReqdPropRelational *prprel = GPOS_NEW(pmp) CReqdPropRelational(GPOS_NEW(pmp) CColRefSet(pmp));
	DrgPstat *pdrgpstatCtxt = GPOS_NEW(pmp) DrgPstat(pmp);

	CLogical::EStatPromise espBest = CLogical::EspNone;
	CGroupExpression *pgexprCurrent = NULL;
	CGroupExpression *pgexprBest = NULL;
	// get first logical group expression
	{
		CGroupProxy gp(this);
		pgexprCurrent = gp.PgexprNextLogical(NULL /*pgexpr*/);
	}

	while (NULL != pgexprCurrent)
	{
		CLogical::EStatPromise espCurrent =
			EspDerive(pmp, pmp, pgexprCurrent, prprel, pdrgpstatCtxt, false /*fDeriveChildStats*/);

		if (pgexprCurrent->FMatchNonScalarChildren(pgexprToMatch) &&
			FBetterPromise(pmp, espCurrent, pgexprCurrent, espBest, pgexprBest))
		{
			pgexprBest = pgexprCurrent;
			espBest = espCurrent;
		}

		// move to next logical group expression
		{
			CGroupProxy gp(this);
			pgexprCurrent = gp.PgexprNextLogical(pgexprCurrent);
		}

		GPOS_CHECK_ABORT;
	}

	prprel->Release();
	pdrgpstatCtxt->Release();

	return pgexprBest;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::PgexprBestPromise
//
//	@doc:
//		Find the group expression having the best stats promise for this group
//
//---------------------------------------------------------------------------
CGroupExpression *
CGroup::PgexprBestPromise
	(
	IMemoryPool *pmpLocal,
	IMemoryPool *pmpGlobal,
	CReqdPropRelational *prprelInput,
	DrgPstat *pdrgpstatCtxt
	)
{
	CGroupExpression *pgexprBest = NULL;
	CLogical::EStatPromise espBest = CLogical::EspNone;
	CGroupExpression *pgexprCurrent = NULL;
	// get first logical group expression
	{
		CGroupProxy gp(this);
		pgexprCurrent = gp.PgexprNextLogical(NULL /*pgexpr*/);
	}

	while (NULL != pgexprCurrent)
	{
		CLogical::EStatPromise espCurrent =
			EspDerive(pmpLocal, pmpGlobal, pgexprCurrent, prprelInput, pdrgpstatCtxt, true /*fDeriveChildStats*/);

		if (FBetterPromise(pmpLocal, espCurrent, pgexprCurrent, espBest, pgexprBest))
		{
			pgexprBest = pgexprCurrent;
			espBest = espCurrent;
		}

		// move to next logical group expression
		{
			CGroupProxy gp(this);
			pgexprCurrent = gp.PgexprNextLogical(pgexprCurrent);
		}

		GPOS_CHECK_ABORT;
	}

	return pgexprBest;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::PstatsInitEmpty
//
//	@doc:
//		Initialize and return empty stats for this group
//
//---------------------------------------------------------------------------
IStatistics *
CGroup::PstatsInitEmpty
	(
	IMemoryPool *pmpGlobal
	)
{
	CStatistics *pstats = CStatistics::PstatsEmpty(pmpGlobal);
	if (!FInitStats(pstats))
	{
		pstats->Release();
	}

	return Pstats();
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::OsPrintGrpOptCtxts
//
//	@doc:
//		Print group optimization contexts
//
//---------------------------------------------------------------------------
IOstream &
CGroup::OsPrintGrpOptCtxts
	(
	IOstream &os,
	const CHAR *szPrefix
	)
{
	if (!FScalar() && !FDuplicateGroup() && GPOS_FTRACE(EopttracePrintOptimizationContext))
	{
		os << szPrefix << "Grp OptCtxts:" << std::endl;

		COptimizationContext *poc = NULL;
		ShtIter shtit(m_sht);
		while (shtit.FAdvance())
		{
			{
				ShtAccIter shtitacc(shtit);
				poc = shtitacc.Pt();
			}

			if (NULL != poc)
			{
				os << szPrefix;
				(void) poc->OsPrint(os, szPrefix);
			}

			GPOS_CHECK_ABORT;
		}
	}

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::OsPrintGrpScalarProps
//
//	@doc:
//		Print scalar group properties
//
//---------------------------------------------------------------------------
IOstream &
CGroup::OsPrintGrpScalarProps
	(
	IOstream &os,
	const CHAR *szPrefix
	)
{
	GPOS_ASSERT(FScalar());

	if (NULL != PexprScalar())
	{
		os << szPrefix << "Scalar Expression: "<< std::endl
			<< szPrefix << *PexprScalar() << std::endl;
	}

	GPOS_CHECK_ABORT;

	if (NULL != m_pdrgpexprHashJoinKeysOuter)
	{
		os << szPrefix << "Outer Hash Join Keys: " << std::endl;

		const ULONG ulSize = m_pdrgpexprHashJoinKeysOuter->UlLength();
		for (ULONG ul = 0; ul < ulSize; ul++)
		{
			os << szPrefix << *(*m_pdrgpexprHashJoinKeysOuter)[ul]<< std::endl;
		}
	}

	GPOS_CHECK_ABORT;

	if (NULL != m_pdrgpexprHashJoinKeysInner)
	{
		os << szPrefix << "Inner Hash Join Keys: " << std::endl;

		const ULONG ulSize = m_pdrgpexprHashJoinKeysInner->UlLength();
		for (ULONG ul = 0; ul < ulSize; ul++)
		{
			os << szPrefix << *(*m_pdrgpexprHashJoinKeysInner)[ul]<< std::endl;
		}
	}

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::OsPrintGrpProps
//
//	@doc:
//		Print group properties and stats
//
//---------------------------------------------------------------------------
IOstream &
CGroup::OsPrintGrpProps
	(
	IOstream &os,
	const CHAR *szPrefix
	)
{
	if (!FDuplicateGroup() && GPOS_FTRACE(EopttracePrintGroupProperties))
	{
		os << szPrefix << "Grp Props:" << std::endl
		   << szPrefix << szPrefix << *m_pdp << std::endl;
		if (!FScalar() && NULL != m_pstats)
		{
		   os << szPrefix << "Grp Stats:" << std::endl
			  << szPrefix << szPrefix << *m_pstats;
		}

		if (FScalar())
		{
			(void) OsPrintGrpScalarProps(os, szPrefix);
		}

		GPOS_CHECK_ABORT;
	}

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::ResetGroupState
//
//	@doc:
//		Reset group state;
//		resetting state is not thread-safe
//
//---------------------------------------------------------------------------
void
CGroup::ResetGroupState()
{
	// reset group expression states
	CGroupExpression *pgexpr = m_listGExprs.PtFirst();
	while (NULL != pgexpr)
	{
		pgexpr->ResetState();
		pgexpr = m_listGExprs.PtNext(pgexpr);

		GPOS_CHECK_ABORT;
	}

	// reset group state
	{
		CGroupProxy gp(this);
		m_estate = estUnexplored;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::Pstats
//
//	@doc:
//		Group stats accessor
//
//---------------------------------------------------------------------------
IStatistics *
CGroup::Pstats() const
{
	if (NULL != m_pstats)
	{
		return m_pstats;
	}

	if (FDuplicateGroup())
	{
		return PgroupDuplicate()->Pstats();
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::ResetStats
//
//	@doc:
//		Reset computed stats;
//
//
//---------------------------------------------------------------------------
void
CGroup::ResetStats()
{
	GPOS_ASSERT(!FScalar());

	IStatistics *pstats = NULL;
	{
		CGroupProxy gp(this);
		pstats = m_pstats;
		m_pstats = NULL;
	}
 	CRefCount::SafeRelease(pstats);
 	pstats = NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::ResetLinkMap
//
//	@doc:
//		Reset link map for plan enumeration;
//		this operation is not thread safe
//
//
//---------------------------------------------------------------------------
void
CGroup::ResetLinkMap()
{
	GPOS_ASSERT(NULL != m_plinkmap);

	m_plinkmap->Release();
	m_plinkmap = GPOS_NEW(m_pmp) LinkMap(m_pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::FResetStats
//
//	@doc:
//		Check if we need to reset group stats before deriving statistics;
//		this function reset group stats in the following two cases:
//		(1) current group has newly-added logical operators, this can happen
//		during multi-stage search, where stage_{i+1} may append new logical
//		operators to a group created at stage_{i}
//		(2) a child group, reachable from current group at any depth, has new
//		logical group expressions
//
//---------------------------------------------------------------------------
BOOL
CGroup::FResetStats()
{
	GPOS_CHECK_STACK_SIZE;

	if (NULL == Pstats())
	{
		// end recursion early if group stats have been already reset
		return true;
	}

	BOOL fResetStats = false;
	if (FHasNewLogicalOperators())
	{
		fResetStats = true;
	}

	// get first logical group expression
	CGroupExpression *pgexprCurrent = NULL;
	{
		CGroupProxy gp(this);
		pgexprCurrent = gp.PgexprNextLogical(NULL /*pgexpr*/);
	}

	// recursively process child groups reachable from current group
	while (NULL != pgexprCurrent)
	{
		const ULONG ulArity = pgexprCurrent->UlArity();
		for (ULONG ul = 0; ul < ulArity; ul++)
		{
			GPOS_CHECK_ABORT;

			CGroup *pgroupChild = (*pgexprCurrent)[ul];
			if (!pgroupChild->FScalar() && pgroupChild->FResetStats())
			{
				fResetStats = true;
				// we cannot break here since we must visit all child groups
			}
		}

		// move to next logical group expression
		{
			CGroupProxy gp(this);
			pgexprCurrent = gp.PgexprNextLogical(pgexprCurrent);
		}

		GPOS_CHECK_ABORT;
	}

	if (fResetStats)
	{
		ResetStats();
	}

	return fResetStats;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::ResetGroupJobQueues
//
//	@doc:
//		Reset group job queues;
//
//---------------------------------------------------------------------------
void
CGroup::ResetGroupJobQueues()
{
	CGroupProxy gp(this);
	m_jqExploration.Reset();
	m_jqImplementation.Reset();
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::PstatsCompute
//
//	@doc:
//		Compute stats during costing
//
//---------------------------------------------------------------------------
IStatistics *
CGroup::PstatsCompute
	(
	COptimizationContext *poc,
	CExpressionHandle &exprhdl,
	CGroupExpression *pgexpr
	)
{
	GPOS_ASSERT(NULL != poc);
	GPOS_ASSERT(NULL != pgexpr);
	GPOS_ASSERT(this == pgexpr->Pgroup());

	IStatistics *pstats = m_pstatsmap->PtLookup(poc);
	if (NULL != pstats)
	{
		return pstats;
	}

	pstats = CLogical::PopConvert(pgexpr->Pop())->PstatsDerive(m_pmp, exprhdl, poc->Pdrgpstat());
	GPOS_ASSERT(NULL != pstats);

#ifdef GPOS_DEBUG
	BOOL fSuccess = false;
#endif  // GPOS_DEBUG

	// add computed stats to local map -- we can't use group proxy here due to potential memory allocation
	// which is disallowed with spin locks
	{
		CAutoMutex am(m_mutexStats);
		am.Lock();
		poc->AddRef();
#ifdef GPOS_DEBUG
		fSuccess =
#endif  // GPOS_DEBUG
		m_pstatsmap->FInsert(poc, pstats);
	}
	GPOS_ASSERT(fSuccess);

	return pstats;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::OsPrint
//
//	@doc:
//		Print function;
//		printing is not thread-safe
//
//---------------------------------------------------------------------------
IOstream &
CGroup::OsPrint
	(
	IOstream &os
	)
{
	const CHAR *szPrefix = "  ";
	os << std::endl << "Group " << m_ulId << " (";
	if (!FScalar())
	{
		os << "#GExprs: " << m_listGExprs.UlSize();

		if (0 < m_listDupGExprs.UlSize())
		{
			os << ", #Duplicate GExprs: " << m_listDupGExprs.UlSize();
		}
		if (FDuplicateGroup())
		{
			os << ", Duplicate Group: " << m_pgroupDuplicate->UlId();
		}
	}
	os << "):" << std::endl;

	CGroupExpression *pgexpr = m_listGExprs.PtFirst();
	while (NULL != pgexpr)
	{
		(void) pgexpr->OsPrint(os, szPrefix);
		pgexpr = m_listGExprs.PtNext(pgexpr);

		GPOS_CHECK_ABORT;
	}

	(void) OsPrintGrpProps(os, szPrefix);
	(void) OsPrintGrpOptCtxts(os, szPrefix);

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::CostLowerBound
//
//	@doc:
//		Compute a cost lower bound on plans, rooted by a group expression
//		in current group, and satisfying the given required properties
//
//---------------------------------------------------------------------------
CCost
CGroup::CostLowerBound
	(
	IMemoryPool *pmp,
	CReqdPropPlan *prppInput
	)
{
	GPOS_ASSERT(NULL != prppInput);
	GPOS_ASSERT(!FScalar());

	CCost *pcostLowerBound =  m_pcostmap->PtLookup(prppInput);
	if (NULL != pcostLowerBound)
	{
		return *pcostLowerBound;
	}

	CCost costLowerBound = GPOPT_INFINITE_COST;

	// start with first non-logical group expression
	CGroupExpression *pgexprCurrent = NULL;
	{
		CGroupProxy gp(this);
		pgexprCurrent = gp.PgexprSkipLogical(NULL /*pgexpr*/);
	}

	while (NULL != pgexprCurrent)
	{
		// considering an enforcer introduces a deadlock here since its child is
		// the same group that contains it,
		// since an enforcer must reside on top of another operator from the same
		// group, it cannot produce a better cost lower-bound and can be skipped here

		if (!CUtils::FEnforcer(pgexprCurrent->Pop()))
		{
			CCost costLowerBoundGExpr =
				pgexprCurrent->CostLowerBound(pmp, prppInput, NULL /*pccChild*/,
											  gpos::ulong_max /*ulChildIndex*/);
			if (costLowerBoundGExpr < costLowerBound)
			{
				costLowerBound = costLowerBoundGExpr;
			}
		}

		// move to next non-logical group expression
		{
			CGroupProxy gp(this);
			pgexprCurrent = gp.PgexprSkipLogical(pgexprCurrent);
		}
	}


	prppInput->AddRef();
#ifdef GPOS_DEBUG
	BOOL fSuccess =
#endif // GPOS_DEBUG
		m_pcostmap->FInsert(prppInput, GPOS_NEW(pmp) CCost(costLowerBound.DVal()));
	GPOS_ASSERT(fSuccess);

	return costLowerBound;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CGroup::DbgPrint
//
//	@doc:
//		Print driving function for use in interactive debugging;
//		always prints to stderr;
//
//---------------------------------------------------------------------------
void
CGroup::DbgPrint()
{
	CAutoTraceFlag atf(EopttracePrintGroupProperties, true);
	CAutoTrace at(m_pmp);
	(void) this->OsPrint(at.Os());
}
#endif

// EOF

