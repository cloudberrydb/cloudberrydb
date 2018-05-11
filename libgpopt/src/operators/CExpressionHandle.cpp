//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CExpressionHandle.cpp
//
//	@doc:
//		Handle to an expression to abstract topology;
//
//		The handle provides access to an expression and the properties
//		of its children; regardless of whether the expression is a group
//		expression or a stand-alone tree;
//---------------------------------------------------------------------------

#include "gpos/base.h"


#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CCostContext.h"
#include "gpopt/base/CCTEReq.h"
#include "gpopt/base/CDrvdPropCtxtPlan.h"
#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CReqdPropPlan.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogical.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/operators/CPattern.h"
#include "gpopt/operators/CPhysicalScan.h"
#include "gpopt/operators/CLogicalCTEConsumer.h"
#include "gpopt/operators/CPhysicalCTEConsumer.h"
#include "gpopt/base/COptCtxt.h"

#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpnaucrates;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::CExpressionHandle
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CExpressionHandle::CExpressionHandle
	(
	IMemoryPool *mp
	)
	:
	m_mp(mp),
	m_pexpr(NULL),
	m_pgexpr(NULL),
	m_pcc(NULL),
	m_pdp(NULL),
	m_pstats(NULL),
	m_prp(NULL),
	m_pdrgpdp(NULL),
	m_pdrgpstat(NULL),
	m_pdrgprp(NULL)
{
	GPOS_ASSERT(NULL != mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::~CExpressionHandle
//
//	@doc:
//		dtor
//
//		Since handles live on the stack this dtor will be called during
//		exceptions, hence, need to be defensive
//
//---------------------------------------------------------------------------
CExpressionHandle::~CExpressionHandle()
{
	CRefCount::SafeRelease(m_pexpr);
	CRefCount::SafeRelease(m_pgexpr);
	CRefCount::SafeRelease(m_pdp);
	CRefCount::SafeRelease(m_pstats);
	CRefCount::SafeRelease(m_prp);
	CRefCount::SafeRelease(m_pdrgpdp);
	CRefCount::SafeRelease(m_pdrgpstat);
	CRefCount::SafeRelease(m_pdrgprp);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::FStatsDerived
//
//	@doc:
//		Check if stats are derived for attached expression and its children
//
//---------------------------------------------------------------------------
BOOL
CExpressionHandle::FStatsDerived() const
{
	IStatistics *stats = NULL;
	if (NULL != m_pexpr)
	{
		stats = const_cast<IStatistics *>(m_pexpr->Pstats());
	}
	else
	{
		GPOS_ASSERT(NULL != m_pgexpr);
		stats = m_pgexpr->Pgroup()->Pstats();
	}

	if (NULL == stats)
	{
		// stats of attached expression have not been derived yet
		return false;
	}

	const ULONG arity = Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (FScalarChild(ul))
		{
			// skip scalar children
			continue;
		}

		IStatistics *child_stats = NULL;
		if (NULL != m_pexpr)
		{
			child_stats = const_cast<IStatistics *>((*m_pexpr)[ul]->Pstats());
		}
		else
		{
			child_stats = (*m_pgexpr)[ul]->Pstats();
		}

		if (NULL == child_stats)
		{
			// stats of attached expression child have not been derived yet
			return false;
		}
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::CopyStats
//
//	@doc:
//		Copy stats from attached expression/group expression to local stats
//		members
//
//---------------------------------------------------------------------------
void
CExpressionHandle::CopyStats()
{
	if (!FStatsDerived())
	{
		// stats of attached expression (or its children) have not been derived yet
		return;
	}

	IStatistics *stats = NULL;
	if (NULL != m_pexpr)
	{
		stats = const_cast<IStatistics *>(m_pexpr->Pstats());
	}
	else
	{
		GPOS_ASSERT(NULL != m_pgexpr);
		stats = m_pgexpr->Pgroup()->Pstats();
	}
	GPOS_ASSERT(NULL != stats);

	// attach stats
	stats->AddRef();
	GPOS_ASSERT(NULL == m_pstats);
	m_pstats = stats;

	// attach child stats
	GPOS_ASSERT(NULL == m_pdrgpstat);
	m_pdrgpstat = GPOS_NEW(m_mp) IStatisticsArray(m_mp);
	const ULONG arity = Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		IStatistics *child_stats = NULL;
		if (NULL != m_pexpr)
		{
			child_stats = const_cast<IStatistics *>((*m_pexpr)[ul]->Pstats());
		}
		else
		{
			child_stats = (*m_pgexpr)[ul]->Pstats();
		}

		if (NULL != child_stats)
		{
			child_stats->AddRef();
		}
		else
		{
			GPOS_ASSERT(FScalarChild(ul));

			// create dummy stats for missing scalar children
			child_stats = CStatistics::MakeEmptyStats(m_mp);
		}

		m_pdrgpstat->Append(child_stats);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Attach
//
//	@doc:
//		Attach to a given expression
//
//---------------------------------------------------------------------------
void
CExpressionHandle::Attach
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL == m_pexpr);
	GPOS_ASSERT(NULL == m_pgexpr);
	GPOS_ASSERT(NULL != pexpr);

	// increment ref count on base expression
	pexpr->AddRef();
	m_pexpr = pexpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Attach
//
//	@doc:
//		Attach to a given group expression
//
//---------------------------------------------------------------------------
void
CExpressionHandle::Attach
	(
	CGroupExpression *pgexpr
	)
{
	GPOS_ASSERT(NULL == m_pexpr);
	GPOS_ASSERT(NULL == m_pgexpr);
	GPOS_ASSERT(NULL != pgexpr);

	// increment ref count on group expression
	pgexpr->AddRef();
	m_pgexpr = pgexpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Attach
//
//	@doc:
//		Attach to a given cost context
//
//---------------------------------------------------------------------------
void
CExpressionHandle::Attach
	(
	CCostContext *pcc
	)
{
	GPOS_ASSERT(NULL == m_pcc);
	GPOS_ASSERT(NULL != pcc);

	m_pcc = pcc;
	Attach(pcc->Pgexpr());
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::CopyGroupProps
//
//	@doc:
//		Cache properties of group and its children on the handle
//
//---------------------------------------------------------------------------
void
CExpressionHandle::CopyGroupProps()
{
	GPOS_ASSERT(NULL != m_pgexpr);
	GPOS_ASSERT(NULL == m_pdrgpdp);
	GPOS_ASSERT(NULL == m_pdp);

	// add-ref group properties
	DrvdPropArray *pdp = m_pgexpr->Pgroup()->Pdp();
	pdp->AddRef();
	m_pdp = pdp;

	// add-ref child groups' properties
	const ULONG arity = Arity();
	m_pdrgpdp = GPOS_NEW(m_mp) CDrvdProp2dArray(m_mp, arity);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		DrvdPropArray *pdpChild = (*m_pgexpr)[ul]->Pdp();
		pdpChild->AddRef();
		m_pdrgpdp->Append(pdpChild);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::CopyExprProps
//
//	@doc:
//		Cache properties of expression and its children on the handle
//
//---------------------------------------------------------------------------
void
CExpressionHandle::CopyExprProps()
{
	GPOS_ASSERT(NULL != m_pexpr);
	GPOS_ASSERT(NULL == m_pdrgpdp);
	GPOS_ASSERT(NULL == m_pdp);

	// add-ref expression properties
	DrvdPropArray *pdp = m_pexpr->PdpDerive();
	pdp->AddRef();
	m_pdp = pdp;

	// add-ref child expressions' properties
	const ULONG arity = Arity();
	m_pdrgpdp = GPOS_NEW(m_mp) CDrvdProp2dArray(m_mp, arity);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		DrvdPropArray *pdpChild = (*m_pexpr)[ul]->PdpDerive();
		pdpChild->AddRef();
		m_pdrgpdp->Append(pdpChild);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::CopyCostCtxtProps
//
//	@doc:
//		Cache plan properties of cost context and its children on the handle
//
//---------------------------------------------------------------------------
void
CExpressionHandle::CopyCostCtxtProps()
{
	GPOS_ASSERT(NULL != m_pcc);
	GPOS_ASSERT(NULL == m_pdrgpdp);
	GPOS_ASSERT(NULL == m_pdp);

	// add-ref context properties
	DrvdPropArray *pdp = m_pcc->Pdpplan();
	pdp->AddRef();
	m_pdp = pdp;

	// add-ref child group expressions' properties
	const ULONG arity = Arity();
	m_pdrgpdp = GPOS_NEW(m_mp) CDrvdProp2dArray(m_mp, arity);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CGroup *pgroupChild = (*m_pgexpr)[ul];
		if (!pgroupChild->FScalar())
		{
			COptimizationContext *pocChild = (*m_pcc->Pdrgpoc())[ul];
			GPOS_ASSERT(NULL != pocChild);

			CCostContext *pccChild = pocChild->PccBest();
			GPOS_ASSERT(NULL != pccChild);

			pdp = pccChild->Pdpplan();
			pdp->AddRef();
			m_pdrgpdp->Append(pdp);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::DeriveProps
//
//	@doc:
//		Recursive property derivation
//
//---------------------------------------------------------------------------
void
CExpressionHandle::DeriveProps
	(
	CDrvdPropCtxt *pdpctxt
	)
{
	GPOS_ASSERT(NULL == m_pdrgpdp);
	GPOS_ASSERT(NULL == m_pdp);
	GPOS_CHECK_ABORT;

	if (NULL != m_pgexpr)
	{
		CopyGroupProps();
		return;
	}
	GPOS_ASSERT(NULL != m_pexpr);

	// check if expression already has derived props
	if (NULL != m_pexpr->Pdp(m_pexpr->Ept()))
	{
		// When an expression handle is attached to a group expression, there is no
		// actual work that needs to happen in terms of property derivation. This
		// is because a group expression must originate from a Memo group. At the
		// first time a Memo group is created, the relational/scalar properties are
		// copied from the expression that caused the creation of the group to the
		// property containers of the group itself. So, simply copy the properties
		// from the group to the handle.
		CopyExprProps();
		return;
	}

	// copy stats of attached expression
	CopyStats();

	// extract children's properties
	m_pdrgpdp = GPOS_NEW(m_mp) CDrvdProp2dArray(m_mp);
	const ULONG arity = m_pexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = (*m_pexpr)[ul];
		DrvdPropArray *pdp = pexprChild->PdpDerive(pdpctxt);
		pdp->AddRef();
		m_pdrgpdp->Append(pdp);

		// add child props to derivation context
		CDrvdPropCtxt::AddDerivedProps(pdp, pdpctxt);
	}

	// create/derive local properties
	m_pdp = Pop()->PdpCreate(m_mp);
	m_pdp->Derive(m_mp, *this, pdpctxt);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::PdrgpstatOuterRefs
//
//	@doc:
//		Given an array of stats objects and a child index, return an array
//		of stats objects starting from the first stats object referenced by
//		child
//
//---------------------------------------------------------------------------
IStatisticsArray *
CExpressionHandle::PdrgpstatOuterRefs
	(
	IStatisticsArray *statistics_array,
	ULONG child_index
	)
	const
{
	GPOS_ASSERT(NULL != statistics_array);
	GPOS_ASSERT(child_index < Arity());

	if (FScalarChild(child_index) || !HasOuterRefs(child_index))
	{
		// if child is scalar or has no outer references, return empty array
		return  GPOS_NEW(m_mp) IStatisticsArray(m_mp);
	}

	IStatisticsArray *pdrgpstatResult = GPOS_NEW(m_mp) IStatisticsArray(m_mp);
	CColRefSet *outer_refs = GetRelationalProperties(child_index)->PcrsOuter();
	GPOS_ASSERT(0 < outer_refs->Size());

	const ULONG size = statistics_array->Size();
	ULONG ulStartIndex = gpos::ulong_max;
	for (ULONG ul = 0; ul < size; ul++)
	{
		IStatistics *stats = (*statistics_array)[ul];
		CColRefSet *pcrsStats = stats->GetColRefSet(m_mp);
		BOOL fStatsColsUsed = !outer_refs->IsDisjoint(pcrsStats);
		pcrsStats->Release();
		if (fStatsColsUsed)
		{
			ulStartIndex = ul;
			break;
		}
	}

	if (gpos::ulong_max != ulStartIndex)
	{
		// copy stats starting from index of outer-most stats object referenced by child
		CUtils::AddRefAppend<IStatistics, CleanupStats>(pdrgpstatResult, statistics_array, ulStartIndex);
	}

	return pdrgpstatResult;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::FAttachedToLeafPattern
//
//	@doc:
//		Return True if handle is attached to a leaf pattern
//
//---------------------------------------------------------------------------
BOOL
CExpressionHandle::FAttachedToLeafPattern() const
{
	return
		0 == Arity() &&
		NULL != m_pexpr &&
		NULL != m_pexpr->Pgexpr();
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::DeriveRootStats
//
//	@doc:
//		CheckState derivation at root operator where handle is attached
//
//---------------------------------------------------------------------------
void
CExpressionHandle::DeriveRootStats
	(
	IStatisticsArray *stats_ctxt
	)
{
	GPOS_ASSERT(NULL == m_pstats);

	CLogical *popLogical = CLogical::PopConvert(Pop());
	IStatistics *pstatsRoot = NULL;
	if (FAttachedToLeafPattern())
	{
		// for leaf patterns extracted from memo, trigger state derivation on origin group
		GPOS_ASSERT(NULL != m_pexpr);
		GPOS_ASSERT(NULL != m_pexpr->Pgexpr());

		pstatsRoot = m_pexpr->Pgexpr()->Pgroup()->PstatsRecursiveDerive(m_mp, m_mp, CReqdPropRelational::GetReqdRelationalProps(m_prp), stats_ctxt);
		pstatsRoot->AddRef();
	}
	else
	{
		// otherwise, derive stats using root operator
		pstatsRoot = popLogical->PstatsDerive(m_mp, *this, stats_ctxt);
	}
	GPOS_ASSERT(NULL != pstatsRoot);

	m_pstats = pstatsRoot;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::DeriveStats
//
//	@doc:
//		Recursive stat derivation
//
//---------------------------------------------------------------------------
void
CExpressionHandle::DeriveStats
	(
	IStatisticsArray *stats_ctxt,
	BOOL fComputeRootStats
	)
{
	GPOS_ASSERT(NULL != stats_ctxt);
	GPOS_ASSERT(NULL == m_pdrgpstat);
	GPOS_ASSERT(NULL == m_pstats);
	GPOS_ASSERT(NULL != m_pdrgprp);

	// copy input context
	IStatisticsArray *pdrgpstatCurrentCtxt = GPOS_NEW(m_mp) IStatisticsArray(m_mp);
	CUtils::AddRefAppend<IStatistics, CleanupStats>(pdrgpstatCurrentCtxt, stats_ctxt);

	// create array of children stats
	m_pdrgpstat = GPOS_NEW(m_mp) IStatisticsArray(m_mp);
	ULONG ulMaxChildRisk = 1;
	const ULONG arity = Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		// create a new context for outer references used by current child
		IStatisticsArray *pdrgpstatChildCtxt = PdrgpstatOuterRefs(pdrgpstatCurrentCtxt, ul);

		IStatistics *stats = NULL;
		if (NULL != Pexpr())
		{
			// derive stats recursively on child expression
			stats = (*Pexpr())[ul]->PstatsDerive(GetReqdRelationalProps(ul), pdrgpstatChildCtxt);
		}
		else
		{
			// derive stats recursively on child group
			stats = (*Pgexpr())[ul]->PstatsRecursiveDerive(m_mp, m_mp, GetReqdRelationalProps(ul), pdrgpstatChildCtxt);
		}
		GPOS_ASSERT(NULL != stats);

		// add child stat to current context
		stats->AddRef();
		pdrgpstatCurrentCtxt->Append(stats);
		pdrgpstatChildCtxt->Release();

		// add child stat to children stat array
		stats->AddRef();
		m_pdrgpstat->Append(stats);
		if (stats->StatsEstimationRisk() > ulMaxChildRisk)
		{
			ulMaxChildRisk = stats->StatsEstimationRisk();
		}
	}

	if (fComputeRootStats)
	{
		// call stat derivation on operator to compute local stats
		GPOS_ASSERT(NULL == m_pstats);

		DeriveRootStats(stats_ctxt);
		GPOS_ASSERT(NULL != m_pstats);

		CLogical *popLogical = CLogical::PopConvert(Pop());
		ULONG risk = ulMaxChildRisk;
		if (CStatisticsUtils::IncreasesRisk(popLogical))
		{
			++risk;
		}
		m_pstats->SetStatsEstimationRisk(risk);
	}

	// clean up current stat context
	pdrgpstatCurrentCtxt->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::DeriveCostContextStats
//
//	@doc:
//		Stats derivation based on required plan properties
//
//---------------------------------------------------------------------------
void
CExpressionHandle::DeriveCostContextStats()
{
	GPOS_ASSERT(NULL != m_pcc);
	GPOS_ASSERT(NULL == m_pcc->Pstats());

	// copy group properties and stats
	CopyGroupProps();
	CopyStats();

	if (NULL != m_pstats && !m_pcc->FNeedsNewStats())
	{
		// there is no need to derive stats,
		// stats are copied from owner group

		return;
	}

	CEnfdPartitionPropagation *pepp = m_pcc->Poc()->Prpp()->Pepp();
	COperator *pop = Pop();
	if (CUtils::FPhysicalScan(pop) &&
		CPhysicalScan::PopConvert(pop)->FDynamicScan() &&
		!pepp->PpfmDerived()->IsEmpty())
	{
		// derive stats on dynamic table scan using stats of part selector
		CPhysicalScan *popScan = CPhysicalScan::PopConvert(m_pgexpr->Pop());
		IStatistics *pstatsDS = popScan->PstatsDerive(m_mp, *this, m_pcc->Poc()->Prpp(), m_pcc->Poc()->Pdrgpstat());

		CRefCount::SafeRelease(m_pstats);
		m_pstats = pstatsDS;

		return;
	}

	// release current stats since we will derive new stats
	CRefCount::SafeRelease(m_pstats);
	m_pstats = NULL;

	// load stats from child cost context -- these may be different from child groups stats
	CRefCount::SafeRelease(m_pdrgpstat);
	m_pdrgpstat = NULL;

	m_pdrgpstat = GPOS_NEW(m_mp) IStatisticsArray(m_mp);
	const ULONG arity = m_pcc->Pdrgpoc()->Size();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		COptimizationContext *pocChild = (*m_pcc->Pdrgpoc())[ul];
		CCostContext *pccChild = pocChild->PccBest();
		GPOS_ASSERT(NULL != pccChild);
		GPOS_ASSERT(NULL != pccChild->Pstats());

		pccChild->Pstats()->AddRef();
		m_pdrgpstat->Append(pccChild->Pstats());
	}

	if (CPhysical::PopConvert(m_pgexpr->Pop())->FPassThruStats())
	{
		GPOS_ASSERT(1 == m_pdrgpstat->Size());

		// copy stats from first child
		(*m_pdrgpstat)[0]->AddRef();
		m_pstats = (*m_pdrgpstat)[0];

		return;
	}

	// derive stats using the best logical expression with the same children as attached physical operator
	CGroupExpression *pgexprForStats = m_pcc->PgexprForStats();
	GPOS_ASSERT(NULL != pgexprForStats);

	CExpressionHandle exprhdl(m_mp);
	exprhdl.Attach(pgexprForStats);
	exprhdl.DeriveProps(NULL /*pdpctxt*/);
	m_pdrgpstat->AddRef();
	exprhdl.m_pdrgpstat = m_pdrgpstat;
	exprhdl.ComputeReqdProps(m_pcc->Poc()->GetReqdRelationalProps(), 0 /*ulOptReq*/);

	GPOS_ASSERT(NULL == exprhdl.m_pstats);
	IStatistics *stats = m_pgexpr->Pgroup()->PstatsCompute(m_pcc->Poc(), exprhdl, pgexprForStats);

	// copy stats to main handle
	GPOS_ASSERT(NULL == m_pstats);
	GPOS_ASSERT(NULL != stats);

	stats->AddRef();
	m_pstats = stats;

	GPOS_ASSERT(m_pstats != NULL);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::DeriveStats
//
//	@doc:
//		CheckState derivation using given properties and context
//
//---------------------------------------------------------------------------
void
CExpressionHandle::DeriveStats
	(
	IMemoryPool *pmpLocal,
	IMemoryPool *pmpGlobal,
	CReqdPropRelational *prprel,
	IStatisticsArray *stats_ctxt
	)
{
	CReqdPropRelational *prprelNew = prprel;
	if (NULL == prprelNew)
	{
		// create empty property container
		CColRefSet *pcrs = GPOS_NEW(pmpGlobal) CColRefSet(pmpGlobal);
		prprelNew = GPOS_NEW(pmpGlobal) CReqdPropRelational(pcrs);
	}
	else
	{
		prprelNew->AddRef();
	}

	IStatisticsArray *pdrgpstatCtxtNew = stats_ctxt;
	if (NULL == stats_ctxt)
	{
		// create empty context
		pdrgpstatCtxtNew = GPOS_NEW(pmpGlobal) IStatisticsArray(pmpGlobal);
	}
	else
	{
		pdrgpstatCtxtNew->AddRef();
	}

	if (NULL != Pgexpr())
	{
		(void) Pgexpr()->Pgroup()->PstatsRecursiveDerive(pmpLocal, pmpGlobal, prprelNew, pdrgpstatCtxtNew);
	}
	else
	{
		GPOS_ASSERT(NULL != Pexpr());

		(void) Pexpr()->PstatsDerive(prprelNew, pdrgpstatCtxtNew);
	}

	prprelNew->Release();
	pdrgpstatCtxtNew->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::DerivePlanProps
//
//	@doc:
//		Derive the properties of the plan carried by attached cost context
//
//---------------------------------------------------------------------------
void
CExpressionHandle::DerivePlanProps
	(
	CDrvdPropCtxtPlan *pdpctxtplan
	)
{
	GPOS_ASSERT(NULL != m_pcc);
	GPOS_ASSERT(NULL != m_pgexpr);
	GPOS_ASSERT(NULL == m_pdrgpdp);
	GPOS_ASSERT(NULL == m_pdp);
	GPOS_CHECK_ABORT;

	// check if properties have been already derived
	if (NULL != m_pcc->Pdpplan())
	{
		CopyCostCtxtProps();
		return;
	}
	GPOS_ASSERT(NULL != pdpctxtplan);

	// extract children's properties
	m_pdrgpdp = GPOS_NEW(m_mp) CDrvdProp2dArray(m_mp);
	const ULONG arity = m_pcc->Pdrgpoc()->Size();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		COptimizationContext *pocChild = (*m_pcc->Pdrgpoc())[ul];
		CDrvdPropPlan *pdpplan = pocChild->PccBest()->Pdpplan();
		GPOS_ASSERT(NULL != pdpplan);

		pdpplan->AddRef();
		m_pdrgpdp->Append(pdpplan);

		// add child props to derivation context
		CDrvdPropCtxt::AddDerivedProps(pdpplan, pdpctxtplan);
	}

	COperator *pop = m_pgexpr->Pop();
	if (COperator::EopPhysicalCTEConsumer == pop->Eopid())
	{
		// copy producer plan properties to passed derived plan properties context
		ULONG ulCTEId = CPhysicalCTEConsumer::PopConvert(pop)->UlCTEId();
		CDrvdPropPlan *pdpplan = m_pcc->Poc()->Prpp()->Pcter()->Pdpplan(ulCTEId);
		if (NULL != pdpplan)
		{
			pdpctxtplan->CopyCTEProducerProps(pdpplan, ulCTEId);
		}
	}

	// set the number of expected partition selectors in the context
	pdpctxtplan->SetExpectedPartitionSelectors(pop, m_pcc);

	// create/derive local properties
	m_pdp = Pop()->PdpCreate(m_mp);
	m_pdp->Derive(m_mp, *this, pdpctxtplan);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::DerivePlanProps
//
//	@doc:
//		Derive the properties of the plan carried by attached cost context
//
//---------------------------------------------------------------------------
void
CExpressionHandle::DerivePlanProps()
{
	CDrvdPropCtxtPlan *pdpctxtplan = GPOS_NEW(m_mp) CDrvdPropCtxtPlan(m_mp);

	// copy stats
	CopyStats();

	DerivePlanProps(pdpctxtplan);
	pdpctxtplan->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::InitReqdProps
//
//	@doc:
//		Init required properties containers
//
//
//---------------------------------------------------------------------------
void
CExpressionHandle::InitReqdProps
	(
	CReqdProp *prpInput
	)
{
	GPOS_ASSERT(NULL != prpInput);
	GPOS_ASSERT(NULL == m_prp);
	GPOS_ASSERT(NULL == m_pdrgprp);

	// set required properties of attached expr/gexpr
	m_prp = prpInput;
	m_prp->AddRef();
	
	if (m_prp->FPlan())
	{
		CReqdPropPlan *prpp = CReqdPropPlan::Prpp(prpInput);
		if (NULL == prpp->Pepp())
		{
			CPartInfo *ppartinfo = GetRelationalProperties()->Ppartinfo();
			prpp->InitReqdPartitionPropagation(m_mp, ppartinfo);
		}
	}
	
	// compute required properties of children
	m_pdrgprp = GPOS_NEW(m_mp) CReqdPropArray(m_mp);

	// initialize array with input requirements,
	// the initial requirements are only place holders in the array
	// and they are replaced when computing the requirements of each child
	const ULONG arity = Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		m_prp->AddRef();
		m_pdrgprp->Append(m_prp);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::ComputeChildReqdProps
//
//	@doc:
//		Compute required properties of the n-th child
//
//
//---------------------------------------------------------------------------
void
CExpressionHandle::ComputeChildReqdProps
	(
	ULONG child_index,
	CDrvdProp2dArray *pdrgpdpCtxt,
	ULONG ulOptReq
	)
{
	GPOS_ASSERT(NULL != m_prp);
	GPOS_ASSERT(NULL != m_pdrgprp);
	GPOS_ASSERT(m_pdrgprp->Size() == Arity());
	GPOS_ASSERT(child_index < m_pdrgprp->Size() && "uninitialized required child properties");
	GPOS_CHECK_ABORT;

	CReqdProp *prp = m_prp;
	if (FScalarChild(child_index))
	{
		// use local reqd properties to fill scalar child entry in children array
		prp->AddRef();
	}
	else
	{
		// compute required properties based on child type
		prp = Pop()->PrpCreate(m_mp);
		prp->Compute(m_mp, *this, m_prp, child_index, pdrgpdpCtxt, ulOptReq);
	}

	// replace required properties of given child
	m_pdrgprp->Replace(child_index, prp);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::CopyChildReqdProps
//
//	@doc:
//		Copy required properties of the n-th child
//
//
//---------------------------------------------------------------------------
void
CExpressionHandle::CopyChildReqdProps
	(
	ULONG child_index,
	CReqdProp *prp
	)
{
	GPOS_ASSERT(NULL != prp);
	GPOS_ASSERT(NULL != m_pdrgprp);
	GPOS_ASSERT(m_pdrgprp->Size() == Arity());
	GPOS_ASSERT(child_index < m_pdrgprp->Size() && "uninitialized required child properties");

	m_pdrgprp->Replace(child_index, prp);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::ComputeChildReqdCols
//
//	@doc:
//		Compute required columns of the n-th child
//
//
//---------------------------------------------------------------------------
void
CExpressionHandle::ComputeChildReqdCols
	(
	ULONG child_index,
	CDrvdProp2dArray *pdrgpdpCtxt
	)
{
	GPOS_ASSERT(NULL != m_prp);
	GPOS_ASSERT(NULL != m_pdrgprp);
	GPOS_ASSERT(m_pdrgprp->Size() == Arity());
	GPOS_ASSERT(child_index < m_pdrgprp->Size() && "uninitialized required child properties");

	CReqdProp *prp = m_prp;
	if (FScalarChild(child_index))
	{
		// use local reqd properties to fill scalar child entry in children array
		prp->AddRef();
	}
	else
	{
		// compute required columns
		prp = Pop()->PrpCreate(m_mp);
		CReqdPropPlan::Prpp(prp)->ComputeReqdCols(m_mp, *this, m_prp, child_index, pdrgpdpCtxt);
	}

	// replace required properties of given child
	m_pdrgprp->Replace(child_index, prp);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::ComputeReqdProps
//
//	@doc:
//		Set required properties of attached expr/gexpr, and compute required
//		properties of all children
//
//---------------------------------------------------------------------------
void
CExpressionHandle::ComputeReqdProps
	(
	CReqdProp *prpInput,
	ULONG ulOptReq
	)
{
	InitReqdProps(prpInput);
	const ULONG arity = Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		ComputeChildReqdProps(ul, NULL /*pdrgpdpCtxt*/, ulOptReq);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::FScalarChild
//
//	@doc:
//		Check if a given child is a scalar expression/group
//
//---------------------------------------------------------------------------
BOOL
CExpressionHandle::FScalarChild
	(
	ULONG child_index
	)
	const
{
	if (NULL != Pexpr())
	{
		return (*Pexpr())[child_index]->Pop()->FScalar();
	}

	GPOS_ASSERT(NULL != Pgexpr());

	return (*Pgexpr())[child_index]->FScalar();
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Arity
//
//	@doc:
//		Return number of children of attached expression/group expression
//
//---------------------------------------------------------------------------
ULONG
CExpressionHandle::Arity() const
{
	if (NULL != Pexpr())
	{
		return Pexpr()->Arity();
	}

	GPOS_ASSERT(NULL != Pgexpr());

	return Pgexpr()->Arity();
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::UlLastNonScalarChild
//
//	@doc:
//		Return the index of the last non-scalar child. This is only valid if
//		Arity() is greater than 0
//
//---------------------------------------------------------------------------
ULONG
CExpressionHandle::UlLastNonScalarChild() const
{
	const ULONG arity = Arity();
	if (0 == arity)
	{
		return gpos::ulong_max;
	}

	ULONG ulLastNonScalarChild = arity - 1;
	while (0 < ulLastNonScalarChild && FScalarChild(ulLastNonScalarChild))
	{
		ulLastNonScalarChild --;
	}

	if (!FScalarChild(ulLastNonScalarChild))
	{
		// we need to check again that index points to a non-scalar child
		// since operator's children may be all scalar (e.g. index-scan)
		return ulLastNonScalarChild;
	}

	return gpos::ulong_max;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::UlFirstNonScalarChild
//
//	@doc:
//		Return the index of the first non-scalar child. This is only valid if
//		Arity() is greater than 0
//
//---------------------------------------------------------------------------
ULONG
CExpressionHandle::UlFirstNonScalarChild() const
{
	const ULONG arity = Arity();
	if (0 == arity)
	{
		return gpos::ulong_max;
	}

	ULONG ulFirstNonScalarChild = 0;
	while (ulFirstNonScalarChild  < arity - 1 && FScalarChild(ulFirstNonScalarChild))
	{
		ulFirstNonScalarChild ++;
	}

	if (!FScalarChild(ulFirstNonScalarChild))
	{
		// we need to check again that index points to a non-scalar child
		// since operator's children may be all scalar (e.g. index-scan)
		return ulFirstNonScalarChild;
	}

	return gpos::ulong_max;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::UlNonScalarChildren
//
//	@doc:
//		Return number of non-scalar children
//
//---------------------------------------------------------------------------
ULONG
CExpressionHandle::UlNonScalarChildren() const
{
	const ULONG arity = Arity();
	ULONG ulNonScalarChildren = 0;
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (!FScalarChild(ul))
		{
			ulNonScalarChildren++;
		}
	}

	return ulNonScalarChildren;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::GetRelationalProperties
//
//	@doc:
//		Retrieve derived relational props of n-th child;
//		Assumes caller knows what properties to ask for;
//
//---------------------------------------------------------------------------
CDrvdPropRelational *
CExpressionHandle::GetRelationalProperties
	(
	ULONG child_index
	)
	const
{
	if (NULL != Pexpr() && NULL == m_pdrgpdp)
	{
		// handle is used for required property computation
		if (Pexpr()->Pop()->FPhysical())
		{
			// relational props were copied from memo, return props directly
			return CDrvdPropRelational::GetRelationalProperties((*Pexpr())[child_index]->Pdp(DrvdPropArray::EptRelational));
		}

		// return props after calling derivation function
		return CDrvdPropRelational::GetRelationalProperties((*Pexpr())[child_index]->PdpDerive());
	}

	if (FAttachedToLeafPattern())
	{
		GPOS_ASSERT(NULL != Pexpr());
		GPOS_ASSERT(NULL != Pexpr()->Pgexpr());

		// handle is attached to a leaf pattern, get relational props from child group
		return CDrvdPropRelational::GetRelationalProperties((*Pexpr()->Pgexpr())[child_index]->Pdp());
	}

	if (NULL != m_pcc)
	{
		// handle is used for deriving plan properties, get relational props from child group
		return CDrvdPropRelational::GetRelationalProperties((*Pgexpr())[child_index]->Pdp());
	}

	GPOS_ASSERT(child_index < m_pdrgpdp->Size());

	DrvdPropArray *pdp = (*m_pdrgpdp)[child_index];

	return CDrvdPropRelational::GetRelationalProperties(pdp);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::GetRelationalProperties
//
//	@doc:
//		Retrieve relational properties of attached expr/gexpr;
//
//---------------------------------------------------------------------------
CDrvdPropRelational *
CExpressionHandle::GetRelationalProperties() const
{
	if (NULL != Pexpr())
	{
		if (Pexpr()->Pop()->FPhysical())
		{
			// relational props were copied from memo, return props directly
			return CDrvdPropRelational::GetRelationalProperties(Pexpr()->Pdp(DrvdPropArray::EptRelational));
		}
		// return props after calling derivation function
		return CDrvdPropRelational::GetRelationalProperties(Pexpr()->PdpDerive());
	}

	if (NULL != m_pcc)
	{
		// get relational props from group
		return CDrvdPropRelational::GetRelationalProperties(Pgexpr()->Pgroup()->Pdp());
	}

	return CDrvdPropRelational::GetRelationalProperties(m_pdp);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Pstats
//
//	@doc:
//		Return derived stats of n-th child
//
//---------------------------------------------------------------------------
IStatistics *
CExpressionHandle::Pstats
	(
	ULONG child_index
	)
	const
{
	GPOS_ASSERT(child_index < m_pdrgpstat->Size());

	return (*m_pdrgpstat)[child_index];
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Pdpplan
//
//	@doc:
//		Retrieve derived plan props of n-th child;
//		Assumes caller knows what properties to ask for;
//
//---------------------------------------------------------------------------
CDrvdPropPlan *
CExpressionHandle::Pdpplan
	(
	ULONG child_index
	)
	const
{
	GPOS_ASSERT(child_index < m_pdrgpdp->Size());

	DrvdPropArray *pdp = (*m_pdrgpdp)[child_index];

	return CDrvdPropPlan::Pdpplan(pdp);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::GetDrvdScalarProps
//
//	@doc:
//		Retrieve derived scalar props of n-th child;
//		Assumes caller knows what properties to ask for;
//
//---------------------------------------------------------------------------
CDrvdPropScalar *
CExpressionHandle::GetDrvdScalarProps
	(
	ULONG child_index
	)
	const
{
	if (NULL != Pexpr() && NULL == m_pdrgpdp)
	{
		// handle is used for required property computation
		DrvdPropArray *pdp = (*Pexpr())[child_index]->PdpDerive();
		return CDrvdPropScalar::GetDrvdScalarProps(pdp);
	}

	if (FAttachedToLeafPattern())
	{
		GPOS_ASSERT(NULL != Pexpr());
		GPOS_ASSERT(NULL != Pexpr()->Pgexpr());

		// handle is attached to a leaf pattern, get scalar props from child group
		return CDrvdPropScalar::GetDrvdScalarProps((*Pexpr()->Pgexpr())[child_index]->Pdp());
	}

	if (NULL != m_pcc)
	{
		// handle is used for deriving plan properties, get scalar props from child group
		return CDrvdPropScalar::GetDrvdScalarProps((*Pgexpr())[child_index]->Pdp());
	}

	GPOS_ASSERT(child_index < m_pdrgpdp->Size());

	DrvdPropArray *pdp = (*m_pdrgpdp)[child_index];

	return CDrvdPropScalar::GetDrvdScalarProps(pdp);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::GetReqdRelationalProps
//
//	@doc:
//		Retrieve required relational props of n-th child;
//		Assumes caller knows what properties to ask for;
//
//---------------------------------------------------------------------------
CReqdPropRelational *
CExpressionHandle::GetReqdRelationalProps
	(
	ULONG child_index
	)
	const
{
	GPOS_ASSERT(child_index < m_pdrgprp->Size());

	CReqdProp *prp = (*m_pdrgprp)[child_index];
	GPOS_ASSERT(prp->FRelational() && "Unexpected property type");

	return CReqdPropRelational::GetReqdRelationalProps(prp);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Prpp
//
//	@doc:
//		Retrieve required relational props of n-th child;
//		Assumes caller knows what properties to ask for;
//
//---------------------------------------------------------------------------
CReqdPropPlan *
CExpressionHandle::Prpp
	(
	ULONG child_index
	)
	const
{
	GPOS_ASSERT(child_index < m_pdrgprp->Size());

	CReqdProp *prp = (*m_pdrgprp)[child_index];
	GPOS_ASSERT(prp->FPlan() && "Unexpected property type");

	return CReqdPropPlan::Prpp(prp);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Pop
//
//	@doc:
//		Get operator from handle
//
//---------------------------------------------------------------------------
COperator *
CExpressionHandle::Pop() const
{
	if (NULL != m_pexpr)
	{
		GPOS_ASSERT(NULL == m_pgexpr);

		return m_pexpr->Pop();
	}

	if (NULL != m_pgexpr)
	{
		return m_pgexpr->Pop();
	}
	
	GPOS_ASSERT(!"Handle was not attached properly");
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Pop
//
//	@doc:
//		Get child operator from handle
//
//---------------------------------------------------------------------------
COperator *
CExpressionHandle::Pop
	(
	ULONG child_index
	)
	const
{
	GPOS_ASSERT(child_index < Arity());

	if (NULL != m_pexpr)
	{
		GPOS_ASSERT(NULL == m_pgexpr);

		return (*m_pexpr)[child_index]->Pop();
	}

	if (NULL != m_pcc)
	{
		COptimizationContext *pocChild = (*m_pcc->Pdrgpoc())[child_index];
		GPOS_ASSERT(NULL != pocChild);

		CCostContext *pccChild = pocChild->PccBest();
		GPOS_ASSERT(NULL != pccChild);

		return pccChild->Pgexpr()->Pop();
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::DeriveProducerStats
//
//	@doc:
//		If the child (child_index) is a CTE consumer, then derive is corresponding
//		producer statistics.
//
//---------------------------------------------------------------------------
void
CExpressionHandle::DeriveProducerStats
	(
	ULONG child_index,
	CColRefSet *pcrsStats
	)
{
	// check to see if there are any CTE consumers in the group whose properties have
	// to be pushed to its corresponding CTE producer
	CGroupExpression *pgexpr = Pgexpr();
	if (NULL != pgexpr)
	{
		CGroup *pgroupChild = (*pgexpr)[child_index];
		if (pgroupChild->FHasAnyCTEConsumer())
		{
			CGroupExpression *pgexprCTEConsumer = pgroupChild->PgexprAnyCTEConsumer();
			CLogicalCTEConsumer *popConsumer = CLogicalCTEConsumer::PopConvert(pgexprCTEConsumer->Pop());
			COptCtxt::PoctxtFromTLS()->Pcteinfo()->DeriveProducerStats(popConsumer, pcrsStats);
		}

		return;
	}

	// statistics are also derived on expressions representing the producer that may have
	// multiple CTE consumers. We should ensure that their properties are to pushed to their
	// corresponding CTE producer
	CExpression *pexpr = Pexpr();
	if (NULL != pexpr)
	{
		CExpression *pexprChild = (*pexpr)[child_index];
		if (COperator::EopLogicalCTEConsumer == pexprChild->Pop()->Eopid())
		{
			CLogicalCTEConsumer *popConsumer = CLogicalCTEConsumer::PopConvert(pexprChild->Pop());
			COptCtxt::PoctxtFromTLS()->Pcteinfo()->DeriveProducerStats(popConsumer, pcrsStats);
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::PexprScalarChild
//
//	@doc:
//		Get the scalar child at given index
//
//---------------------------------------------------------------------------
CExpression *
CExpressionHandle::PexprScalarChild
	(
	ULONG child_index
	)
	const
{
	GPOS_ASSERT(child_index < Arity());

	if (NULL != m_pgexpr)
	{
		// access scalar expression cached on the child scalar group
		GPOS_ASSERT((*m_pgexpr)[child_index]->FScalar());

		CExpression *pexprScalar = (*m_pgexpr)[child_index]->PexprScalar();
		GPOS_ASSERT_IMP(NULL == pexprScalar, CDrvdPropScalar::GetDrvdScalarProps((*m_pgexpr)[child_index]->Pdp())->FHasSubquery());

		return pexprScalar;
	}

	if (NULL != m_pexpr && NULL != (*m_pexpr)[child_index]->Pgexpr())
	{
		// if the expression does not come from a group, but its child does then
		// get the scalar child from that group
		CGroupExpression *pgexpr = (*m_pexpr)[child_index]->Pgexpr();
		CExpression *pexprScalar = pgexpr->Pgroup()->PexprScalar();
		GPOS_ASSERT_IMP(NULL == pexprScalar, CDrvdPropScalar::GetDrvdScalarProps((*m_pexpr)[child_index]->PdpDerive())->FHasSubquery());

		return pexprScalar;
	}

	// access scalar expression from the child expression node
	GPOS_ASSERT((*m_pexpr)[child_index]->Pop()->FScalar());

	return (*m_pexpr)[child_index];
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::PexprScalar
//
//	@doc:
//		Get the scalar expression attached to handle,
//		return NULL if handle is not attached to a scalar expression
//
//---------------------------------------------------------------------------
CExpression *
CExpressionHandle::PexprScalar() const
{
	if (!Pop()->FScalar())
	{
		return NULL;
	}

	if (NULL != m_pexpr)
	{
		return m_pexpr;
	}

	if (NULL != m_pgexpr)
	{
		return m_pgexpr->Pgroup()->PexprScalar();
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::PfpChild
//
//	@doc:
//		Retrieve derived function props of n-th child;
//
//---------------------------------------------------------------------------
CFunctionProp *
CExpressionHandle::PfpChild
	(
	ULONG child_index
	)
	const
{
	if (FScalarChild(child_index))
	{
		return GetDrvdScalarProps(child_index)->Pfp();
	}

	return GetRelationalProperties(child_index)->Pfp();
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::FChildrenHaveVolatileFuncScan
//
//	@doc:
//		Check whether an expression's children have a volatile function
//
//---------------------------------------------------------------------------
BOOL
CExpressionHandle::FChildrenHaveVolatileFuncScan() const
{
	const ULONG arity = Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (PfpChild(ul)->FHasVolatileFunctionScan())
		{
			return true;
		}
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::UlFirstOptimizedChildIndex
//
//	@doc:
//		Return the index of first child to be optimized
//
//---------------------------------------------------------------------------
ULONG
CExpressionHandle::UlFirstOptimizedChildIndex() const
{
	const ULONG arity = Arity();
	GPOS_ASSERT(0 < arity);

	CPhysical::EChildExecOrder eceo = CPhysical::PopConvert(Pop())->Eceo();
	if (CPhysical::EceoRightToLeft == eceo)
	{
		return arity - 1;
	}
	GPOS_ASSERT(CPhysical::EceoLeftToRight == eceo);

	return 0;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::UlLastOptimizedChildIndex
//
//	@doc:
//		Return the index of last child to be optimized
//
//---------------------------------------------------------------------------
ULONG
CExpressionHandle::UlLastOptimizedChildIndex() const
{
	const ULONG arity = Arity();
	GPOS_ASSERT(0 < arity);

	CPhysical::EChildExecOrder eceo = CPhysical::PopConvert(Pop())->Eceo();
	if (CPhysical::EceoRightToLeft == eceo)
	{
		return 0;
	}
	GPOS_ASSERT(CPhysical::EceoLeftToRight == eceo);

	return arity - 1;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::UlNextOptimizedChildIndex
//
//	@doc:
//		Return the index of child to be optimized next to the given child,
//		return gpos::ulong_max if there is no next child index
//
//
//---------------------------------------------------------------------------
ULONG
CExpressionHandle::UlNextOptimizedChildIndex
	(
	ULONG child_index
	)
	const
{
	CPhysical::EChildExecOrder eceo = CPhysical::PopConvert(Pop())->Eceo();

	ULONG ulNextChildIndex = gpos::ulong_max;
	if (CPhysical::EceoRightToLeft == eceo)
	{
		if (0 < child_index)
		{
			ulNextChildIndex = child_index - 1;
		}
	}
	else
	{
		GPOS_ASSERT(CPhysical::EceoLeftToRight == eceo);

		if (Arity() - 1 > child_index)
		{
			ulNextChildIndex = child_index + 1;
		}
	}

	return ulNextChildIndex;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::UlPreviousOptimizedChildIndex
//
//	@doc:
//		Return the index of child optimized before the given child,
//		return gpos::ulong_max if there is no previous child index
//
//
//---------------------------------------------------------------------------
ULONG
CExpressionHandle::UlPreviousOptimizedChildIndex
	(
	ULONG child_index
	)
	const
{
	CPhysical::EChildExecOrder eceo = CPhysical::PopConvert(Pop())->Eceo();

	ULONG ulPrevChildIndex = gpos::ulong_max;
	if (CPhysical::EceoRightToLeft == eceo)
	{
		if (Arity() - 1 > child_index)
		{
			ulPrevChildIndex = child_index + 1;
		}
	}
	else
	{
		GPOS_ASSERT(CPhysical::EceoLeftToRight == eceo);

		if (0 < child_index)
		{
			ulPrevChildIndex = child_index - 1;
		}
	}

	return ulPrevChildIndex;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::FNextChildIndex
//
//	@doc:
//		Get next child index based on child optimization order, return
//		true if such index could be found
//
//---------------------------------------------------------------------------
BOOL
CExpressionHandle::FNextChildIndex
	(
	ULONG *pulChildIndex
	)
	const
{
	GPOS_ASSERT(NULL != pulChildIndex);

	const ULONG arity = Arity();
	if (0 == arity)
	{
		// operator does not have children
		return false;
	}

	ULONG ulNextChildIndex = UlNextOptimizedChildIndex(*pulChildIndex);
	if (gpos::ulong_max == ulNextChildIndex)
	{
		return false;
	}
	*pulChildIndex = ulNextChildIndex;

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::PcrsUsedColumns
//
//	@doc:
//		Return the columns used by a logical operator and all its scalar children
//
//---------------------------------------------------------------------------
CColRefSet *
CExpressionHandle::PcrsUsedColumns
	(
	IMemoryPool *mp
	)
{
	COperator *pop = Pop();
	GPOS_ASSERT(pop->FLogical());

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);

	// get columns used by the operator itself
	pcrs->Include(CLogical::PopConvert(pop)->PcrsLocalUsed());

	// get columns used by the scalar children
	const ULONG arity = Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (FScalarChild(ul))
		{
			pcrs->Include(GetDrvdScalarProps(ul)->PcrsUsed());
		}
	}

	return pcrs;
}

// EOF
