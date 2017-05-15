//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		COptimizationContext.cpp
//
//	@doc:
//		Implementation of optimization context
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CEnfdOrder.h"
#include "gpopt/base/COptimizationContext.h"
#include "gpopt/base/COrderSpec.h"
#include "gpopt/search/CGroupExpression.h"

#include "gpopt/operators/CPhysicalAgg.h"
#include "gpopt/operators/CPhysicalMotion.h"
#include "gpopt/operators/CPhysicalSort.h"
#include "gpopt/operators/CPhysicalNLJoin.h"
#include "gpopt/xforms/CXformUtils.h"


using namespace gpopt;


// invalid optimization context
const COptimizationContext COptimizationContext::m_ocInvalid;

// invalid optimization context pointer
const OPTCTXT_PTR COptimizationContext::m_pocInvalid = NULL;



//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::~COptimizationContext
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
COptimizationContext::~COptimizationContext()
{
	CRefCount::SafeRelease(m_prpp);
	CRefCount::SafeRelease(m_prprel);
	CRefCount::SafeRelease(m_pdrgpstatCtxt);
}


//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::PgexprBest
//
//	@doc:
//		Best group expression accessor
//
//---------------------------------------------------------------------------
CGroupExpression *
COptimizationContext::PgexprBest() const
{
	if (NULL == m_pccBest)
	{
		return NULL;
	}

	return m_pccBest->Pgexpr();
}


//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::SetBest
//
//	@doc:
//		 Set best cost context
//
//---------------------------------------------------------------------------
void
COptimizationContext::SetBest
	(
	CCostContext *pcc
	)
{
	GPOS_ASSERT(NULL != pcc);

	m_pccBest = pcc;

	COperator *pop = pcc->Pgexpr()->Pop();
	if (CUtils::FPhysicalAgg(pop) && CPhysicalAgg::PopConvert(pop)->FMultiStage())
	{
		m_fHasMultiStageAggPlan = true;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::FMatch
//
//	@doc:
//		Match against another context
//
//---------------------------------------------------------------------------
BOOL
COptimizationContext::FMatch
	(
	const COptimizationContext *poc
	)
	const
{
	GPOS_ASSERT(NULL != poc);

	if (m_pgroup != poc->Pgroup() || m_ulSearchStageIndex != poc->UlSearchStageIndex())
	{
		return false;
	}

	CReqdPropPlan *prppFst = this->Prpp();
	CReqdPropPlan *prppSnd = poc->Prpp();

	// make sure we are not comparing to invalid context
	if (NULL == prppFst|| NULL == prppSnd)
	{
		return NULL == prppFst && NULL == prppSnd;
	}

	return prppFst->FEqual(prppSnd);
}


//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::FEqualForStats
//
//	@doc:
//		Equality function used for computing stats during costing
//
//---------------------------------------------------------------------------
BOOL
COptimizationContext::FEqualForStats
	(
	const COptimizationContext *pocLeft,
	const COptimizationContext *pocRight
	)
{
	GPOS_ASSERT(NULL != pocLeft);
	GPOS_ASSERT(NULL != pocRight);

	return
		pocLeft->Prprel()->PcrsStat()->FEqual(pocRight->Prprel()->PcrsStat()) &&
		pocLeft->Pdrgpstat()->FEqual(pocRight->Pdrgpstat()) &&
		pocLeft->Prpp()->Pepp()->PpfmDerived()->FEqual(pocRight->Prpp()->Pepp()->PpfmDerived());
}


//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::FOptimize
//
//	@doc:
//		Return true if given group expression should be optimized under
//		given context
//
//---------------------------------------------------------------------------
BOOL
COptimizationContext::FOptimize
	(
	IMemoryPool *pmp,
	CGroupExpression *pgexprParent,
	CGroupExpression *pgexprChild,
	COptimizationContext *pocChild,
	ULONG ulSearchStages
	)
{
	COperator *pop = pgexprChild->Pop();

	if (CUtils::FPhysicalMotion(pop))
	{
		return FOptimizeMotion(pmp, pgexprParent, pgexprChild, pocChild, ulSearchStages);
	}

	if (COperator::EopPhysicalSort == pop->Eopid())
	{
		return FOptimizeSort(pmp, pgexprParent, pgexprChild, pocChild, ulSearchStages);
	}

	if (CUtils::FPhysicalAgg(pop))
	{
		return FOptimizeAgg(pmp, pgexprParent, pgexprChild, pocChild, ulSearchStages);
	}

	if (CUtils::FNLJoin(pop))
	{
		return FOptimizeNLJoin(pmp, pgexprParent, pgexprChild, pocChild, ulSearchStages);
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::FEqualIds
//
//	@doc:
//		Compare array of optimization contexts based on context ids
//
//---------------------------------------------------------------------------
BOOL
COptimizationContext::FEqualContextIds
	(
	DrgPoc *pdrgpocFst,
	DrgPoc *pdrgpocSnd
	)
{
	if (NULL == pdrgpocFst || NULL == pdrgpocSnd)
	{
		return (NULL == pdrgpocFst && NULL == pdrgpocSnd);
	}

	const ULONG ulCtxts = pdrgpocFst->UlLength();
	if (ulCtxts != pdrgpocSnd->UlLength())
	{
		return false;
	}

	BOOL fEqual = true;
	for (ULONG ul = 0; fEqual && ul < ulCtxts; ul++)
	{
		fEqual = (*pdrgpocFst)[ul]->UlId() == (*pdrgpocSnd)[ul]->UlId();
	}

	return fEqual;
}


//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::FOptimizeMotion
//
//	@doc:
//		Check if a Motion node should be optimized for the given context
//
//---------------------------------------------------------------------------
BOOL
COptimizationContext::FOptimizeMotion
	(
	IMemoryPool *,  // pmp
	CGroupExpression *, // pgexprParent
	CGroupExpression *pgexprMotion,
	COptimizationContext *poc,
	ULONG // ulSearchStages
	)
{
	GPOS_ASSERT(NULL != pgexprMotion);
	GPOS_ASSERT(NULL != poc);
	GPOS_ASSERT(CUtils::FPhysicalMotion(pgexprMotion->Pop()));

	CPhysicalMotion *pop = CPhysicalMotion::PopConvert(pgexprMotion->Pop());

	return poc->Prpp()->Ped()->FCompatible(pop->Pds());
}


//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::FOptimizeSort
//
//	@doc:
//		Check if a Sort node should be optimized for the given context
//
//---------------------------------------------------------------------------
BOOL
COptimizationContext::FOptimizeSort
	(
	IMemoryPool *, // pmp
	CGroupExpression *, // pgexprParent
	CGroupExpression *pgexprSort,
	COptimizationContext *poc,
	ULONG // ulSearchStages
	)
{
	GPOS_ASSERT(NULL != pgexprSort);
	GPOS_ASSERT(NULL != poc);
	GPOS_ASSERT(COperator::EopPhysicalSort == pgexprSort->Pop()->Eopid());

	CPhysicalSort *pop = CPhysicalSort::PopConvert(pgexprSort->Pop());

	return poc->Prpp()->Peo()->FCompatible(const_cast<COrderSpec *>(pop->Pos()));
}


//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::FOptimizeAgg
//
//	@doc:
//		Check if Agg node should be optimized for the given context
//
//---------------------------------------------------------------------------
BOOL
COptimizationContext::FOptimizeAgg
	(
	IMemoryPool *pmp,
	CGroupExpression *, // pgexprParent
	CGroupExpression *pgexprAgg,
	COptimizationContext *poc,
	ULONG ulSearchStages
	)
{
	GPOS_ASSERT(NULL != pgexprAgg);
	GPOS_ASSERT(NULL != poc);
	GPOS_ASSERT(CUtils::FPhysicalAgg(pgexprAgg->Pop()));
	GPOS_ASSERT(0 < ulSearchStages);

	if (GPOS_FTRACE(EopttraceForceExpandedMDQAs))
	{
		BOOL fHasMultipleDistinctAggs = CDrvdPropScalar::Pdpscalar((*pgexprAgg)[1]->Pdp())->FHasMultipleDistinctAggs();
		if (fHasMultipleDistinctAggs)
		{
			// do not optimize plans with MDQAs, since preference is for plans with expanded MDQAs
			return false;
		}
	}

	if (!GPOS_FTRACE(EopttraceForceMultiStageAgg))
	{
		// no preference for multi-stage agg, we always proceed with optimization
		return true;
	}

	// otherwise, we need to avoid optimizing node unless it is a multi-stage agg
	COptimizationContext *pocFound = pgexprAgg->Pgroup()->PocLookupBest(pmp, ulSearchStages, poc->Prpp());
	if (NULL != pocFound && pocFound->FHasMultiStageAggPlan())
	{
		// context already has a multi-stage agg plan, optimize child only if it is also a multi-stage agg
		return CPhysicalAgg::PopConvert(pgexprAgg->Pop())->FMultiStage();
	}

	// child context has no plan yet, return true
	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalNLJoin::FOptimizeNLJoin
//
//	@doc:
//		Check if NL join node should be optimized for the given context
//
//---------------------------------------------------------------------------
BOOL
COptimizationContext::FOptimizeNLJoin
	(
	IMemoryPool *pmp,
	CGroupExpression *, // pgexprParent
	CGroupExpression *pgexprJoin,
	COptimizationContext *poc,
	ULONG // ulSearchStages
	)
{
	GPOS_ASSERT(NULL != pgexprJoin);
	GPOS_ASSERT(NULL != poc);
	GPOS_ASSERT(CUtils::FNLJoin(pgexprJoin->Pop()));

	COperator *pop = pgexprJoin->Pop();
	if (!CUtils::FCorrelatedNLJoin(pop))
	{
		return true;
	}

	// for correlated join, the requested columns must be covered by outer child
	// columns and columns to be generated from inner child
	CPhysicalNLJoin *popNLJoin = CPhysicalNLJoin::PopConvert(pop);
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp, popNLJoin->PdrgPcrInner());
	CColRefSet *pcrsOuterChild = CDrvdPropRelational::Pdprel((*pgexprJoin)[0]->Pdp())->PcrsOutput();
	pcrs->Include(pcrsOuterChild);
	BOOL fIncluded = pcrs->FSubset(poc->Prpp()->PcrsRequired());
	pcrs->Release();

	return fIncluded;
}


//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::PrppCTEProducer
//
//	@doc:
//		Compute required properties to CTE producer based on plan properties
//		of CTE consumer
//
//---------------------------------------------------------------------------
CReqdPropPlan *
COptimizationContext::PrppCTEProducer
	(
	IMemoryPool *pmp,
	COptimizationContext *poc,
	ULONG ulSearchStages
	)
{
	GPOS_ASSERT(NULL != poc);
	GPOS_ASSERT(NULL != poc->PccBest());

	CCostContext *pccBest = poc->PccBest();
	CGroupExpression *pgexpr = pccBest->Pgexpr();
	BOOL fOptimizeCTESequence =
			(
			COperator::EopPhysicalSequence == pgexpr->Pop()->Eopid() &&
			(*pgexpr)[0]->FHasCTEProducer()
			);

	if (!fOptimizeCTESequence)
	{
		// best group expression is not a CTE sequence
		return NULL;
	}

	COptimizationContext *pocProducer = (*pgexpr)[0]->PocLookupBest(pmp, ulSearchStages, (*pccBest->Pdrgpoc())[0]->Prpp());
	if (NULL == pocProducer)
	{
		return NULL;
	}

	CCostContext *pccProducer = pocProducer->PccBest();
	if (NULL == pccProducer)
	{
		return NULL;
	}
	COptimizationContext *pocConsumer = (*pgexpr)[1]->PocLookupBest(pmp, ulSearchStages, (*pccBest->Pdrgpoc())[1]->Prpp());
	if (NULL == pocConsumer)
	{
		return NULL;
	}

	CCostContext *pccConsumer = pocConsumer->PccBest();
	if (NULL == pccConsumer)
	{
		return NULL;
	}

	CColRefSet *pcrsInnerOutput = CDrvdPropRelational::Pdprel((*pgexpr)[1]->Pdp())->PcrsOutput();
	CPhysicalCTEProducer *popProducer = CPhysicalCTEProducer::PopConvert(pccProducer->Pgexpr()->Pop());
	HMUlCr *phmulcr = COptCtxt::PoctxtFromTLS()->Pcteinfo()->PhmulcrConsumerToProducer(pmp, popProducer->UlCTEId(), pcrsInnerOutput, popProducer->Pdrgpcr());
	CReqdPropPlan *prppProducer = CReqdPropPlan::PrppRemap(pmp, pocProducer->Prpp(), pccConsumer->Pdpplan(), phmulcr);
	phmulcr->Release();

	if (prppProducer->FEqual(pocProducer->Prpp()))
	{
		prppProducer->Release();

		return NULL;
	}

	return prppProducer;
}


//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
COptimizationContext::OsPrint
	(
	IOstream &os,
	const CHAR *szPrefix
	)
	const
{

	os << szPrefix << m_ulId << " (stage " << m_ulSearchStageIndex << "): (" << *m_prpp <<  ") => Best Expr:";
	if (NULL != PgexprBest())
	{
		os << PgexprBest()->UlId();
	}
	os << std::endl;

	return os;
}

#ifdef GPOS_DEBUG
void
COptimizationContext::DbgPrint()
{
	CAutoTraceFlag atf(EopttracePrintOptimizationContext, true);
	CAutoTrace at(m_pmp);
	(void) this->OsPrint(at.Os(), " ");
}
#endif // GPOS_DEBUG

// EOF

