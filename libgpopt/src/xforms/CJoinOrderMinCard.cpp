//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CJoinOrderMinCard.cpp
//
//	@doc:
//		Implementation of cardinality-based join order generation
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "gpos/common/clibwrapper.h"
#include "gpos/common/CBitSet.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/xforms/CJoinOrderMinCard.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderMinCard::CJoinOrderMinCard
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrderMinCard::CJoinOrderMinCard
	(
	IMemoryPool *pmp,
	DrgPexpr *pdrgpexprComponents,
	DrgPexpr *pdrgpexprConjuncts
	)
	:
	CJoinOrder(pmp, pdrgpexprComponents, pdrgpexprConjuncts),
	m_pcompResult(NULL)
{
#ifdef GPOS_DEBUG
	for (ULONG ul = 0; ul < m_ulComps; ul++)
	{
		GPOS_ASSERT(NULL != m_rgpcomp[ul]->m_pexpr->Pstats() &&
				"stats were not derived on input component");
	}
#endif // GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderMinCard::~CJoinOrderMinCard
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderMinCard::~CJoinOrderMinCard()
{
	CRefCount::SafeRelease(m_pcompResult);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderMinCard::PcompCombine
//
//	@doc:
//		Combine the two given components using applicable edges
//
//
//---------------------------------------------------------------------------
CJoinOrder::SComponent *
CJoinOrderMinCard::PcompCombine
	(
	SComponent *pcompOuter,
	SComponent *pcompInner
	)
{
	CBitSet *pbs = GPOS_NEW(m_pmp) CBitSet(m_pmp);
	pbs->Union(pcompOuter->m_pbs);
	pbs->Union(pcompInner->m_pbs);
	DrgPexpr *pdrgpexpr = GPOS_NEW(m_pmp) DrgPexpr(m_pmp);
	for (ULONG ul = 0; ul < m_ulEdges; ul++)
	{
		SEdge *pedge = m_rgpedge[ul];
		if (pedge->m_fUsed)
		{
			// edge is already used in result component
			continue;
		}

		if (pbs->FSubset(pedge->m_pbs))
		{
			// edge is subsumed by the cover of the combined component
			CExpression *pexpr = pedge->m_pexpr;
			pexpr->AddRef();
			pdrgpexpr->Append(pexpr);
		}
	}

	CExpression *pexprOuter = pcompOuter->m_pexpr;
	CExpression *pexprInner = pcompInner->m_pexpr;
	CExpression *pexprScalar = CPredicateUtils::PexprConjunction(m_pmp, pdrgpexpr);

	CExpression *pexpr = NULL;
	if (NULL == pexprOuter)
	{
		// first call to this function, we create a Select node
		pexpr = CUtils::PexprCollapseSelect(m_pmp, pexprInner, pexprScalar);
		pexprScalar->Release();
	}
	else
	{
		// not first call, we create an Inner Join
		pexprInner->AddRef();
		pexprOuter->AddRef();
		pexpr = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(m_pmp, pexprOuter, pexprInner, pexprScalar);
	}

	return GPOS_NEW(m_pmp) SComponent(pexpr, pbs);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderMinCard::MarkUsedEdges
//
//	@doc:
//		Mark edges used by result component
//
//---------------------------------------------------------------------------
void
CJoinOrderMinCard::MarkUsedEdges()
{
	GPOS_ASSERT(NULL != m_pcompResult);

	CExpression *pexpr = m_pcompResult->m_pexpr;
	COperator::EOperatorId eopid = pexpr->Pop()->Eopid();
	if (0 == pexpr->UlArity() ||
		(COperator::EopLogicalSelect != eopid && COperator::EopLogicalInnerJoin != eopid))
	{
		// result component does not have a scalar child, e.g. a Get node
		return;
	}

	CExpression *pexprScalar = (*pexpr) [pexpr->UlArity() - 1];
	DrgPexpr *pdrgpexpr = CPredicateUtils::PdrgpexprConjuncts(m_pmp, pexprScalar);
	const ULONG ulSize = pdrgpexpr->UlLength();

	for (ULONG ulEdge = 0; ulEdge < m_ulEdges; ulEdge++)
	{
		SEdge *pedge = m_rgpedge[ulEdge];
		if (pedge->m_fUsed)
		{
			continue;
		}

		for (ULONG ulPred = 0; ulPred < ulSize; ulPred++)
		{
			if ((*pdrgpexpr)[ulPred] == pedge->m_pexpr)
			{
				pedge->m_fUsed = true;
			}
		}
	}
	pdrgpexpr->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderMinCard::DeriveStats
//
//	@doc:
//		Helper function to derive stats on a given component
//
//---------------------------------------------------------------------------
void
CJoinOrderMinCard::DeriveStats
	(
	IMemoryPool *pmp,
	SComponent *pcomp
	)
{
	GPOS_ASSERT(NULL != pcomp);
	GPOS_ASSERT(NULL != pcomp->m_pexpr);

	CExpression *pexpr = pcomp->m_pexpr;
	if (NULL != pexpr->Pstats())
	{
		// stats have been already derived
		return;
	}

	CExpressionHandle exprhdl(pmp);
	exprhdl.Attach(pexpr);
	exprhdl.DeriveStats(pmp, pmp, NULL /*prprel*/, NULL /*pdrgpstatCtxt*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderMinCard::PexprExpand
//
//	@doc:
//		Create join order
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderMinCard::PexprExpand()
{
	GPOS_ASSERT(NULL == m_pcompResult && "join order is already expanded");

	m_pcompResult = GPOS_NEW(m_pmp) SComponent(m_pmp, NULL /*pexpr*/);
	ULONG ulCoveredComps = 0;
	while (ulCoveredComps < m_ulComps)
	{
		CDouble dMinRows(0.0);
		SComponent *pcompBest = NULL; // best component to be added to current result
		SComponent *pcompBestResult = NULL; // result after adding best component

		for (ULONG ul = 0; ul < m_ulComps; ul++)
		{
			SComponent *pcompCurrent = m_rgpcomp[ul];
			if (pcompCurrent->m_fUsed)
			{
				// used components are already included in current result
				continue;
			}

			// combine component with current result and derive stats
			CJoinOrder::SComponent *pcompTemp = PcompCombine(m_pcompResult, pcompCurrent);
			DeriveStats(m_pmp, pcompTemp);
			CDouble dRows = pcompTemp->m_pexpr->Pstats()->DRows();

			if (NULL == pcompBestResult || dRows < dMinRows)
			{
				pcompBest = pcompCurrent;
				dMinRows = dRows;
				pcompTemp->AddRef();
				CRefCount::SafeRelease(pcompBestResult);
				pcompBestResult = pcompTemp;
			}
			pcompTemp->Release();
		}
		GPOS_ASSERT(NULL != pcompBestResult);

		// mark best component as used
		pcompBest->m_fUsed = true;
		m_pcompResult->Release();
		m_pcompResult = pcompBestResult;

		// mark used edges to avoid including them multiple times
		MarkUsedEdges();
		ulCoveredComps++;
	}
	GPOS_ASSERT(NULL != m_pcompResult->m_pexpr);

	CExpression *pexprResult = m_pcompResult->m_pexpr;
	pexprResult->AddRef();

	return pexprResult;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderMinCard::OsPrint
//
//	@doc:
//		Print created join order
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrderMinCard::OsPrint
	(
	IOstream &os
	)
	const
{
	if (NULL != m_pcompResult->m_pexpr)
	{
		os << *m_pcompResult->m_pexpr;
	}

	return os;
}

// EOF
