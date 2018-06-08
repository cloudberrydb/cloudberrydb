//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CJoinOrder.cpp
//
//	@doc:
//		Implementation of join order logic
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "gpos/common/clibwrapper.h"
#include "gpos/common/CBitSet.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/xforms/CJoinOrder.h"


using namespace gpopt;

			
//---------------------------------------------------------------------------
//	@function:
//		ICmpEdgesByLength
//
//	@doc:
//		Comparison function for simple join ordering: sort edges by length
//		only to guaranteed that single-table predicates don't end up above 
//		joins;
//
//---------------------------------------------------------------------------
INT ICmpEdgesByLength
	(
	const void *pvOne,
	const void *pvTwo
	)
{
	CJoinOrder::SEdge *pedgeOne = *(CJoinOrder::SEdge**)pvOne;
	CJoinOrder::SEdge *pedgeTwo = *(CJoinOrder::SEdge**)pvTwo;

	
	INT iDiff = (pedgeOne->m_pbs->CElements() - pedgeTwo->m_pbs->CElements());
	if (0 == iDiff)
	{
		return (INT)pedgeOne->m_pbs->UlHash() - (INT)pedgeTwo->m_pbs->UlHash();
	}
		
	return iDiff;
}

	
//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::SComponent::SComponent
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrder::SComponent::SComponent
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
	:
	m_pbs(NULL),
	m_pexpr(pexpr),
	m_fUsed(false)
{	
	m_pbs = GPOS_NEW(pmp) CBitSet(pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::SComponent::SComponent
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrder::SComponent::SComponent
	(
	CExpression *pexpr,
	CBitSet *pbs
	)
	:
	m_pbs(pbs),
	m_pexpr(pexpr),
	m_fUsed(false)
{
	GPOS_ASSERT(NULL != pbs);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::SComponent::~SComponent
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrder::SComponent::~SComponent()
{	
	m_pbs->Release();
	CRefCount::SafeRelease(m_pexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::SComponent::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrder::SComponent::OsPrint
	(
	IOstream &os
	)
const
{
	CBitSet *pbs = m_pbs;
	os 
		<< "Component: ";
	os
		<< (*pbs) << std::endl;
	os
		<< *m_pexpr << std::endl;
		
	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::SEdge::SEdge
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrder::SEdge::SEdge
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
	:
	m_pbs(NULL),
	m_pexpr(pexpr),
	m_fUsed(false)
{	
	m_pbs = GPOS_NEW(pmp) CBitSet(pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::SEdge::~SEdge
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrder::SEdge::~SEdge()
{	
	m_pbs->Release();
	m_pexpr->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::SEdge::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrder::SEdge::OsPrint
	(
	IOstream &os
	)
	const
{
	return os 
		<< "Edge : " 
		<< (*m_pbs) << std::endl 
		<< *m_pexpr << std::endl;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::CJoinOrder
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrder::CJoinOrder
	(
	IMemoryPool *pmp,
	DrgPexpr *pdrgpexpr,
	DrgPexpr *pdrgpexprConj
	)
	:
	m_pmp(pmp),
	m_rgpedge(NULL),
	m_ulEdges(0),
	m_rgpcomp(NULL),
	m_ulComps(0)
{
	typedef SComponent* Pcomp;
	typedef SEdge* Pedge;
	
	m_ulComps = pdrgpexpr->UlLength();
	m_rgpcomp = GPOS_NEW_ARRAY(pmp, Pcomp, m_ulComps);
	
	for (ULONG ul = 0; ul < m_ulComps; ul++)
	{
		CExpression *pexprComp = (*pdrgpexpr)[ul];
		pexprComp->AddRef();
		m_rgpcomp[ul] = GPOS_NEW(pmp) SComponent(pmp, pexprComp);
		
		// component always covers itself
		(void) m_rgpcomp[ul]->m_pbs->FExchangeSet(ul);
	}

	m_ulEdges = pdrgpexprConj->UlLength();
	m_rgpedge = GPOS_NEW_ARRAY(pmp, Pedge, m_ulEdges);
	
	for (ULONG ul = 0; ul < m_ulEdges; ul++)
	{
		CExpression *pexprEdge = (*pdrgpexprConj)[ul];
		pexprEdge->AddRef();
		m_rgpedge[ul] = GPOS_NEW(pmp) SEdge(pmp, pexprEdge);
	}
	
	pdrgpexpr->Release();
	pdrgpexprConj->Release();
	
	ComputeEdgeCover();
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::~CJoinOrder
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrder::~CJoinOrder()
{
	for (ULONG ul = 0; ul < m_ulComps; ul++)
	{
		m_rgpcomp[ul]->Release();
	}
	GPOS_DELETE_ARRAY(m_rgpcomp);

	for (ULONG ul = 0; ul < m_ulEdges; ul++)
	{
		m_rgpedge[ul]->Release();
	}
	GPOS_DELETE_ARRAY(m_rgpedge);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::ComputeEdgeCover
//
//	@doc:
//		Compute cover for each edge
//
//---------------------------------------------------------------------------
void
CJoinOrder::ComputeEdgeCover()
{
	for (ULONG ulEdge = 0; ulEdge < m_ulEdges; ulEdge++)
	{
		CExpression *pexpr = m_rgpedge[ulEdge]->m_pexpr;
		CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexpr->PdpDerive())->PcrsUsed();

		for (ULONG ulComp = 0; ulComp < m_ulComps; ulComp++)
		{
			CExpression *pexprComp = m_rgpcomp[ulComp]->m_pexpr;
			CColRefSet *pcrsOutput = CDrvdPropRelational::Pdprel(pexprComp->PdpDerive())->PcrsOutput();

			if (!pcrsUsed->FDisjoint(pcrsOutput))
			{
				(void) m_rgpedge[ulEdge]->m_pbs->FExchangeSet(ulComp);
			}
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::PcompCombine
//
//	@doc:
//		Combine the two given components using applicable edges
//
//
//---------------------------------------------------------------------------
CJoinOrder::SComponent *
CJoinOrder::PcompCombine
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
//		CJoinOrder::DeriveStats
//
//	@doc:
//		Helper function to derive stats on a given component
//
//---------------------------------------------------------------------------
void
CJoinOrder::DeriveStats
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	if (NULL != pexpr->Pstats())
	{
		// stats have been already derived
		return;
	}

	CExpressionHandle exprhdl(m_pmp);
	exprhdl.Attach(pexpr);
	exprhdl.DeriveStats(m_pmp, m_pmp, NULL /*prprel*/, NULL /*pdrgpstatCtxt*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::OsPrint
//
//	@doc:
//		Helper function to print a join order class
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrder::OsPrint
	(
	IOstream &os
	)
	const
{
	os	
		<< "Join Order: " << std::endl
		<< "Edges: " << m_ulEdges << std::endl;
		
	for (ULONG ul = 0; ul < m_ulEdges; ul++)
	{
		m_rgpedge[ul]->OsPrint(os);
		os << std::endl;
	}

	os << "Components: " << m_ulComps << std::endl;
	for (ULONG ul = 0; ul < m_ulComps; ul++)
	{
		os << (void*)m_rgpcomp[ul] << " - " << std::endl;
		m_rgpcomp[ul]->OsPrint(os);
	}
	
	return os;
}


// EOF
