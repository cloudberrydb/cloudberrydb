//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDecorrelator.cpp
//
//	@doc:
//		Implementation of decorrelation logic
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CPredicateUtils.h"

#include "gpopt/xforms/CDecorrelator.h"

#include "naucrates/md/IMDScalarOp.h"


using namespace gpopt;

// initialization of handlers array
const CDecorrelator::SOperatorProcessor CDecorrelator::m_rgopproc[] =
{
	{COperator::EopLogicalSelect, FProcessSelect},
	{COperator::EopLogicalGbAgg, FProcessGbAgg},
	{COperator::EopLogicalInnerJoin, FProcessJoin},
	{COperator::EopLogicalInnerCorrelatedApply, FProcessJoin},
	{COperator::EopLogicalLeftSemiJoin, FProcessJoin},
	{COperator::EopLogicalLeftSemiCorrelatedApplyIn, FProcessJoin},
	{COperator::EopLogicalLeftAntiSemiJoin, FProcessJoin},
	{COperator::EopLogicalLeftOuterJoin, FProcessJoin},
	{COperator::EopLogicalLeftOuterCorrelatedApply, FProcessJoin},
	{COperator::EopLogicalNAryJoin, FProcessJoin},
	{COperator::EopLogicalProject, FProcessProject},
	{COperator::EopLogicalSequenceProject, FProcessProject},
	{COperator::EopLogicalAssert, FProcessAssert},
	{COperator::EopLogicalMaxOneRow, FProcessMaxOneRow},
	{COperator::EopLogicalLimit, FProcessLimit},
};


//---------------------------------------------------------------------------
//	@function:
//		CDecorrelator::FPullableCorrelations
//
//	@doc:
//		Helper to check if correlations below join are valid to be pulled-up
//
//---------------------------------------------------------------------------
BOOL
CDecorrelator::FPullableCorrelations
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	DrgPexpr *pdrgpexprChildren,
	DrgPexpr *pdrgpexprCorrelations
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pdrgpexprChildren);
	GPOS_ASSERT(NULL != pdrgpexprCorrelations);

	// check for pullable semi join correlations
	COperator::EOperatorId eopid = pexpr->Pop()->Eopid();
	BOOL fSemiJoin =
		COperator::EopLogicalLeftSemiJoin == eopid ||
		COperator::EopLogicalLeftAntiSemiJoin == eopid ||
		CUtils::FCorrelatedApply(pexpr->Pop());
	GPOS_ASSERT_IMP(fSemiJoin, 2 == pdrgpexprChildren->UlLength());

	if (fSemiJoin)
	{
		return CPredicateUtils::FValidSemiJoinCorrelations(pmp, (*pdrgpexprChildren)[0], (*pdrgpexprChildren)[1], pdrgpexprCorrelations);
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CDecorrelator::FDelayableScalarOp
//
//	@doc:
//		Check if scalar operator can be delayed
//
//---------------------------------------------------------------------------
BOOL
CDecorrelator::FDelayableScalarOp
	(
	CExpression *pexprScalar
	)
{
	GPOS_ASSERT(NULL != pexprScalar);

	// check operator
	COperator::EOperatorId eopidScalar = pexprScalar->Pop()->Eopid();
	switch(eopidScalar)
	{
		case COperator::EopScalarIdent:
			return true;

		case COperator::EopScalarCast:
			return CScalarIdent::FCastedScId(pexprScalar);

		case COperator::EopScalarCmp:
			return CPredicateUtils::FEquality(pexprScalar);

		default:
			return false;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDecorrelator::FDelayable
//
//	@doc:
//		Check if predicate can be delayed
//
//---------------------------------------------------------------------------
BOOL
CDecorrelator::FDelayable
	(
	CExpression *pexprLogical, // logical parent of predicate tree
	CExpression *pexprScalar,
	BOOL fEqualityOnly
	)
{
	GPOS_CHECK_STACK_SIZE;

	GPOS_ASSERT(NULL != pexprLogical);
	GPOS_ASSERT(pexprLogical->Pop()->FLogical());
	GPOS_ASSERT(NULL != pexprScalar);
	GPOS_ASSERT(pexprScalar->Pop()->FScalar());
	
	BOOL fDelay = true;

	COperator::EOperatorId eopid = pexprLogical->Pop()->Eopid();
 	if (COperator::EopLogicalLeftSemiJoin == eopid || COperator::EopLogicalLeftAntiSemiJoin == eopid)
 	{
 		// for semi-joins, we disallow predicates referring to inner child to be pulled above the join
 		CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexprScalar->PdpDerive())->PcrsUsed();
 		CColRefSet *pcrsInner = CDrvdPropRelational::Pdprel((*pexprLogical)[1]->PdpDerive())->PcrsOutput();
 		if (!pcrsUsed->FDisjoint(pcrsInner))
 		{
 			// predicate uses a column produced by semi-join inner child

 			fDelay = false;
 		}
 	}
	
	if (fDelay && fEqualityOnly)
	{
		// check operator 
		fDelay = FDelayableScalarOp(pexprScalar);
	}

	// check its children
	const ULONG ulArity = pexprScalar->UlArity();
	for (ULONG ul = 0; ul < ulArity && fDelay; ul++)
	{
		fDelay = FDelayable(pexprLogical, (*pexprScalar)[ul], fEqualityOnly);
	}

	return fDelay;
}


//---------------------------------------------------------------------------
//	@function:
//		CDecorrelator::FProcess
//
//	@doc:
//		Main driver for decorrelation of expression
//
//---------------------------------------------------------------------------
BOOL
CDecorrelator::FProcess
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	BOOL fEqualityOnly,
	CExpression **ppexprDecorrelated,
	DrgPexpr *pdrgpexprCorrelations
	)
{
	GPOS_CHECK_STACK_SIZE;

	// only correlated Apply can be encountered here
	GPOS_ASSERT_IMP(CUtils::FApply(pexpr->Pop()), CUtils::FCorrelatedApply(pexpr->Pop()) &&
			"Apply expression is encountered by decorrelator");

	CDrvdPropRelational *pdprel = CDrvdPropRelational::Pdprel(pexpr->PdpDerive());

	// no outer references?
	if (0 == pdprel->PcrsOuter()->CElements())
	{
		pexpr->AddRef();
		*ppexprDecorrelated = pexpr;
		return true;
	}
	
	BOOL fSuccess = FProcessOperator(pmp, pexpr, fEqualityOnly, ppexprDecorrelated, pdrgpexprCorrelations);

	// in case of success make sure there are no outer references left
	GPOS_ASSERT_IMP
		(
		fSuccess,
		0 == CDrvdPropRelational::Pdprel((*ppexprDecorrelated)->PdpDerive())->PcrsOuter()->CElements()
		);

	return fSuccess;
}	


//---------------------------------------------------------------------------
//	@function:
//		CDecorrelator::FProcessOperator
//
//	@doc:
//		Router for different operators to process
//
//---------------------------------------------------------------------------
BOOL
CDecorrelator::FProcessOperator
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	BOOL fEqualityOnly,
	CExpression **ppexprDecorrelated,
	DrgPexpr *pdrgpexprCorrelations
	)
{
	FnProcessor *pfnp = NULL;
	COperator::EOperatorId eopid = pexpr->Pop()->Eopid();
	const ULONG ulSize = GPOS_ARRAY_SIZE(m_rgopproc);
	// find the handler corresponding to the given operator
	for (ULONG ul = 0; pfnp == NULL && ul < ulSize; ul++)
	{
		if (eopid == m_rgopproc[ul].m_eopid)
		{
			pfnp = m_rgopproc[ul].m_pfnp;
		}
	}
	
	if (NULL != pfnp)
	{
		// subqueries must be processed before reaching here
		const ULONG ulArity = pexpr->UlArity();
		if ((*pexpr)[ulArity - 1]->Pop()->FScalar() && CUtils::FHasSubquery((*pexpr)[ulArity - 1]))
		{
			return false;
		}

		// check for abort before descending into recursion
		GPOS_CHECK_ABORT;

		return pfnp(pmp, pexpr, fEqualityOnly, ppexprDecorrelated, pdrgpexprCorrelations);
	}
	
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CDecorrelator::FProcessPredicate
//
//	@doc:
//		Decorrelate predicate
//
//---------------------------------------------------------------------------
BOOL
CDecorrelator::FProcessPredicate
	(
	IMemoryPool *pmp,
	CExpression *pexprLogical, // logical parent of predicate tree
	CExpression *pexprScalar,
	BOOL fEqualityOnly,
	CColRefSet *pcrsOutput,
	CExpression **ppexprDecorrelated,
	DrgPexpr *pdrgpexprCorrelations
	)
{
	GPOS_ASSERT(pexprLogical->Pop()->FLogical());
	GPOS_ASSERT(pexprScalar->Pop()->FScalar());

	*ppexprDecorrelated = NULL;
	
	DrgPexpr *pdrgpexprConj = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprScalar);
	DrgPexpr *pdrgpexprResiduals = GPOS_NEW(pmp) DrgPexpr(pmp);
	BOOL fSuccess = true;
	
	// divvy up the predicates in residuals (w/ no outer ref) and correlations (w/ outer refs)
	ULONG ulLength = pdrgpexprConj->UlLength();
	for(ULONG ul = 0; ul < ulLength && fSuccess; ul++)
	{
		CExpression *pexprConj = (*pdrgpexprConj)[ul];
		CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexprConj->PdpDerive())->PcrsUsed();
		
		if (pcrsOutput->FSubset(pcrsUsed))
		{
			// no outer ref
			pexprConj->AddRef();
			pdrgpexprResiduals->Append(pexprConj);			

			continue;
		}
		
		fSuccess = FDelayable(pexprLogical, pexprConj, fEqualityOnly);
		if (fSuccess)
		{
			pexprConj->AddRef();
			pdrgpexprCorrelations->Append(pexprConj);
		}
		
	}

	pdrgpexprConj->Release();

	if (!fSuccess || 0 == pdrgpexprResiduals->UlLength())
	{
		// clean up
		pdrgpexprResiduals->Release();
	}
	else
	{
		// residuals become new predicate
		*ppexprDecorrelated = CPredicateUtils::PexprConjunction(pmp, pdrgpexprResiduals);
	}


	return fSuccess;
}


//---------------------------------------------------------------------------
//	@function:
//		CDecorrelator::FProcessSelect
//
//	@doc:
//		Decorrelate select operator
//
//---------------------------------------------------------------------------
BOOL
CDecorrelator::FProcessSelect
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	BOOL fEqualityOnly,
	CExpression **ppexprDecorrelated,
	DrgPexpr *pdrgpexprCorrelations
	)
{
	GPOS_ASSERT(COperator::EopLogicalSelect == pexpr->Pop()->Eopid());

	// decorrelate relational child
	CExpression *pexprRelational = NULL;
	if (!FProcess(pmp, (*pexpr)[0], fEqualityOnly, &pexprRelational, pdrgpexprCorrelations))
	{
		GPOS_ASSERT(NULL == pexprRelational);
		return false;
	}

	// process predicate
	CExpression *pexprPredicate = NULL;
	CColRefSet *pcrsOutput = CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->PcrsOutput();
	BOOL fSuccess  = FProcessPredicate(pmp, pexpr, (*pexpr)[1], fEqualityOnly, pcrsOutput, &pexprPredicate, pdrgpexprCorrelations);
	
	// build substitute
	if (fSuccess)
	{
		if (NULL != pexprPredicate)
		{
			CLogicalSelect *popSelect = CLogicalSelect::PopConvert(pexpr->Pop());
			popSelect->AddRef();
			
			*ppexprDecorrelated = GPOS_NEW(pmp) CExpression(pmp, popSelect, pexprRelational, pexprPredicate);
		}
		else 
		{
			*ppexprDecorrelated = pexprRelational;
		}

	}
	else
	{
		pexprRelational->Release();
	}
	
	return fSuccess;
}


//---------------------------------------------------------------------------
//	@function:
//		CDecorrelator::FProcessGbAgg
//
//	@doc:
//		Decorrelate GbAgg operator
//
//---------------------------------------------------------------------------
BOOL
CDecorrelator::FProcessGbAgg
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	BOOL, // fEqualityOnly
	CExpression **ppexprDecorrelated,
	DrgPexpr *pdrgpexprCorrelations
	)
{
	CLogicalGbAgg *popAggOriginal = CLogicalGbAgg::PopConvert(pexpr->Pop());
	
	// fail if agg has outer references
	if (CUtils::FHasOuterRefs(pexpr) && !CUtils::FHasOuterRefs((*pexpr)[0]))
	{
		return false;
	}

	// TODO: 12/20/2012 - ; check for strictness of agg function

	// decorrelate relational child
	CExpression *pexprRelational = NULL;
	if (!FProcess(pmp, (*pexpr)[0], true /*fEqualityOnly*/, &pexprRelational, pdrgpexprCorrelations))
	{
		GPOS_ASSERT(NULL == pexprRelational);
		return false;
	}
	
	// get the output columns of decorrelated child
	CColRefSet *pcrsOutput = CDrvdPropRelational::Pdprel(pexprRelational->PdpDerive())->PcrsOutput();

	// create temp expression of correlations to determine inner columns
	pdrgpexprCorrelations->AddRef();
	CExpression *pexprTemp = CPredicateUtils::PexprConjunction(pmp, pdrgpexprCorrelations);
	CColRefSet *pcrs = 
		GPOS_NEW(pmp) CColRefSet(pmp,
			*(CDrvdPropScalar::Pdpscalar(pexprTemp->PdpDerive())->PcrsUsed()));
		
	pcrs->Intersection(pcrsOutput);
	pexprTemp->Release();

	// add grouping columns from original agg
	pcrs->Include(popAggOriginal->Pdrgpcr());

	// assemble grouping columns
	DrgPcr *pdrgpcr = pcrs->Pdrgpcr(pmp);
	pcrs->Release();
	
	// assemble agg
	CExpression *pexprProjList = (*pexpr)[1];
	pexprProjList->AddRef();
	CLogicalGbAgg *popAgg = GPOS_NEW(pmp) CLogicalGbAgg(pmp, pdrgpcr, popAggOriginal->Egbaggtype());
	*ppexprDecorrelated = GPOS_NEW(pmp) CExpression(pmp, popAgg, pexprRelational, pexprProjList);
	
	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CDecorrelator::FProcessJoin
//
//	@doc:
//		Decorrelate a join expression;
//
//---------------------------------------------------------------------------
BOOL
CDecorrelator::FProcessJoin
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	BOOL fEqualityOnly,
	CExpression **ppexprDecorrelated,
	DrgPexpr *pdrgpexprCorrelations
	)
{
	GPOS_ASSERT(CUtils::FLogicalJoin(pexpr->Pop()) || CUtils::FApply(pexpr->Pop()));

	ULONG ulArity = pexpr->UlArity();	
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp, ulArity);
	CColRefSet *pcrsOutput = GPOS_NEW(pmp) CColRefSet(pmp);

	// decorrelate all relational children
	for (ULONG ul = 0; ul < ulArity - 1; ul++)
	{
		CExpression *pexprInput = NULL;
		if (FProcess(pmp, (*pexpr)[ul], fEqualityOnly, &pexprInput, pdrgpexprCorrelations))
		{
			pdrgpexpr->Append(pexprInput);
			pcrsOutput->Union(CDrvdPropRelational::Pdprel(pexprInput->PdpDerive())->PcrsOutput());
		}
		else
		{
			pdrgpexpr->Release();
			pcrsOutput->Release();
			
			return false;
		}
	}

	// check for valid semi join correlations
	if (!FPullableCorrelations(pmp, pexpr, pdrgpexpr, pdrgpexprCorrelations))
	{
		pdrgpexpr->Release();
		pcrsOutput->Release();

		return false;
	 }

	// decorrelate predicate and build new join operator
	CExpression *pexprPredicate = NULL;
	BOOL fSuccess = FProcessPredicate(pmp, pexpr, (*pexpr)[ulArity - 1], fEqualityOnly, pcrsOutput, &pexprPredicate, pdrgpexprCorrelations);
	pcrsOutput->Release();

	if (fSuccess)
	{
		// in case entire predicate is being deferred, plug in a 'true'
		if (NULL == pexprPredicate)
		{
			pexprPredicate = CUtils::PexprScalarConstBool(pmp, true /*fVal*/);
		}
		
		pdrgpexpr->Append(pexprPredicate);
		
		COperator *pop = pexpr->Pop();
		pop->AddRef();
		*ppexprDecorrelated = GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexpr);
	}
	else
	{
		pdrgpexpr->Release();
		CRefCount::SafeRelease(pexprPredicate);
	}
	
	return fSuccess;
}


//---------------------------------------------------------------------------
//	@function:
//		CDecorrelator::FProcessAssert
//
//	@doc:
//		Decorrelate assert operator
//
//---------------------------------------------------------------------------
BOOL
CDecorrelator::FProcessAssert
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	BOOL fEqualityOnly,
	CExpression **ppexprDecorrelated,
	DrgPexpr *pdrgpexprCorrelations
	)
{
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	GPOS_ASSERT(COperator::EopLogicalAssert == pop->Eopid());

	CExpression *pexprScalar = (*pexpr)[1];

	// fail if assert expression has outer references
	CColRefSet *pcrsOutput = CDrvdPropRelational::Pdprel((*pexpr)[0]->PdpDerive())->PcrsOutput();
	CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexprScalar->PdpDerive())->PcrsUsed();
	if (!pcrsOutput->FSubset(pcrsUsed))
	{
		return false;
	}

	// decorrelate relational child
	CExpression *pexprRelational = NULL;
	if (!FProcess(pmp, (*pexpr)[0], fEqualityOnly, &pexprRelational, pdrgpexprCorrelations))
	{
		GPOS_ASSERT(NULL == pexprRelational);
		return false;
	}

	// assemble new project
	pop->AddRef();
	pexprScalar->AddRef();
	*ppexprDecorrelated = GPOS_NEW(pmp) CExpression(pmp, pop, pexprRelational, pexprScalar);

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CDecorrelator::FProcessMaxOneRow
//
//	@doc:
//		Decorrelate max one row operator
//
//---------------------------------------------------------------------------
BOOL
CDecorrelator::FProcessMaxOneRow
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	BOOL fEqualityOnly,
	CExpression **ppexprDecorrelated,
	DrgPexpr *pdrgpexprCorrelations
	)
{
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	GPOS_ASSERT(COperator::EopLogicalMaxOneRow == pop->Eopid());

	// fail if MaxOneRow expression has outer references
	if (CUtils::FHasOuterRefs(pexpr))
	{
		return false;
	}

	// decorrelate relational child
	CExpression *pexprRelational = NULL;
	if (!FProcess(pmp, (*pexpr)[0], fEqualityOnly, &pexprRelational, pdrgpexprCorrelations))
	{
		GPOS_ASSERT(NULL == pexprRelational);
		return false;
	}

	// assemble new project
	pop->AddRef();
	*ppexprDecorrelated = GPOS_NEW(pmp) CExpression(pmp, pop, pexprRelational);

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CDecorrelator::FProcessProject
//
//	@doc:
//		Decorrelate project/sequence project
//
//---------------------------------------------------------------------------
BOOL
CDecorrelator::FProcessProject
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	BOOL fEqualityOnly,
	CExpression **ppexprDecorrelated,
	DrgPexpr *pdrgpexprCorrelations
	)
{
	COperator::EOperatorId eopid = pexpr->Pop()->Eopid();

	GPOS_ASSERT(COperator::EopLogicalProject == eopid ||
			COperator::EopLogicalSequenceProject == eopid);

	CExpression *pexprPrjList = (*pexpr)[1];

	// fail if project elements have outer references
	CColRefSet *pcrsOutput = CDrvdPropRelational::Pdprel((*pexpr)[0]->PdpDerive())->PcrsOutput();
	CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexprPrjList->PdpDerive())->PcrsUsed();
	if (!pcrsOutput->FSubset(pcrsUsed))
	{
		return false;
	}

	if (COperator::EopLogicalSequenceProject == eopid)
	{
		(void) pexpr->PdpDerive();
		CExpressionHandle exprhdl(pmp);
		exprhdl.Attach(pexpr);
		exprhdl.DeriveProps(NULL /*pdpctxt*/);
		// fail if a relational child of LogicalSequenceProject has outer references or
		// if a SequenceProject has local outer references
		// Ex: select C.j from C where C.i in (select rank() over (order by B.i) from B where B.i=C.i) order by C.j;
		// The above subquery has outer references and result of window function is projected from subquery
		if (CLogicalSequenceProject::PopConvert(pexpr->Pop())->FHasLocalOuterRefs(exprhdl) || CUtils::FHasOuterRefs((*pexpr)[0]))
		{
			return false;
		}
	}

	// decorrelate relational child
	CExpression *pexprRelational = NULL;
	if (!FProcess(pmp, (*pexpr)[0], fEqualityOnly, &pexprRelational, pdrgpexprCorrelations))
	{
		GPOS_ASSERT(NULL == pexprRelational);
		return false;
	}
	
	// assemble new project
	COperator *pop = pexpr->Pop();
	pop->AddRef();
	pexprPrjList->AddRef();
	
	*ppexprDecorrelated = GPOS_NEW(pmp) CExpression(pmp, pop, pexprRelational, pexprPrjList);
	
	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CDecorrelator::FProcessLimit
//
//	@doc:
//		Decorrelate limit
//
//---------------------------------------------------------------------------
BOOL
CDecorrelator::FProcessLimit
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	BOOL fEqualityOnly,
	CExpression **ppexprDecorrelated,
	DrgPexpr *pdrgpexprCorrelations
	)
{
	GPOS_ASSERT(COperator::EopLogicalLimit == pexpr->Pop()->Eopid());

	CExpression *pexprOffset = (*pexpr)[1];
	CExpression *pexprRowCount = (*pexpr)[2];

	// fail if there are any outer references below Limit
	if (CUtils::FHasOuterRefs(pexpr))
	{
		return false;
	}

	// decorrelate relational child
	CExpression *pexprRelational = NULL;
	if (!FProcess(pmp, (*pexpr)[0], fEqualityOnly, &pexprRelational, pdrgpexprCorrelations))
	{
		GPOS_ASSERT(NULL == pexprRelational);
		return false;
	}

	// assemble new project
	COperator *pop = pexpr->Pop();
	pop->AddRef();
	pexprOffset->AddRef();
	pexprRowCount->AddRef();

	*ppexprDecorrelated = GPOS_NEW(pmp) CExpression(pmp, pop, pexprRelational, pexprOffset, pexprRowCount);

	return true;
}

// EOF
