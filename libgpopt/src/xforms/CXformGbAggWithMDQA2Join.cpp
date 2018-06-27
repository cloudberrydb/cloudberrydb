//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Inc.
//
//	@filename:
//		CXformGbAggWithMDQA2Join.cpp
//
//	@doc:
//		Transform a GbAgg with multiple distinct qualified aggregates (MDQAs)
//		to a join tree with single DQA leaves
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/xforms/CXformGbAggWithMDQA2Join.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAggWithMDQA2Join::CXformGbAggWithMDQA2Join
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformGbAggWithMDQA2Join::CXformGbAggWithMDQA2Join
	(
	IMemoryPool *pmp
	)
	:
	CXformExploration
		(
		 // pattern
		GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalGbAgg(pmp),
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp)), // relational child
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))  // scalar project list
					)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAggWithMDQA2Join::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformGbAggWithMDQA2Join::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	CAutoMemoryPool amp;

	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(exprhdl.Pop());

	if (COperator::EgbaggtypeGlobal == popAgg->Egbaggtype() &&
		exprhdl.Pdpscalar(1 /*ulChildIndex*/)->FHasMultipleDistinctAggs())
	{
		return CXform::ExfpHigh;
	}

	return CXform::ExfpNone;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAggWithMDQA2Join::PexprMDQAs2Join
//
//	@doc:
//		Converts GbAgg with multiple distinct aggregates into a join of single
//		distinct aggregates,
//
//		distinct aggregates that share the same argument are grouped together
//		in one leaf of the generated join expression,
//
//		non-distinct aggregates are also grouped together in one leaf of the
//		generated join expression
//
//---------------------------------------------------------------------------
CExpression *
CXformGbAggWithMDQA2Join::PexprMDQAs2Join
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(COperator::EopLogicalGbAgg == pexpr->Pop()->Eopid());
	GPOS_ASSERT(CDrvdPropScalar::Pdpscalar((*pexpr)[1]->PdpDerive())->FHasMultipleDistinctAggs());

	// extract components
	CExpression *pexprChild = (*pexpr)[0];

	CColRefSet *pcrsChildOutput = CDrvdPropRelational::Pdprel(pexprChild->PdpDerive())->PcrsOutput();
	DrgPcr *pdrgpcrChildOutput = pcrsChildOutput->Pdrgpcr(pmp);

	// create a CTE producer based on child expression
	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();
	const ULONG ulCTEId = pcteinfo->UlNextId();
	(void) CXformUtils::PexprAddCTEProducer(pmp, ulCTEId, pdrgpcrChildOutput, pexprChild);

	// create a CTE consumer with child output columns
	CExpression *pexprConsumer =
			GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalCTEConsumer(pmp, ulCTEId, pdrgpcrChildOutput)
				);
	pcteinfo->IncrementConsumers(ulCTEId);

	// finalize GbAgg expression by replacing its child with CTE consumer
	pexpr->Pop()->AddRef();
	(*pexpr)[1]->AddRef();
	CExpression *pexprGbAggWithConsumer = GPOS_NEW(pmp) CExpression(pmp, pexpr->Pop(), pexprConsumer, (*pexpr)[1]);

	CExpression *pexprJoinDQAs = CXformUtils::PexprGbAggOnCTEConsumer2Join(pmp, pexprGbAggWithConsumer);
	GPOS_ASSERT(NULL != pexprJoinDQAs);

	pexprGbAggWithConsumer->Release();

	return GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalCTEAnchor(pmp, ulCTEId),
					pexprJoinDQAs
					);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAggWithMDQA2Join::PexprExpandMDQAs
//
//	@doc:
//		Expand GbAgg with multiple distinct aggregates into a join of single
//		distinct aggregates,
//		return NULL if expansion is not done
//
//---------------------------------------------------------------------------
CExpression *
CXformGbAggWithMDQA2Join::PexprExpandMDQAs
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(COperator::EopLogicalGbAgg == pexpr->Pop()->Eopid());

	COperator *pop = pexpr->Pop();
	if (CLogicalGbAgg::PopConvert(pop)->FGlobal())
	{
		BOOL fHasMultipleDistinctAggs = CDrvdPropScalar::Pdpscalar((*pexpr)[1]->PdpDerive())->FHasMultipleDistinctAggs();
		if (fHasMultipleDistinctAggs)
		{
			CExpression *pexprExpanded = PexprMDQAs2Join(pmp, pexpr);

			// recursively process the resulting expression
			CExpression *pexprResult = PexprTransform(pmp, pexprExpanded);
			pexprExpanded->Release();

			return pexprResult;
		}
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAggWithMDQA2Join::PexprTransform
//
//	@doc:
//		Main transformation driver
//
//---------------------------------------------------------------------------
CExpression *
CXformGbAggWithMDQA2Join::PexprTransform
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	if (COperator::EopLogicalGbAgg == pop->Eopid())
	{
		CExpression *pexprResult = PexprExpandMDQAs(pmp, pexpr);
		if (NULL != pexprResult)
		{
			return pexprResult;
		}
	}

	// recursively process child expressions
	const ULONG ulArity = pexpr->UlArity();
	DrgPexpr *pdrgpexprChildren = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = PexprTransform(pmp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexprChildren);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAggWithMDQA2Join::Transform
//
//	@doc:
//		Actual transformation to expand multiple distinct qualified aggregates
//		(MDQAs) to a join tree with single DQA leaves
//
//---------------------------------------------------------------------------
void
CXformGbAggWithMDQA2Join::Transform
	(
	CXformContext *pxfctxt,
	CXformResult *pxfres,
	CExpression *pexpr
	)
	const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	IMemoryPool *pmp = pxfctxt->Pmp();

	CExpression *pexprResult = PexprTransform(pmp, pexpr);
	if (NULL != pexprResult)
	{
		pxfres->Add(pexprResult);
	}
}

BOOL
CXformGbAggWithMDQA2Join::IsApplyOnce()
{
	return true;
}
// EOF
