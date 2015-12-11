//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformPushGbBelowSetOp.inl
//
//	@doc:
//		Push grouping below set operation
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/operators/ops.h"


using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbBelowSetOp::CXformPushGbBelowSetOp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
template<class TSetOp>
CXformPushGbBelowSetOp<TSetOp>::CXformPushGbBelowSetOp
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
					GPOS_NEW(pmp) CExpression  // left child is a set operation
					 	 (
					 	 pmp,
					 	 GPOS_NEW(pmp) TSetOp(pmp),
					 	 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternMultiLeaf(pmp))
					 	 ),
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp)) // project list of group-by 
					)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbBelowSetOp::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
template<class TSetOp>
CXform::EXformPromise
CXformPushGbBelowSetOp<TSetOp>::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(exprhdl.Pop());
	if (popGbAgg->FGlobal())
	{
		return ExfpHigh;
	}
	
	return ExfpNone;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbBelowSetOp::Transform
//
//	@doc:
//		Actual transformation;
//		
//		Example:
//			SELECT x from (SELECT x1 from A UNION ALL SELECT x2 from B) as foo(x) GROUP BY x;
//
//		Input:
//			GbAgg(x)
//				+--SetOp(x)
//					|--Get(A)[x1]
//					+--Get(B)[x2]
//
//		Output:
//			GbAgg(x)
//				+--SetOp(x)
//					|--GbAgg(x1)
//					|	+--Get(A)[x1]
//					+--GbAgg(x2)
//						+Get(B)[x2]
//			
//
//---------------------------------------------------------------------------
template<class TSetOp>
void
CXformPushGbBelowSetOp<TSetOp>::Transform
	(
	CXformContext *pxfctxt,
	CXformResult *pxfres,
	CExpression *pexpr
	)
	const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	IMemoryPool *pmp = pxfctxt->Pmp();
	
	CExpression *pexprSetOp = (*pexpr)[0];
	CExpression *pexprPrjList = (*pexpr)[1];
	if (0 < pexprPrjList->UlArity())
	{
		// bail-out if group-by has any aggregate functions
		return;
	}
	
	CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());
	CLogicalSetOp *popSetOp = CLogicalSetOp::PopConvert(pexprSetOp->Pop());
	
	DrgPcr *pdrgpcrGb = popGbAgg->Pdrgpcr();
	DrgPcr *pdrgpcrOutput = popSetOp->PdrgpcrOutput();
	CColRefSet *pcrsOutput = GPOS_NEW(pmp) CColRefSet(pmp, pdrgpcrOutput);
	DrgDrgPcr *pdrgpdrgpcrInput = popSetOp->PdrgpdrgpcrInput();
	DrgPexpr *pdrgpexprNewChildren = GPOS_NEW(pmp) DrgPexpr(pmp);
	DrgDrgPcr *pdrgpdrgpcrNewInput = GPOS_NEW(pmp) DrgDrgPcr(pmp);
	const ULONG ulArity = pexprSetOp->UlArity();

	BOOL fNewChild = false;

	for (ULONG ulChild = 0; ulChild < ulArity; ulChild++)
	{
		CExpression *pexprChild = (*pexprSetOp)[ulChild];
		DrgPcr *pdrgpcrChild = (*pdrgpdrgpcrInput)[ulChild];
		CColRefSet *pcrsChild =  GPOS_NEW(pmp) CColRefSet(pmp, pdrgpcrChild);
		
		DrgPcr *pdrgpcrChildGb = NULL;
		if (!pcrsChild->FEqual(pcrsOutput))
		{
			// use column mapping in SetOp to set child grouping colums
			HMUlCr *phmulcr = CUtils::PhmulcrMapping(pmp, pdrgpcrOutput, pdrgpcrChild);	
			pdrgpcrChildGb = CUtils::PdrgpcrRemap(pmp, pdrgpcrGb, phmulcr, true /*fMustExist*/);
			phmulcr->Release();
		}
		else
		{
			// use grouping columns directly as child grouping colums
			pdrgpcrGb->AddRef();
			pdrgpcrChildGb = pdrgpcrGb;
		}	
		
		pexprChild->AddRef();
		pcrsChild->Release();

		// if child of setop is already an Agg with the same grouping columns
		// that we want to use, there is no need to add another agg on top of it
		COperator *popChild = pexprChild->Pop();
		if (COperator::EopLogicalGbAgg == popChild->Eopid())
		{
			CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(popChild);
			if (CColRef::FEqual(popGbAgg->Pdrgpcr(), pdrgpcrChildGb))
			{
				pdrgpexprNewChildren->Append(pexprChild);
				pdrgpdrgpcrNewInput->Append(pdrgpcrChildGb);

				continue;
			}
		}

		fNewChild = true;
		pexprPrjList->AddRef();
		CExpression *pexprChildGb = CUtils::PexprLogicalGbAggGlobal(pmp, pdrgpcrChildGb, pexprChild, pexprPrjList);
		pdrgpexprNewChildren->Append(pexprChildGb);
		
		pdrgpcrChildGb->AddRef();
		pdrgpdrgpcrNewInput->Append(pdrgpcrChildGb);
	}
	
	pcrsOutput->Release();

	if (!fNewChild)
	{
		// all children of the union were already Aggs with the same grouping
		// columns that we would have created. No new alternative expressions
		pdrgpdrgpcrNewInput->Release();
		pdrgpexprNewChildren->Release();

		return;
	}

	pdrgpcrGb->AddRef();
	TSetOp *popSetOpNew = GPOS_NEW(pmp) TSetOp(pmp, pdrgpcrGb, pdrgpdrgpcrNewInput);
	CExpression *pexprNewSetOp = GPOS_NEW(pmp) CExpression(pmp, popSetOpNew, pdrgpexprNewChildren);

	popGbAgg->AddRef();
	pexprPrjList->AddRef();	
	CExpression *pexprResult = GPOS_NEW(pmp) CExpression(pmp, popGbAgg, pexprNewSetOp, pexprPrjList); 

	pxfres->Add(pexprResult);
}


// EOF

