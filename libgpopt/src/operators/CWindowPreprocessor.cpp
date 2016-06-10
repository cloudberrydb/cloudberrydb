//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2015 Pivotal Inc.
//
//	@filename:
//		CWindowPreprocessor.cpp
//
//	@doc:
//		Preprocessing routines of window functions
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CWindowPreprocessor.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CWindowPreprocessor::SplitPrjList
//
//	@doc:
//		Iterate over project elements and split them elements between
//		Distinct Aggs list, and Others list
//
//---------------------------------------------------------------------------
void
CWindowPreprocessor::SplitPrjList
	(
	IMemoryPool *pmp,
	CExpression *pexprSeqPrj,
	DrgPexpr **ppdrgpexprDistinctAggsPrEl, // output: list of project elements with Distinct Aggs
	DrgPexpr **ppdrgpexprOtherPrEl, // output: list of project elements with Other window functions
	DrgPos **ppdrgposOther, // output: array of order specs of window functions used in Others list
	DrgPwf **ppdrgpwfOther // output: array of frame specs of window functions used in Others list
	)
{
	GPOS_ASSERT(NULL != pexprSeqPrj);
	GPOS_ASSERT(NULL != ppdrgpexprDistinctAggsPrEl);
	GPOS_ASSERT(NULL != ppdrgpexprOtherPrEl);
	GPOS_ASSERT(NULL != ppdrgposOther);
	GPOS_ASSERT(NULL != ppdrgpwfOther);

	CLogicalSequenceProject *popSeqPrj = CLogicalSequenceProject::PopConvert(pexprSeqPrj->Pop());
	CExpression *pexprPrjList = (*pexprSeqPrj)[1];

	DrgPos *pdrgpos = popSeqPrj->Pdrgpos();
	BOOL fHasOrderSpecs = popSeqPrj->FHasOrderSpecs();

	DrgPwf *pdrgpwf = popSeqPrj->Pdrgpwf();
	BOOL fHasFrameSpecs = popSeqPrj->FHasFrameSpecs();

	DrgPexpr *pdrgpexprDistinctAggsPrEl = GPOS_NEW(pmp) DrgPexpr(pmp);

	DrgPexpr *pdrgpexprOtherPrEl = GPOS_NEW(pmp) DrgPexpr(pmp);
	DrgPos *pdrgposOther = GPOS_NEW(pmp) DrgPos(pmp);
	DrgPwf *pdrgpwfOther = GPOS_NEW(pmp) DrgPwf(pmp);

	// iterate over project list and split project elements between
	// Distinct Aggs list, and Others list
	const ULONG ulArity = pexprPrjList->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprPrjEl = (*pexprPrjList)[ul];
		CExpression *pexprWinFunc = (*pexprPrjEl)[0];
		CScalarWindowFunc *popScWinFunc = CScalarWindowFunc::PopConvert(pexprWinFunc->Pop());
		CScalarProjectElement *popScPrjElem = CScalarProjectElement::PopConvert(pexprPrjEl->Pop());
		CColRef *pcrPrjElem = popScPrjElem->Pcr();

		if (popScWinFunc->FDistinct() && popScWinFunc->FAgg())
		{
			CExpression *pexprAgg = CXformUtils::PexprWinFuncAgg2ScalarAgg(pmp, pexprWinFunc);
			CExpression *pexprNewPrjElem = CUtils::PexprScalarProjectElement(pmp, pcrPrjElem, pexprAgg);
			pdrgpexprDistinctAggsPrEl->Append(pexprNewPrjElem);
		}
		else
		{
			if (fHasOrderSpecs)
			{
				(*pdrgpos)[ul]->AddRef();
				pdrgposOther->Append((*pdrgpos)[ul]);
			}

			if (fHasFrameSpecs)
			{
				(*pdrgpwf)[ul]->AddRef();
				pdrgpwfOther->Append((*pdrgpwf)[ul]);
			}

			pexprWinFunc->AddRef();
			CExpression *pexprNewPrjElem = CUtils::PexprScalarProjectElement(pmp, pcrPrjElem, pexprWinFunc);
			pdrgpexprOtherPrEl->Append(pexprNewPrjElem);
		}
	}

	*ppdrgpexprDistinctAggsPrEl = pdrgpexprDistinctAggsPrEl;
	*ppdrgpexprOtherPrEl = pdrgpexprOtherPrEl;
	*ppdrgposOther = pdrgposOther;
	*ppdrgpwfOther = pdrgpwfOther;
}


//---------------------------------------------------------------------------
//	@function:
//		CWindowPreprocessor::SplitSeqPrj
//
//	@doc:
//		Split SeqPrj expression into:
//		- A GbAgg expression containing distinct Aggs, and
//		- A SeqPrj expression containing all remaining window functions
//
//---------------------------------------------------------------------------
void
CWindowPreprocessor::SplitSeqPrj
	(
	IMemoryPool *pmp,
	CExpression *pexprSeqPrj,
	CExpression **ppexprGbAgg,	// output: GbAgg expression containing distinct Aggs
	CExpression **ppexprOutputSeqPrj // output: SeqPrj expression containing all remaining window functions
	)
{
	GPOS_ASSERT(NULL != pexprSeqPrj);
	GPOS_ASSERT(NULL != ppexprGbAgg);
	GPOS_ASSERT(NULL != ppexprOutputSeqPrj);

	// split project elements between Distinct Aggs list, and Others list
	DrgPexpr *pdrgpexprDistinctAggsPrEl = NULL;
	DrgPexpr *pdrgpexprOtherPrEl = NULL;
	DrgPos *pdrgposOther = NULL;
	DrgPwf *pdrgpwfOther = NULL;
	SplitPrjList(pmp, pexprSeqPrj, &pdrgpexprDistinctAggsPrEl, &pdrgpexprOtherPrEl, &pdrgposOther, &pdrgpwfOther);

	// check distribution spec of original SeqPrj and extract grouping columns
	// from window (PARTITION BY) clause
	CLogicalSequenceProject *popSeqPrj = CLogicalSequenceProject::PopConvert(pexprSeqPrj->Pop());
	CDistributionSpec *pds = popSeqPrj->Pds();
	DrgPcr *pdrgpcrGrpCols = NULL;
	if (CDistributionSpec::EdtHashed == pds->Edt())
	{
		CColRefSet *pcrs = CUtils::PcrsExtractColumns(pmp, CDistributionSpecHashed::PdsConvert(pds)->Pdrgpexpr());
		pdrgpcrGrpCols = pcrs->Pdrgpcr(pmp);
		pcrs->Release();
	}
	else
	{
		// no (PARTITION BY) clause
		pdrgpcrGrpCols = GPOS_NEW(pmp) DrgPcr(pmp);
	}

	CExpression *pexprSeqPrjChild = (*pexprSeqPrj)[0];
	pexprSeqPrjChild->AddRef();
	*ppexprGbAgg =
				GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalGbAgg(pmp, pdrgpcrGrpCols, COperator::EgbaggtypeGlobal),
					pexprSeqPrjChild,
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp), pdrgpexprDistinctAggsPrEl)
					);

	pexprSeqPrjChild->AddRef();
	if (0 == pdrgpexprOtherPrEl->UlLength())
	{
		// no remaining window functions after excluding distinct aggs,
		// reuse the original SeqPrj child in this case
		pdrgpexprOtherPrEl->Release();
		pdrgposOther->Release();
		pdrgpwfOther->Release();
		*ppexprOutputSeqPrj = pexprSeqPrjChild;

		return;
	}

	// create a new SeqPrj expression for remaining window functions
	pds->AddRef();
	*ppexprOutputSeqPrj =
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CLogicalSequenceProject(pmp, pds, pdrgposOther, pdrgpwfOther),
			pexprSeqPrjChild,
			GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp), pdrgpexprOtherPrEl)
			);
}


//---------------------------------------------------------------------------
//	@function:
//		CWindowPreprocessor::CreateCTE
//
//	@doc:
//		Create a CTE with two consumers using the child expression of
//		Sequence Project
//
//---------------------------------------------------------------------------
void
CWindowPreprocessor::CreateCTE
	(
	IMemoryPool *pmp,
	CExpression *pexprSeqPrj,
	CExpression **ppexprFirstConsumer,
	CExpression **ppexprSecondConsumer
	)
{
	GPOS_ASSERT(NULL != pexprSeqPrj);
	GPOS_ASSERT(COperator::EopLogicalSequenceProject == pexprSeqPrj->Pop()->Eopid());
	GPOS_ASSERT(NULL != ppexprFirstConsumer);
	GPOS_ASSERT(NULL != ppexprSecondConsumer);

	CExpression *pexprChild = (*pexprSeqPrj)[0];
	CColRefSet *pcrsChildOutput = CDrvdPropRelational::Pdprel(pexprChild->PdpDerive())->PcrsOutput();
	DrgPcr *pdrgpcrChildOutput = pcrsChildOutput->Pdrgpcr(pmp);

	// create a CTE producer based on SeqPrj child expression
	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();
	const ULONG ulCTEId = pcteinfo->UlNextId();
	CExpression *pexprCTEProd = CXformUtils::PexprAddCTEProducer(pmp, ulCTEId, pdrgpcrChildOutput, pexprChild);
	DrgPcr *pdrgpcrProducerOutput = CDrvdPropRelational::Pdprel(pexprCTEProd->PdpDerive())->PcrsOutput()->Pdrgpcr(pmp);

	// first consumer creates new output columns to be used later as input to GbAgg expression
	*ppexprFirstConsumer =
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CLogicalCTEConsumer(pmp, ulCTEId, CUtils::PdrgpcrCopy(pmp, pdrgpcrProducerOutput))
			);
	pcteinfo->IncrementConsumers(ulCTEId);
	pdrgpcrProducerOutput->Release();

	// second consumer reuses the same output columns of SeqPrj child to be able to provide any requested columns upstream
	*ppexprSecondConsumer =
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CLogicalCTEConsumer(pmp, ulCTEId, pdrgpcrChildOutput)
			);
	pcteinfo->IncrementConsumers(ulCTEId);
}


//---------------------------------------------------------------------------
//	@function:
//		CWindowPreprocessor::PdrgpcrGrpCols
//
//	@doc:
//		Extract grouping columns from given expression,
//		we expect expression to be either a GbAgg expression or a join
//		whose inner child is a GbAgg expression
//
//---------------------------------------------------------------------------
DrgPcr *
CWindowPreprocessor::PdrgpcrGrpCols
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();

	if (COperator::EopLogicalGbAgg == pop->Eopid())
	{
		// passed expression is a Group By, return grouping columns
		return CLogicalGbAgg::PopConvert(pop)->Pdrgpcr();
	}

	if (CUtils::FLogicalJoin(pop))
	{
		// pass expression is a join, we expect a Group By on the inner side
		COperator *popInner = (*pexpr)[1]->Pop();
		if (COperator::EopLogicalGbAgg == popInner->Eopid())
		{
			return CLogicalGbAgg::PopConvert(popInner)->Pdrgpcr();
		}
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CWindowPreprocessor::PexprSeqPrj2Join
//
//	@doc:
//		Transform sequence project expression with distinct aggregates
//		into an inner join expression,
//			- the outer child of the join is a GbAgg expression that computes
//			distinct aggs,
//			- the inner child of the join is a SeqPrj expression that computes
//			remaining window functions
//
//		we use a CTE to compute the input to both join children, while maintaining
//		all column references upstream by reusing the same computed columns in the
//		original SeqPrj expression
//
//---------------------------------------------------------------------------
CExpression *
CWindowPreprocessor::PexprSeqPrj2Join
	(
	IMemoryPool *pmp,
	CExpression *pexprSeqPrj
	)
{
	GPOS_ASSERT(NULL != pexprSeqPrj);
	GPOS_ASSERT(COperator::EopLogicalSequenceProject == pexprSeqPrj->Pop()->Eopid());
	GPOS_ASSERT(0 < CDrvdPropScalar::Pdpscalar((*pexprSeqPrj)[1]->PdpDerive())->UlDistinctAggs());

	// split SeqPrj expression into a GbAgg expression (for distinct Aggs), and
	// another SeqPrj expression (for remaining window functions)
	CExpression *pexprGbAgg = NULL;
	CExpression *pexprWindow = NULL;
	SplitSeqPrj(pmp, pexprSeqPrj, &pexprGbAgg, &pexprWindow);

	// create CTE using SeqPrj child expression
	CExpression *pexprGbAggConsumer = NULL;
	CExpression *pexprWindowConsumer = NULL;
	CreateCTE(pmp, pexprSeqPrj, &pexprGbAggConsumer, &pexprWindowConsumer);

	// extract output columns of SeqPrj child expression
	CExpression *pexprChild = (*pexprSeqPrj)[0];
	DrgPcr *pdrgpcrChildOutput = CDrvdPropRelational::Pdprel(pexprChild->PdpDerive())->PcrsOutput()->Pdrgpcr(pmp);

	// to match requested columns upstream, we have to re-use the same computed
	// columns that define the aggregates, we avoid recreating new columns during
	// expression copy by passing fMustExist as false
	DrgPcr *pdrgpcrConsumerOutput = CLogicalCTEConsumer::PopConvert(pexprGbAggConsumer->Pop())->Pdrgpcr();
	HMUlCr *phmulcr = CUtils::PhmulcrMapping(pmp, pdrgpcrChildOutput, pdrgpcrConsumerOutput);
	CExpression *pexprGbAggRemapped = pexprGbAgg->PexprCopyWithRemappedColumns(pmp, phmulcr, false /*fMustExist*/);
	phmulcr->Release();
	pdrgpcrChildOutput->Release();
	pexprGbAgg->Release();

	// finalize GbAgg expression by replacing its child with CTE consumer
	pexprGbAggRemapped->Pop()->AddRef();
	(*pexprGbAggRemapped)[1]->AddRef();
	CExpression *pexprGbAggWithConsumer = GPOS_NEW(pmp) CExpression(pmp, pexprGbAggRemapped->Pop(), pexprGbAggConsumer, (*pexprGbAggRemapped)[1]);
	pexprGbAggRemapped->Release();

	// in case of multiple Distinct Aggs, we need to expand the GbAgg expression
	// into a join expression where leaves carry single Distinct Aggs
	CExpression *pexprJoinDQAs = CXformUtils::PexprGbAggOnCTEConsumer2Join(pmp, pexprGbAggWithConsumer);
	pexprGbAggWithConsumer->Release();

	CExpression *pexprWindowFinal = NULL;
	if (COperator::EopLogicalSequenceProject == pexprWindow->Pop()->Eopid())
	{
		// create a new SeqPrj expression for remaining window functions,
		// and replace expression child withCTE consumer
		pexprWindow->Pop()->AddRef();
		(*pexprWindow)[1]->AddRef();
		pexprWindowFinal = GPOS_NEW(pmp) CExpression(pmp, pexprWindow->Pop(), pexprWindowConsumer, (*pexprWindow)[1]);
	}
	else
	{
		// no remaining window functions, simply reuse created CTE consumer
		pexprWindowFinal = pexprWindowConsumer;
	}
	pexprWindow->Release();

	// extract grouping columns from created join expression
	DrgPcr *pdrgpcrGrpCols = PdrgpcrGrpCols(pexprJoinDQAs);

	// create final join condition
	CExpression *pexprJoinCondition = NULL;

	if (NULL != pdrgpcrGrpCols && 0 < pdrgpcrGrpCols->UlLength())
	{
		// extract PARTITION BY columns from original SeqPrj expression
		CLogicalSequenceProject *popSeqPrj = CLogicalSequenceProject::PopConvert(pexprSeqPrj->Pop());
		CDistributionSpec *pds = popSeqPrj->Pds();
		CColRefSet *pcrs = CUtils::PcrsExtractColumns(pmp, CDistributionSpecHashed::PdsConvert(pds)->Pdrgpexpr());
		DrgPcr *pdrgpcrPartitionBy = pcrs->Pdrgpcr(pmp);
		pcrs->Release();
		GPOS_ASSERT(pdrgpcrGrpCols->UlLength() == pdrgpcrPartitionBy->UlLength() &&
				"Partition By columns in window function are not the same as grouping columns in created Aggs");

		// create a conjunction of INDF expressions comparing a GROUP BY column to a PARTITION BY column
		pexprJoinCondition = CPredicateUtils::PexprINDFConjunction(pmp, pdrgpcrGrpCols, pdrgpcrPartitionBy);
		pdrgpcrPartitionBy->Release();
	}
	else
	{
		// no PARTITION BY, join condition is const True
		pexprJoinCondition = CUtils::PexprScalarConstBool(pmp, true /*fVal*/);
	}

	// create a join between expanded DQAs and Window expressions
	CExpression *pexprJoin =
		CUtils::PexprLogicalJoin<CLogicalInnerJoin>(pmp, pexprJoinDQAs, pexprWindowFinal, pexprJoinCondition);

	ULONG ulCTEId = CLogicalCTEConsumer::PopConvert(pexprGbAggConsumer->Pop())->UlCTEId();
	return GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalCTEAnchor(pmp, ulCTEId),
				pexprJoin
				);
}


//---------------------------------------------------------------------------
//	@function:
//		CWindowPreprocessor::PexprPreprocess
//
//	@doc:
//		Main driver
//
//---------------------------------------------------------------------------
CExpression *
CWindowPreprocessor::PexprPreprocess
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
	if (COperator::EopLogicalSequenceProject == pop->Eopid() &&
		0 < CDrvdPropScalar::Pdpscalar((*pexpr)[1]->PdpDerive())->UlDistinctAggs())
	{
		CExpression *pexprJoin = PexprSeqPrj2Join(pmp, pexpr);

		// recursively process the resulting expression
		CExpression *pexprResult = PexprPreprocess(pmp, pexprJoin);
		pexprJoin->Release();

		return pexprResult;
	}

	// recursively process child expressions
	const ULONG ulArity = pexpr->UlArity();
	DrgPexpr *pdrgpexprChildren = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = PexprPreprocess(pmp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexprChildren);
}

// EOF
