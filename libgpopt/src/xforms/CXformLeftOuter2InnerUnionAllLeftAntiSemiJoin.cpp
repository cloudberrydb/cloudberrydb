//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin.cpp
//
//	@doc:
//		Transform
//      LOJ
//        |--Small
//        +--Big
//
// 		to
//
//      UnionAll
//      |---CTEConsumer(A)
//      +---Project_{append nulls)
//          +---LASJ_(key(Small))
//                   |---CTEConsumer(B)
//                   +---Gb(keys(Small))
//                        +---CTEConsumer(A)
//
//		where B is the CTE that produces Small
//		and A is the CTE that produces InnerJoin(Big, CTEConsumer(B)).
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin.h"

#include "gpos/common/CDynamicPtrArrayUtils.h"
#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"

#include "naucrates/traceflags/traceflags.h"

using namespace gpopt;

// if ratio of the cardinalities outer/inner is below this value, we apply the xform
const DOUBLE
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::m_dOuterInnerRatioThreshold = 0.001;

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin
	(
	IMemoryPool *pmp
	)
	:
	// pattern
	CXformExploration
		(
		GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalLeftOuterJoin(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp)), // left child
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp)), // right child
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))  // predicate
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle.
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	CColRefSet *pcrsInner = exprhdl.Pdprel(1 /*ulChildIndex*/)->PcrsOutput();
	CExpression *pexprScalar = exprhdl.PexprScalarChild(2 /*ulChildIndex*/);
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	if (!CPredicateUtils::FSimpleEqualityUsingCols(pmp, pexprScalar, pcrsInner))
	{
		return ExfpNone;
	}

	if (GPOS_FTRACE(gpos::EopttraceApplyLeftOuter2InnerUnionAllLeftAntiSemiJoinDisregardingStats)
			|| NULL == exprhdl.Pgexpr())
	{
		return CXform::ExfpHigh;
	}

	// check if stats are derivable on child groups
	const ULONG ulArity = exprhdl.UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CGroup *pgroupChild = (*exprhdl.Pgexpr())[ul];
		if (!pgroupChild->FScalar() && !pgroupChild->FStatsDerivable(pmp))
		{
			// stats must be derivable on every child
			return CXform::ExfpNone;
		}
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::FCheckStats
//
//	@doc:
//		Check the stats ratio to decide whether to apply the xform or not.
//
//---------------------------------------------------------------------------
BOOL
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::FApplyXformUsingStatsInfo
	(
	const IStatistics *pstatsOuter,
	const IStatistics *pstatsInner
	)
	const
{
	if (GPOS_FTRACE(gpos::EopttraceApplyLeftOuter2InnerUnionAllLeftAntiSemiJoinDisregardingStats))
	{
		return true;
	}

	if (NULL == pstatsOuter || NULL == pstatsInner)
	{
		return false;
	}

	DOUBLE dRowsOuter = pstatsOuter->DRows().DVal();
	DOUBLE dRowsInner = pstatsInner->DRows().DVal();
	GPOS_ASSERT(0 < dRowsInner);

	return dRowsOuter / dRowsInner <= m_dOuterInnerRatioThreshold;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::Transform
//
//	@doc:
//		Apply the transformation, e.g.
//  Input:
//  +--CLogicalLeftOuterJoin
//     |--CLogicalGet "items", Columns: ["i_item_sk" (95)]
//     |--CLogicalGet "store_sales", Columns: ["ss_item_sk" (124)]
//     +--CScalarCmp (=)
//        |--CScalarIdent "i_item_sk"
//        +--CScalarIdent "ss_item_sk"
//  Output:
//  Alternatives:
//  0:
//  +--CLogicalCTEAnchor (2)
//     +--CLogicalCTEAnchor (3)
//        +--CLogicalUnionAll ["i_item_sk" (95), "ss_item_sk" (124)]
//           |--CLogicalCTEConsumer (3), Columns: ["i_item_sk" (95), "ss_item_sk" (124)]
//           +--CLogicalProject
//              |--CLogicalLeftAntiSemiJoin
//              |  |--CLogicalCTEConsumer (2), Columns: ["i_item_sk" (342)]
//              |  |--CLogicalGbAgg( Global ) Grp Cols: ["i_item_sk" (343)][Global], Minimal Grp Cols: [], Generates Duplicates :[ 0 ]
//              |  |  |--CLogicalCTEConsumer (3), Columns: ["i_item_sk" (343), "ss_item_sk" (344)]
//              |  |  +--CScalarProjectList
//              |  +--CScalarBoolOp (EboolopNot)
//              |     +--CScalarIsDistinctFrom (=)
//              |        |--CScalarIdent "i_item_sk" (342)
//              |        +--CScalarIdent "i_item_sk" (343)
//              +--CScalarProjectList
//                 +--CScalarProjectElement "ss_item_sk" (466)
//                    +--CScalarConst (null)
//
//  +--CLogicalCTEProducer (2), Columns: ["i_item_sk" (190)]
//     +--CLogicalGet "items", Columns: ["i_item_sk" (190)]
//
//  +--CLogicalCTEProducer (3), Columns: ["i_item_sk" (247), "ss_item_sk" (248)]
//      +--CLogicalInnerJoin
//         |--CLogicalCTEConsumer (0), Columns: ["ss_item_sk" (248)]
//         |--CLogicalCTEConsumer (2), Columns: ["i_item_sk" (247)]
//         +--CScalarCmp (=)
//            |--CScalarIdent "i_item_sk" (247)
//            +--CScalarIdent "ss_item_sk" (248)
//
//---------------------------------------------------------------------------
void
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::Transform
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
	// extract components
	CExpression *pexprOuter = (*pexpr)[0];
	CExpression *pexprInner = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	if (!FValidInnerExpr(pexprInner))
	{
		return;
	}

	if (!FApplyXformUsingStatsInfo(pexprOuter->Pstats(), pexprInner->Pstats()))
	{
		return;
	}

	const ULONG ulCTEOuterId = COptCtxt::PoctxtFromTLS()->Pcteinfo()->UlNextId();
	CColRefSet *pcrsOuter = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput();
	DrgPcr *pdrgpcrOuter = pcrsOuter->Pdrgpcr(pmp);
	(void) CXformUtils::PexprAddCTEProducer(pmp, ulCTEOuterId, pdrgpcrOuter, pexprOuter);

	// invert the order of the branches of the original join, so that the small one becomes
	// inner
	pexprInner->AddRef();
	pexprScalar->AddRef();
	CExpression *pexprInnerJoin = GPOS_NEW(pmp) CExpression
									(
									pmp,
									GPOS_NEW(pmp) CLogicalInnerJoin(pmp),
									pexprInner,
									CXformUtils::PexprCTEConsumer(pmp, ulCTEOuterId, pdrgpcrOuter),
									pexprScalar
									);

	CColRefSet *pcrsJoinOutput = CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->PcrsOutput();
	DrgPcr *pdrgpcrJoinOutput = pcrsJoinOutput->Pdrgpcr(pmp);
	const ULONG ulCTEJoinId = COptCtxt::PoctxtFromTLS()->Pcteinfo()->UlNextId();
	(void) CXformUtils::PexprAddCTEProducer(pmp, ulCTEJoinId, pdrgpcrJoinOutput, pexprInnerJoin);

	CColRefSet *pcrsScalar = CDrvdPropScalar::Pdpscalar(pexprScalar->PdpDerive())->PcrsUsed();
	CColRefSet *pcrsInner = CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->PcrsOutput();

	DrgPcr *pdrgpcrProjectOutput = NULL;
	CExpression *pexprProjectAppendNulls = PexprProjectOverLeftAntiSemiJoin
											(
											pmp,
											pdrgpcrOuter,
											pcrsScalar,
											pcrsInner,
											pdrgpcrJoinOutput,
											ulCTEJoinId,
											ulCTEOuterId,
											&pdrgpcrProjectOutput
											);
	GPOS_ASSERT(NULL != pdrgpcrProjectOutput);

	DrgDrgPcr *pdrgpdrgpcrUnionInput = GPOS_NEW(pmp) DrgDrgPcr(pmp);
	pdrgpcrJoinOutput->AddRef();
	pdrgpdrgpcrUnionInput->Append(pdrgpcrJoinOutput);
	pdrgpdrgpcrUnionInput->Append(pdrgpcrProjectOutput);
	pdrgpcrJoinOutput->AddRef();

	CExpression *pexprUnionAll =
			GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalUnionAll(pmp, pdrgpcrJoinOutput, pdrgpdrgpcrUnionInput),
					CXformUtils::PexprCTEConsumer(pmp, ulCTEJoinId, pdrgpcrJoinOutput),
					pexprProjectAppendNulls
					);
	CExpression *pexprJoinAnchor = GPOS_NEW(pmp) CExpression
									(
									pmp,
									GPOS_NEW(pmp) CLogicalCTEAnchor(pmp, ulCTEJoinId),
									pexprUnionAll
									);
	CExpression *pexprOuterAnchor = GPOS_NEW(pmp) CExpression
									(
									pmp,
									GPOS_NEW(pmp) CLogicalCTEAnchor(pmp, ulCTEOuterId),
									pexprJoinAnchor
									);
	pexprInnerJoin->Release();

	pxfres->Add(pexprOuterAnchor);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::FValidInnerExpr
//
//	@doc:
//		Check if the inner expression is of a type which should be considered
//		by this xform.
//
//---------------------------------------------------------------------------
BOOL
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::FValidInnerExpr
	(
	CExpression *pexprInner
	)
{
	GPOS_ASSERT(NULL != pexprInner);

	// set of inner operator ids that should not be considered because they usually
	// generate a relatively small number of tuples
	COperator::EOperatorId rgeopids[] =
	{
		COperator::EopLogicalConstTableGet,
		COperator::EopLogicalGbAgg,
		COperator::EopLogicalLimit,
	};

	const COperator::EOperatorId eopid = pexprInner->Pop()->Eopid();
	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(rgeopids); ++ul)
	{
		if (rgeopids[ul] == eopid)
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::PexprLeftAntiSemiJoinWithInnerGroupBy
//
//	@doc:
//		Construct a left anti semi join with the CTE consumer (ulCTEJoinId) as outer
//		and a group by as inner.
//
//---------------------------------------------------------------------------
CExpression *
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::PexprLeftAntiSemiJoinWithInnerGroupBy
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcrOuter,
	DrgPcr *pdrgpcrOuterCopy,
	CColRefSet *pcrsScalar,
	CColRefSet *pcrsInner,
	DrgPcr *pdrgpcrJoinOutput,
	ULONG ulCTEJoinId,
	ULONG ulCTEOuterId
	)
{
	// compute the original outer keys and their correspondent keys on the two branches
	// of the LASJ
	CColRefSet *pcrsOuterKeys = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsOuterKeys->Include(pcrsScalar);
	pcrsOuterKeys->Difference(pcrsInner);
	DrgPcr *pdrgpcrOuterKeys = pcrsOuterKeys->Pdrgpcr(pmp);

	DrgPcr *pdrgpcrConsumer2Output = CUtils::PdrgpcrCopy(pmp, pdrgpcrJoinOutput);
	DrgPul *pdrgpulIndexesOfOuterInGby =
			CDynamicPtrArrayUtils::PdrgpulSubsequenceIndexes(pmp, pdrgpcrOuterKeys, pdrgpcrJoinOutput);
	GPOS_ASSERT(NULL != pdrgpulIndexesOfOuterInGby);
	DrgPcr *pdrgpcrGbyKeys =
			CXformUtils::PdrgpcrReorderedSubsequence(pmp, pdrgpcrConsumer2Output, pdrgpulIndexesOfOuterInGby);

	CExpression *pexprGby =
			GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalGbAgg(pmp, pdrgpcrGbyKeys, COperator::EgbaggtypeGlobal),
				CXformUtils::PexprCTEConsumer(pmp, ulCTEJoinId, pdrgpcrConsumer2Output),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp))
				);

	DrgPul *pdrgpulIndexesOfOuterKeys =
			CDynamicPtrArrayUtils::PdrgpulSubsequenceIndexes(pmp, pdrgpcrOuterKeys, pdrgpcrOuter);
	GPOS_ASSERT(NULL != pdrgpulIndexesOfOuterKeys);
	DrgPcr *pdrgpcrKeysInOuterCopy =
			CXformUtils::PdrgpcrReorderedSubsequence(pmp, pdrgpcrOuterCopy, pdrgpulIndexesOfOuterKeys);

	DrgDrgPcr *pdrgpdrgpcrLASJInput = GPOS_NEW(pmp) DrgDrgPcr(pmp);
	pdrgpdrgpcrLASJInput->Append(pdrgpcrKeysInOuterCopy);
	pdrgpcrGbyKeys->AddRef();
	pdrgpdrgpcrLASJInput->Append(pdrgpcrGbyKeys);

	pcrsOuterKeys->Release();
	pdrgpcrOuterKeys->Release();

	CExpression *pexprLeftAntiSemi =
			GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalLeftAntiSemiJoin(pmp),
					CXformUtils::PexprCTEConsumer(pmp, ulCTEOuterId, pdrgpcrOuterCopy),
					pexprGby,
					CUtils::PexprConjINDFCond(pmp, pdrgpdrgpcrLASJInput)
					);

	pdrgpdrgpcrLASJInput->Release();
	pdrgpulIndexesOfOuterInGby->Release();
	pdrgpulIndexesOfOuterKeys->Release();

	return pexprLeftAntiSemi;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::PexprProjectOverLeftAntiSemiJoin
//
//	@doc:
//		Return a project over a left anti semi join that appends nulls for all
//		columns in the original inner child.
//
//---------------------------------------------------------------------------
CExpression *
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::PexprProjectOverLeftAntiSemiJoin
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcrOuter,
	CColRefSet *pcrsScalar,
	CColRefSet *pcrsInner,
	DrgPcr *pdrgpcrJoinOutput,
	ULONG ulCTEJoinId,
	ULONG ulCTEOuterId,
	DrgPcr **ppdrgpcrProjectOutput
	)
{
	GPOS_ASSERT(NULL != pdrgpcrOuter);
	GPOS_ASSERT(NULL != pcrsScalar);
	GPOS_ASSERT(NULL != pcrsInner);
	GPOS_ASSERT(NULL != pdrgpcrJoinOutput);

	// make a copy of outer for the second CTE consumer (outer of LASJ)
	DrgPcr *pdrgpcrOuterCopy = CUtils::PdrgpcrCopy(pmp, pdrgpcrOuter);

	CExpression *pexprLeftAntiSemi = PexprLeftAntiSemiJoinWithInnerGroupBy
									(
									pmp,
									pdrgpcrOuter,
									pdrgpcrOuterCopy,
									pcrsScalar,
									pcrsInner,
									pdrgpcrJoinOutput,
									ulCTEJoinId,
									ulCTEOuterId
									);

	DrgPul *pdrgpulIndexesOfOuter =
			CDynamicPtrArrayUtils::PdrgpulSubsequenceIndexes(pmp, pdrgpcrOuter, pdrgpcrJoinOutput);
	GPOS_ASSERT(NULL != pdrgpulIndexesOfOuter);

	HMUlCr *phmulcr = GPOS_NEW(pmp) HMUlCr(pmp);
	const ULONG ulOuterCopyLength = pdrgpcrOuterCopy->UlLength();

	for (ULONG ul = 0; ul < ulOuterCopyLength; ++ul)
	{
		ULONG ulOrigIndex = *(*pdrgpulIndexesOfOuter)[ul];
		CColRef *pcrOriginal = (*pdrgpcrJoinOutput)[ulOrigIndex];
#ifdef GPOS_DEBUG
		BOOL fInserted =
#endif
		phmulcr->FInsert(GPOS_NEW(pmp) ULONG(pcrOriginal->UlId()), (*pdrgpcrOuterCopy)[ul]);
		GPOS_ASSERT(fInserted);
	}

	DrgPcr *pdrgpcrInner = pcrsInner->Pdrgpcr(pmp);
	CExpression *pexprProject =
			CUtils::PexprLogicalProjectNulls(pmp, pdrgpcrInner, pexprLeftAntiSemi, phmulcr);

	// compute the output array in the order needed by the union-all above the projection
	*ppdrgpcrProjectOutput =
			CUtils::PdrgpcrRemap(pmp, pdrgpcrJoinOutput, phmulcr, true /*fMustExist*/);

	pdrgpcrInner->Release();
	phmulcr->Release();
	pdrgpulIndexesOfOuter->Release();

	return pexprProject;
}

BOOL
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::IsApplyOnce()
{
	return true;
}
// EOF
