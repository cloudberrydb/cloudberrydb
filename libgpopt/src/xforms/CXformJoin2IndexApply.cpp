//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformJoin2IndexApply.cpp
//
//	@doc:
//		Implementation of Inner/Outer Join to Apply transform
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpos/common/CDynamicPtrArrayUtils.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/xforms/CSubqueryHandler.h"
#include "gpopt/xforms/CXformJoin2IndexApply.h"
#include "gpopt/xforms/CXformUtils.h"

#include "naucrates/md/IMDIndex.h"

using namespace gpmd;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformJoin2IndexApply::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	CDrvdPropScalar *pdpscalar = exprhdl.Pdpscalar(2 /*ulChildIndex*/);
	GPOS_ASSERT(NULL != pdpscalar);

	if (
		0 == pdpscalar->PcrsUsed()->CElements() ||
		pdpscalar->FHasSubquery() ||
		exprhdl.FHasOuterRefs()
		)
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::ComputeColumnSets
//
//	@doc:
//		Based on the inner and the scalar expression, it computes scalar expression
//		columns, outer references and required columns.
//		Caller does not take ownership of ppcrsScalarExpr.
//		Caller takes ownership of ppcrsOuterRefs and ppcrsReqd.
//
//---------------------------------------------------------------------------
void
CXformJoin2IndexApply::ComputeColumnSets
	(
	IMemoryPool *pmp,
	CExpression *pexprInner,
	CExpression *pexprScalar,
	CColRefSet **ppcrsScalarExpr,
	CColRefSet **ppcrsOuterRefs,
	CColRefSet **ppcrsReqd
	) const
{
	CColRefSet *pcrsInnerOutput = CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->PcrsOutput();
	*ppcrsScalarExpr = CDrvdPropScalar::Pdpscalar(pexprScalar->PdpDerive())->PcrsUsed();
	*ppcrsOuterRefs = GPOS_NEW(pmp) CColRefSet(pmp, **ppcrsScalarExpr);
	(*ppcrsOuterRefs)->Difference(pcrsInnerOutput);

	*ppcrsReqd = GPOS_NEW(pmp) CColRefSet(pmp);
	(*ppcrsReqd)->Include(pcrsInnerOutput);
	(*ppcrsReqd)->Include(*ppcrsScalarExpr);
	(*ppcrsReqd)->Difference(*ppcrsOuterRefs);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::CreateFullIndexApplyAlternatives
//
//	@doc:
//		Helper to add IndexApply expression to given xform results container
//
//---------------------------------------------------------------------------
void
CXformJoin2IndexApply::CreateHomogeneousIndexApplyAlternatives
	(
	IMemoryPool *pmp,
	ULONG ulOriginOpId,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	CExpression *pexprScalar,
	CTableDescriptor *ptabdescInner,
	CLogicalDynamicGet *popDynamicGet,
	CXformResult *pxfres,
	IMDIndex::EmdindexType emdtype
	) const
{
	GPOS_ASSERT(NULL != pexprOuter);
	GPOS_ASSERT(NULL != pexprInner);
	GPOS_ASSERT(NULL != pexprScalar);
	GPOS_ASSERT(NULL != ptabdescInner);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(IMDIndex::EmdindBtree == emdtype || IMDIndex::EmdindBitmap == emdtype);

	const ULONG ulIndices = ptabdescInner->UlIndices();
	if (0 == ulIndices)
	{
		return;
	}

	// derive the scalar and relational properties to build set of required columns
	CColRefSet *pcrsScalarExpr = NULL;
	CColRefSet *pcrsOuterRefs = NULL;
	CColRefSet *pcrsReqd = NULL;
	ComputeColumnSets
		(
		pmp,
		pexprInner,
		pexprScalar,
		&pcrsScalarExpr,
		&pcrsOuterRefs,
		&pcrsReqd
		);

	if (IMDIndex::EmdindBtree == emdtype)
	{
		CreateHomogeneousBtreeIndexApplyAlternatives
			(
			pmp,
			ulOriginOpId,
			pexprOuter,
			pexprInner,
			pexprScalar,
			ptabdescInner,
			popDynamicGet,
			pcrsScalarExpr,
			pcrsOuterRefs,
			pcrsReqd,
			ulIndices,
			pxfres
			);
	}
	else
	{
		CreateHomogeneousBitmapIndexApplyAlternatives
			(
			pmp,
			ulOriginOpId,
			pexprOuter,
			pexprInner,
			pexprScalar,
			ptabdescInner,
			pcrsOuterRefs,
			pcrsReqd,
			pxfres
			);
	}

	//clean-up
	pcrsReqd->Release();
	pcrsOuterRefs->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::CreateHomogeneousBtreeIndexApplyAlternatives
//
//	@doc:
//		Helper to add IndexApply expression to given xform results container
//		for homogeneous b-tree indexes
//
//---------------------------------------------------------------------------
void
CXformJoin2IndexApply::CreateHomogeneousBtreeIndexApplyAlternatives
	(
	IMemoryPool *pmp,
	ULONG ulOriginOpId,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	CExpression *pexprScalar,
	CTableDescriptor *ptabdescInner,
	CLogicalDynamicGet *popDynamicGet,
	CColRefSet *pcrsScalarExpr,
	CColRefSet *pcrsOuterRefs,
	CColRefSet *pcrsReqd,
	ULONG ulIndices,
	CXformResult *pxfres
	) const
{
	// array of expressions in the scalar expression
	DrgPexpr *pdrgpexpr = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprScalar);
	GPOS_ASSERT(pdrgpexpr->UlLength() > 0);

	// find the indexes whose included columns meet the required columns
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = pmda->Pmdrel(ptabdescInner->Pmdid());

	for (ULONG ul = 0; ul < ulIndices; ul++)
	{
		IMDId *pmdidIndex = pmdrel->PmdidIndex(ul);
		const IMDIndex *pmdindex = pmda->Pmdindex(pmdidIndex);

		// distribution keys and btree index keys must match each other for left outer join
		// Given:
		//
		// R (a, b, c) distributed by (a)
		// S (a, b, c) distributed by (a), btree index1 on S(a), btree index2 on S(b)
		//
		// R LOJ S on R.a=S.a and R.c=S.c, we are good to do outer index NL join.
		// R LOJ S on R.a=S.a and R.b=S.b, we need to take care that in non-distributed
		// scenario, it is fine to choose either condition as the index scan condition,
		// and the other one as the index scan filter. But in distributed scenario,
		// we have to choose R.a=S.a as the index scan condition, R.b=S.b as the index
		// scan filter, because of Orca issue #309.
		// TODO: remove the constraint when issue #309 is fixed:
		// https://github.com/greenplum-db/gporca/issues/309
		if (m_fOuterJoin && !FMatchDistKeyAndIndexKey(pmdrel, pmdindex))
		{
			continue;
		}

		CPartConstraint *ppartcnstrIndex = NULL;
		if (NULL != popDynamicGet)
		{
			ppartcnstrIndex = CUtils::PpartcnstrFromMDPartCnstr
									(
									pmp,
									COptCtxt::PoctxtFromTLS()->Pmda(),
									popDynamicGet->PdrgpdrgpcrPart(),
									pmdindex->Pmdpartcnstr(),
									popDynamicGet->PdrgpcrOutput()
									);
		}
		CreateAlternativesForBtreeIndex
			(
			pmp,
			ulOriginOpId,
			pexprOuter,
			pexprInner,
			pmda,
			pdrgpexpr,
			pcrsScalarExpr,
			pcrsOuterRefs,
			pcrsReqd,
			pmdrel,
			pmdindex,
			ppartcnstrIndex,
			pxfres
			);
	}

	//clean-up
	pdrgpexpr->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::CreateAlternativesForBtreeIndex
//
//	@doc:
//		Helper to add IndexApply expression to given xform results container
//		for homogeneous b-tree indexes.
//
//---------------------------------------------------------------------------
void
CXformJoin2IndexApply::CreateAlternativesForBtreeIndex
	(
	IMemoryPool *pmp,
	ULONG ulOriginOpId,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	CMDAccessor *pmda,
	DrgPexpr *pdrgpexprConjuncts,
	CColRefSet *pcrsScalarExpr,
	CColRefSet *pcrsOuterRefs,
	CColRefSet *pcrsReqd,
	const IMDRelation *pmdrel,
	const IMDIndex *pmdindex,
	CPartConstraint *ppartcnstrIndex,
	CXformResult *pxfres
	) const
{
	CExpression *pexprLogicalIndexGet = CXformUtils::PexprLogicalIndexGet
						(
						 pmp,
						 pmda,
						 pexprInner,
						 ulOriginOpId,
						 pdrgpexprConjuncts,
						 pcrsReqd,
						 pcrsScalarExpr,
						 pcrsOuterRefs,
						 pmdindex,
						 pmdrel,
						 false /*fAllowPartialIndex*/,
						 ppartcnstrIndex
						);
	if (NULL != pexprLogicalIndexGet)
	{
		// second child has residual predicates, create an apply of outer and inner
		// and add it to xform results
		DrgPcr *pdrgpcr = pcrsOuterRefs->Pdrgpcr(pmp);
		pexprOuter->AddRef();
		CExpression *pexprIndexApply =
			GPOS_NEW(pmp) CExpression
				(
				pmp,
				PopLogicalApply(pmp, pdrgpcr),
				pexprOuter,
				pexprLogicalIndexGet,
				CPredicateUtils::PexprConjunction(pmp, NULL /*pdrgpexpr*/)
				);
		pxfres->Add(pexprIndexApply);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::CreateHomogeneousBitmapIndexApplyAlternatives
//
//	@doc:
//		Helper to add IndexApply expression to given xform results container
//		for homogeneous bitmap indexes.
//
//---------------------------------------------------------------------------
void CXformJoin2IndexApply::CreateHomogeneousBitmapIndexApplyAlternatives
	(
	IMemoryPool *pmp,
	ULONG ulOriginOpId,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	CExpression *pexprScalar,
	CTableDescriptor *ptabdescInner,
	CColRefSet *pcrsOuterRefs,
	CColRefSet *pcrsReqd,
	CXformResult *pxfres
	) const
{
	if (m_fOuterJoin)
	{
		// find the indexes whose included columns meet the required columns
		CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
		const IMDRelation *pmdrel = pmda->Pmdrel(ptabdescInner->Pmdid());
		const ULONG ulIndices = ptabdescInner->UlIndices();
		BOOL fIndexApplicable = false;

		for (ULONG ul = 0; ul < ulIndices; ul++)
		{
			IMDId *pmdidIndex = pmdrel->PmdidIndex(ul);
			const IMDIndex *pmdindex = pmda->Pmdindex(pmdidIndex);

			// there must be at least 1 index matches the distribution keys
			// for left out join index apply.
			// TODO: remove this constraint when issue #309 is fixed:
			// https://github.com/greenplum-db/gporca/issues/309
			if (FMatchDistKeyAndIndexKey(pmdrel, pmdindex))
			{
				fIndexApplicable = true;
				break;
			}
		}

		if (!fIndexApplicable)
		{
			return;
		}
	}

	CLogical *popGet = CLogical::PopConvert(pexprInner->Pop());
	CExpression *pexprLogicalIndexGet = CXformUtils::PexprBitmapTableGet
										(
										pmp,
										popGet,
										ulOriginOpId,
										ptabdescInner,
										pexprScalar,
										pcrsOuterRefs,
										pcrsReqd
										);
	if (NULL != pexprLogicalIndexGet)
	{
		// second child has residual predicates, create an apply of outer and inner
		// and add it to xform results
		DrgPcr *pdrgpcr = pcrsOuterRefs->Pdrgpcr(pmp);
		pexprOuter->AddRef();
		CExpression *pexprIndexApply =
			GPOS_NEW(pmp) CExpression
				(
				pmp,
				PopLogicalApply(pmp, pdrgpcr),
				pexprOuter,
				pexprLogicalIndexGet,
				CPredicateUtils::PexprConjunction(pmp, NULL /*pdrgpexpr*/)
				);
		pxfres->Add(pexprIndexApply);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::CreatePartialIndexApplyAlternatives
//
//	@doc:
//		Helper to add IndexApply expressions to given xform results container
//		for partial indexes. For the partitions without indexes we add one
//		regular inner join.
//		For instance, if there are two partial indexes plus some partitions
//		not covered by any index, the result of this xform on:
//
//    +--CLogicalInnerJoin
//      |--CLogicalGet "my_tt_agg_small", Columns: ["symbol" (0), "event_ts" (1)]
//      |--CLogicalDynamicGet "my_tq_agg_small_part", Columns: ["ets" (11), "end_ts" (15)]
//      +--CScalarBoolOp (EboolopAnd)
//        |--CScalarCmp (>=)
//        |  |--CScalarIdent "event_ts" (1)
//        |  +--CScalarIdent "ets" (11)
//        +--CScalarCmp (<)
//           |--CScalarIdent "event_ts" (1)
//           +--CScalarIdent "end_ts" (15)
//
//    will look like:
//
//    +--CLogicalCTEAnchor (0)
//       +--CLogicalUnionAll ["symbol" (0), "event_ts" (1),  ...]
//        |--CLogicalUnionAll ["symbol" (0), "event_ts" (1),  ...]
//        |  |--CLogicalInnerIndexApply
//        |  |  |--CLogicalCTEConsumer (0), Columns: ["symbol" (0), "event_ts" (1), "gp_segment_id" (10)]
//        |  |  |--CLogicalDynamicIndexGet   Index Name: ("my_tq_agg_small_part_ets_end_ts_ix_1"),
//        |  |  |      Columns: ["ets" (11), "end_ts" (15) "gp_segment_id" (22)]
//        |  |  |  |--CScalarBoolOp (EboolopAnd)
//        |  |  |  |  |--CScalarCmp (<=)
//        |  |  |  |  |  |--CScalarIdent "ets" (11)
//        |  |  |  |  |  +--CScalarIdent "event_ts" (1)
//        |  |  |  |  +--CScalarCmp (>)
//        |  |  |  |     |--CScalarIdent "end_ts" (15)
//        |  |  |  |     +--CScalarIdent "event_ts" (1)
//        |  |  |  +--CScalarConst (TRUE)
//        |  |  +--CScalarConst (TRUE)
//        |  +--CLogicalInnerIndexApply
//        |     |--CLogicalCTEConsumer (0), Columns: ["symbol" (35), "event_ts" (36), "gp_segment_id" (45)]
//        |     |--CLogicalDynamicIndexGet   Index Name: ("my_tq_agg_small_part_ets_end_ts_ix_2"),
//        |     |    Columns: ["ets" (46),  "end_ts" (50), "gp_segment_id" (57)]
//        |     |  |--CScalarCmp (<=)
//        |     |  |  |--CScalarIdent "ets" (46)
//        |     |  |  +--CScalarIdent "event_ts" (36)
//        |     |  +--CScalarCmp (<)
//        |     |     |--CScalarIdent "event_ts" (36)
//        |     |     +--CScalarIdent "end_ts" (50)
//        |     +--CScalarConst (TRUE)
//        +--CLogicalInnerJoin
//             |--CLogicalCTEConsumer (0), Columns: ["symbol" (58), "event_ts" (59), "gp_segment_id" (68)]
//             |--CLogicalDynamicGet "my_tq_agg_small_part" ,
//            Columns: ["ets" (69), "end_ts" (73), "gp_segment_id" (80)]
//               +--CScalarBoolOp (EboolopAnd)
//               |--CScalarCmp (>=)
//               |  |--CScalarIdent "event_ts" (59)
//               |  +--CScalarIdent "ets" (69)
//               +--CScalarCmp (<)
//                  |--CScalarIdent "event_ts" (59)
//                  +--CScalarIdent "end_ts" (73)
//
//---------------------------------------------------------------------------
void
CXformJoin2IndexApply::CreatePartialIndexApplyAlternatives
	(
	IMemoryPool *pmp,
	ULONG ulOriginOpId,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	CExpression *pexprScalar,
	CTableDescriptor *ptabdescInner,
	CLogicalDynamicGet *popDynamicGet,
	CXformResult *pxfres
	) const
{
	GPOS_ASSERT(NULL != pexprOuter);
	GPOS_ASSERT(NULL != pexprInner);
	GPOS_ASSERT(NULL != pexprScalar);
	GPOS_ASSERT(NULL != ptabdescInner);
	GPOS_ASSERT(NULL != pxfres);

	if (NULL == popDynamicGet || popDynamicGet->FPartial())
	{
		// not a dynamic get or
		// already a partial dynamic get; do not try to split further
		return;
	}

	const ULONG ulIndices = ptabdescInner->UlIndices();
	if (0 == ulIndices)
	{
		return;
	}

	CPartConstraint *ppartcnstr = popDynamicGet->Ppartcnstr();
	ppartcnstr->AddRef();

	CColRefSet *pcrsScalarExpr = NULL;
	CColRefSet *pcrsOuterRefs = NULL;
	CColRefSet *pcrsReqd = NULL;
	ComputeColumnSets
		(
		pmp,
		pexprInner,
		pexprScalar,
		&pcrsScalarExpr,
		&pcrsOuterRefs,
		&pcrsReqd
		);

	// find a candidate set of partial index combinations
    CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
    const IMDRelation *pmdrel = pmda->Pmdrel(ptabdescInner->Pmdid());
	
    // array of expressions in the scalar expression
	DrgPexpr *pdrgpexpr = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprScalar);
	
	GPOS_ASSERT(0 < pdrgpexpr->UlLength());	
	
	DrgPdrgPpartdig *pdrgpdrgppartdig = CXformUtils::PdrgpdrgppartdigCandidates
										(
										pmp,
										COptCtxt::PoctxtFromTLS()->Pmda(),
										pdrgpexpr,
										popDynamicGet->PdrgpdrgpcrPart(),
										pmdrel,
										ppartcnstr,
										popDynamicGet->PdrgpcrOutput(),
										pcrsReqd,
										pcrsScalarExpr,
										pcrsOuterRefs
										);



	// construct alternative partial index apply plans
	const ULONG ulCandidates = pdrgpdrgppartdig->UlLength();
	for (ULONG ul = 0; ul < ulCandidates; ul++)
	{
		DrgPpartdig *pdrgppartdig = (*pdrgpdrgppartdig)[ul];
		if (0 < pdrgppartdig->UlLength())
		{
			CreatePartialIndexApplyPlan
				(
				pmp,
				ulOriginOpId,
				pexprOuter,
				pexprScalar,
				pcrsOuterRefs,
				popDynamicGet,
				pdrgppartdig,
				pmdrel,
				pxfres
				);
		}
	}

	ppartcnstr->Release();
	pdrgpdrgppartdig->Release();
	pcrsReqd->Release();
	pcrsOuterRefs->Release();
	pdrgpexpr->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::CreatePartialIndexApplyPlan
//
//	@doc:
//		Create a plan as a union of the given partial index apply candidates and
//		possibly a regular inner join with a dynamic table scan on the inner branch.
//
//---------------------------------------------------------------------------
void
CXformJoin2IndexApply::CreatePartialIndexApplyPlan
	(
	IMemoryPool *pmp,
	ULONG ulOriginOpId,
	CExpression *pexprOuter,
	CExpression *pexprScalar,
	CColRefSet *pcrsOuterRefs,
	CLogicalDynamicGet *popDynamicGet,
	DrgPpartdig *pdrgppartdig,
	const IMDRelation *pmdrel,
	CXformResult *pxfres
	) const
{
	const ULONG ulPartialIndexes = pdrgppartdig->UlLength();
	if (0 == ulPartialIndexes)
	{
		return;
	}

	DrgPcr *pdrgpcrGet = popDynamicGet->PdrgpcrOutput();
	const ULONG ulCTEId = COptCtxt::PoctxtFromTLS()->Pcteinfo()->UlNextId();

	// outer references mentioned in the scan filter: we need them to generate the IndexApply
	DrgPcr *pdrgpcrOuterRefsInScan = pcrsOuterRefs->Pdrgpcr(pmp);

	// columns from the outer branch of the IndexApply or Join
	// we will create copies of these columns because every CTE consumer needs to have its
	// own column ids
	DrgPcr *pdrgpcrOuter = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput()->Pdrgpcr(pmp);

	// positions in pdrgpcrOuter of the outer references mentioned in the scan filter
	// we need them because each time we create copies of pdrgpcrOuter, we will extract the
	// subsequence corresponding to pdrgpcrOuterRefsInScan
	DrgPul *pdrgpulIndexesOfRefsInScan =
			CDynamicPtrArrayUtils::PdrgpulSubsequenceIndexes(pmp, pdrgpcrOuterRefsInScan, pdrgpcrOuter);

	GPOS_ASSERT(NULL != pdrgpulIndexesOfRefsInScan);

	(void) CXformUtils::PexprAddCTEProducer(pmp, ulCTEId, pdrgpcrOuter, pexprOuter);
	BOOL fFirst = true;

	DrgPcr *pdrgpcrOutput = NULL;
	DrgPexpr *pdrgpexprInput = GPOS_NEW(pmp) DrgPexpr(pmp);
	DrgDrgPcr *pdrgpdrgpcrInput = GPOS_NEW(pmp) DrgDrgPcr(pmp);

	for (ULONG ul = 0; ul < ulPartialIndexes; ul++)
	{
		SPartDynamicIndexGetInfo *ppartdig = (*pdrgppartdig)[ul];

		const IMDIndex *pmdindex = ppartdig->m_pmdindex;
		CPartConstraint *ppartcnstr = ppartdig->m_ppartcnstr;
		DrgPexpr *pdrgpexprIndex = ppartdig->m_pdrgpexprIndex;
		DrgPexpr *pdrgpexprResidual = ppartdig->m_pdrgpexprResidual;
		
		DrgPcr *pdrgpcrOuterNew = pdrgpcrOuter;
		DrgPcr *pdrgpcrIndexGet = pdrgpcrGet;
		HMUlCr *phmulcr = NULL;
		if (fFirst)
		{
			// For the first child of the union, reuse the initial columns
			// because the output schema of the union must be identical to its first child's.
			pdrgpcrIndexGet->AddRef();
		}
		else
		{
			phmulcr = GPOS_NEW(pmp) HMUlCr(pmp);
			pdrgpcrOuterNew = CUtils::PdrgpcrCopy(pmp, pdrgpcrOuter, false /*fAllComputed*/, phmulcr);
			pdrgpcrIndexGet = CUtils::PdrgpcrCopy(pmp, pdrgpcrGet, false /*fAllComputed*/, phmulcr);
		}

		CExpression *pexprUnionAllChild = NULL;
		if (NULL != pmdindex)
		{
			pexprUnionAllChild = PexprIndexApplyOverCTEConsumer
					(
					pmp,
					ulOriginOpId,
					popDynamicGet,
					pdrgpexprIndex,
					pdrgpexprResidual,
					pdrgpcrIndexGet,
					pmdindex,
					pmdrel,
					fFirst,
					ulCTEId,
					ppartcnstr,
					pcrsOuterRefs,
					pdrgpcrOuter,
					pdrgpcrOuterNew,
					pdrgpcrOuterRefsInScan,
					pdrgpulIndexesOfRefsInScan
					);
		}
		else
		{
			pexprUnionAllChild = PexprJoinOverCTEConsumer
								(
								pmp,
								ulOriginOpId,
								popDynamicGet,
								ulCTEId,
								pexprScalar,
								pdrgpcrIndexGet,
								ppartcnstr,
								pdrgpcrOuter,
								pdrgpcrOuterNew
								);
		}

		CRefCount::SafeRelease(pdrgpcrIndexGet);

		// if we failed to create a DynamicIndexScan, we give up
		GPOS_ASSERT(NULL != pexprUnionAllChild);

		DrgPcr *pdrgpcrNew = NULL;
		if (NULL == phmulcr)
		{
			pdrgpcrNew = CDrvdPropRelational::Pdprel(pexprUnionAllChild->PdpDerive())->PcrsOutput()->Pdrgpcr(pmp);
		}
		else
		{
			// inner branches of the union-all needs columns in the same order as in the outer branch
			pdrgpcrNew = CUtils::PdrgpcrRemap(pmp, pdrgpcrOutput, phmulcr, true /*fMustExist*/);
		}

		if (fFirst)
		{
			GPOS_ASSERT(NULL != pdrgpcrNew);

			// the output columns of a UnionAll need to be exactly the input
			// columns of its first input branch
			pdrgpcrNew->AddRef();
			pdrgpcrOutput = pdrgpcrNew;
		}
		fFirst = false;

		pdrgpdrgpcrInput->Append(pdrgpcrNew);
		pdrgpexprInput->Append(pexprUnionAllChild);

		CRefCount::SafeRelease(phmulcr);
	}

	GPOS_ASSERT(pdrgpexprInput->UlLength() == pdrgpdrgpcrInput->UlLength());
	GPOS_ASSERT(1 < pdrgpexprInput->UlLength());

	CExpression *pexprResult = NULL;
	if (2 <= pdrgpexprInput->UlLength())
	{
		// construct a new union operator
		pexprResult = GPOS_NEW(pmp) CExpression
								(
								pmp,
								GPOS_NEW(pmp) CLogicalUnionAll(pmp, pdrgpcrOutput, pdrgpdrgpcrInput, popDynamicGet->UlScanId()),
								pdrgpexprInput
								);
	}
	else
	{
		GPOS_ASSERT(1 == pdrgpexprInput->UlLength());
		pexprResult = (*pdrgpexprInput)[0];
		pexprResult->AddRef();

		// clean up
		pdrgpdrgpcrInput->Release();
		pdrgpexprInput->Release();
		pdrgpcrOutput->Release();
	}

	pdrgpcrOuterRefsInScan->Release();
	pdrgpulIndexesOfRefsInScan->Release();

	AddUnionPlanForPartialIndexes(pmp, popDynamicGet, ulCTEId, pexprResult, pexprScalar, pxfres);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::PexprJoinOverCTEConsumer
//
//	@doc:
//		Create an join with a CTE consumer on the inner branch,
//		with the given partition constraint
//
//---------------------------------------------------------------------------
CExpression *
CXformJoin2IndexApply::PexprJoinOverCTEConsumer
	(
	IMemoryPool *pmp,
	ULONG, //  ulOriginOpId
	CLogicalDynamicGet *popDynamicGet,
	ULONG ulCTEId,
	CExpression *pexprScalar,
	DrgPcr *pdrgpcrDynamicGet,
	CPartConstraint *ppartcnstr,
	DrgPcr *pdrgpcrOuter,
	DrgPcr *pdrgpcrOuterNew
	) const
{
	HMUlCr *phmulcr =
			CUtils::PhmulcrMapping(pmp, popDynamicGet->PdrgpcrOutput(), pdrgpcrDynamicGet);

	// construct a partial dynamic get with the negated constraint
	CPartConstraint *ppartcnstrPartialDynamicGet =
			ppartcnstr->PpartcnstrCopyWithRemappedColumns(pmp, phmulcr, true /*fMustExist*/);

	CLogicalDynamicGet *popPartialDynamicGet =
			(CLogicalDynamicGet *) popDynamicGet->PopCopyWithRemappedColumns
													(
													pmp,
													phmulcr,
													true /*fMustExist*/
													);
	popPartialDynamicGet->SetPartConstraint(ppartcnstrPartialDynamicGet);
	popPartialDynamicGet->SetSecondaryScanId(COptCtxt::PoctxtFromTLS()->UlPartIndexNextVal());
	popPartialDynamicGet->SetPartial();

	// if there are any outer references, add them to the mapping
	ULONG ulOuterPcrs = pdrgpcrOuter->UlLength();
	GPOS_ASSERT(ulOuterPcrs == pdrgpcrOuterNew->UlLength());

	for (ULONG ul = 0; ul < ulOuterPcrs; ul++)
	{
		CColRef *pcrOld = (*pdrgpcrOuter)[ul];
		CColRef *pcrNew = (*pdrgpcrOuterNew)[ul];
#ifdef GPOS_DEBUG
		BOOL fInserted =
#endif
		phmulcr->FInsert(GPOS_NEW(pmp) ULONG(pcrOld->UlId()), pcrNew);
		GPOS_ASSERT(fInserted);
	}

	CExpression *pexprJoin = GPOS_NEW(pmp) CExpression
								(
								pmp,
								PopLogicalJoin(pmp),
								CXformUtils::PexprCTEConsumer(pmp, ulCTEId, pdrgpcrOuterNew),
								GPOS_NEW(pmp) CExpression(pmp, popPartialDynamicGet),
								pexprScalar->PexprCopyWithRemappedColumns
											(
											pmp,
											phmulcr,
											true /*fMustExist*/
											)
								);
	phmulcr->Release();

	return pexprJoin;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::PexprIndexApplyOverCTEConsumer
//
//	@doc:
//		Create an index apply with a CTE consumer on the outer branch
//		and a dynamic get on the inner one
//
//---------------------------------------------------------------------------
CExpression *
CXformJoin2IndexApply::PexprIndexApplyOverCTEConsumer
	(
	IMemoryPool *pmp,
	ULONG ulOriginOpId,
	CLogicalDynamicGet *popDynamicGet,
	DrgPexpr *pdrgpexprIndex,
	DrgPexpr *pdrgpexprResidual,
	DrgPcr *pdrgpcrIndexGet,
	const IMDIndex *pmdindex,
	const IMDRelation *pmdrel,
	BOOL fFirst,
	ULONG ulCTEId,
	CPartConstraint *ppartcnstr,
	CColRefSet *pcrsOuterRefs,
	DrgPcr *pdrgpcrOuter,
	DrgPcr *pdrgpcrOuterNew,
	DrgPcr *pdrgpcrOuterRefsInScan,
	DrgPul *pdrgpulIndexesOfRefsInScan
	) const
{
	CExpression *pexprDynamicScan = CXformUtils::PexprPartialDynamicIndexGet
								(
								pmp,
								popDynamicGet,
								ulOriginOpId,
								pdrgpexprIndex,
								pdrgpexprResidual,
								pdrgpcrIndexGet,
								pmdindex,
								pmdrel,
								ppartcnstr,
								pcrsOuterRefs,
								pdrgpcrOuter,
								pdrgpcrOuterNew
								);

	if (NULL == pexprDynamicScan)
	{
		return NULL;
	}

	// create an apply of outer and inner and add it to the union
	DrgPcr *pdrgpcrOuterRefsInScanNew = pdrgpcrOuterRefsInScan;
	if (fFirst)
	{
		pdrgpcrOuterRefsInScanNew->AddRef();
	}
	else
	{
		pdrgpcrOuterRefsInScanNew =
				CXformUtils::PdrgpcrReorderedSubsequence(pmp, pdrgpcrOuterNew, pdrgpulIndexesOfRefsInScan);
	}

	return GPOS_NEW(pmp) CExpression
			(
			pmp,
			PopLogicalApply(pmp, pdrgpcrOuterRefsInScanNew),
			CXformUtils::PexprCTEConsumer(pmp, ulCTEId, pdrgpcrOuterNew),
			pexprDynamicScan,
			CPredicateUtils::PexprConjunction(pmp, NULL /*pdrgpexpr*/)
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::PexprConstructUnionAll
//
//	@doc:
//		Create a union-all with the given children. Takes ownership of all
//		arguments.
//
//---------------------------------------------------------------------------
CExpression *
CXformJoin2IndexApply::PexprConstructUnionAll
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcrLeftSchema,
	DrgPcr *pdrgpcrRightSchema,
	CExpression *pexprLeftChild,
	CExpression *pexprRightChild,
	ULONG ulScanId
	) const
{
	DrgDrgPcr *pdrgpdrgpcrInput = GPOS_NEW(pmp) DrgDrgPcr(pmp);
	pdrgpdrgpcrInput->Append(pdrgpcrLeftSchema);
	pdrgpdrgpcrInput->Append(pdrgpcrRightSchema);
	pdrgpcrLeftSchema->AddRef();

	return GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalUnionAll(pmp, pdrgpcrLeftSchema, pdrgpdrgpcrInput, ulScanId),
				pexprLeftChild,
				pexprRightChild
				);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoin2IndexApply::AddUnionPlanForPartialIndexes
//
//	@doc:
//		Constructs a CTE Anchor over the given UnionAll and adds it to the given
//		Xform result
//
//---------------------------------------------------------------------------
void
CXformJoin2IndexApply::AddUnionPlanForPartialIndexes
	(
	IMemoryPool *pmp,
	CLogicalDynamicGet *popDynamicGet,
	ULONG ulCTEId,
	CExpression *pexprUnion,
	CExpression *pexprScalar,
	CXformResult *pxfres
	) const
{
	if (NULL == pexprUnion)
	{
		return;
	}

	// if scalar expression involves the partitioning key, keep a SELECT node
	// on top for the purposes of partition selection
	DrgDrgPcr *pdrgpdrgpcrPartKeys = popDynamicGet->PdrgpdrgpcrPart();
	CExpression *pexprPredOnPartKey = CPredicateUtils::PexprExtractPredicatesOnPartKeys
										(
										pmp,
										pexprScalar,
										pdrgpdrgpcrPartKeys,
										NULL, /*pcrsAllowedRefs*/
										true /*fUseConstraints*/
										);

	if (NULL != pexprPredOnPartKey)
	{
		pexprUnion = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalSelect(pmp), pexprUnion, pexprPredOnPartKey);
	}

	CExpression *pexprAnchor = GPOS_NEW(pmp) CExpression
									(
									pmp,
									GPOS_NEW(pmp) CLogicalCTEAnchor(pmp, ulCTEId),
									pexprUnion
									);
	pxfres->Add(pexprAnchor);
}

// check whether distribution key and the index key are matched.
// always returns true for master only table.
BOOL
CXformJoin2IndexApply::FMatchDistKeyAndIndexKey
	(
	const IMDRelation *pmdrel,
	const IMDIndex *pmdindex
	) const
{
	if (pmdrel->Ereldistribution() == IMDRelation::EreldistrMasterOnly)
	{
		return true;
	}

	ULONG ulLen = pmdrel->UlDistrColumns();
	if (ulLen != pmdindex->UlKeys())
	{
		return false;
	}

	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		const IMDColumn *pmdCol = pmdrel->PmdcolDistrColumn(ul);
		ULONG ulPos = pmdrel->UlPosFromAttno(pmdCol->IAttno());
		if (pmdindex->UlPosInKey(ulPos) == gpos::ulong_max)
		{
			return false;
		}
	}

	return true;
}

// EOF

