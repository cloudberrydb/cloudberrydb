//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformExpandFullOuterJoin.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/xforms/CXformExpandFullOuterJoin.h"
#include "gpopt/xforms/CXformUtils.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandFullOuterJoin::CXformExpandFullOuterJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformExpandFullOuterJoin::CXformExpandFullOuterJoin
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
				GPOS_NEW(pmp) CLogicalFullOuterJoin(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp)), // outer child
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp)), // inner child
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp)) // scalar child
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformExpandFullOuterJoin::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformExpandFullOuterJoin::Exfp
	(
	CExpressionHandle & //exprhdl
	)
	const
{
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformExpandFullOuterJoin::Transform
//
//	@doc:
//		Actual transformation
// 		The expression A FOJ B is translated to:
//
//		CTEAnchor(cteA)
//		+-- CTEAnchor(cteB)
//			+--UnionAll
//				|--	LOJ
//				|	|--	CTEConsumer(cteA)
//				|	+--	CTEConsumer(cteB)
//				+--	Project
//					+--	LASJ
//					|	|--	CTEConsumer(cteB)
//					|	+--	CTEConsumer(cteA)
//					+-- (NULLS - same schema of A)
//
//		Also, two CTE producers for cteA and cteB are added to CTE info
//
//---------------------------------------------------------------------------
void
CXformExpandFullOuterJoin::Transform
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

	CExpression *pexprA = (*pexpr)[0];
	CExpression *pexprB = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	// 1. create the CTE producers
	const ULONG ulCTEIdA = COptCtxt::PoctxtFromTLS()->Pcteinfo()->UlNextId();
	DrgPcr *pdrgpcrOutA = CDrvdPropRelational::Pdprel(pexprA->PdpDerive())->PcrsOutput()->Pdrgpcr(pmp);
	(void) CXformUtils::PexprAddCTEProducer(pmp, ulCTEIdA, pdrgpcrOutA, pexprA);

	const ULONG ulCTEIdB = COptCtxt::PoctxtFromTLS()->Pcteinfo()->UlNextId();
	DrgPcr *pdrgpcrOutB = CDrvdPropRelational::Pdprel(pexprB->PdpDerive())->PcrsOutput()->Pdrgpcr(pmp);
	(void) CXformUtils::PexprAddCTEProducer(pmp, ulCTEIdB, pdrgpcrOutB, pexprB);

	// 2. create the right child (PROJECT over LASJ)
	DrgPcr *pdrgpcrRightA = CUtils::PdrgpcrCopy(pmp, pdrgpcrOutA);
	DrgPcr *pdrgpcrRightB = CUtils::PdrgpcrCopy(pmp, pdrgpcrOutB);
	CExpression *pexprScalarRight = CXformUtils::PexprRemapColumns
									(
									pmp,
									pexprScalar,
									pdrgpcrOutA,
									pdrgpcrRightA,
									pdrgpcrOutB,
									pdrgpcrRightB
									);
	CExpression *pexprLASJ = PexprLogicalJoinOverCTEs(pmp, EdxljtLeftAntiSemijoin, ulCTEIdB, pdrgpcrRightB, ulCTEIdA, pdrgpcrRightA, pexprScalarRight);
	CExpression *pexprProject = CUtils::PexprLogicalProjectNulls(pmp, pdrgpcrRightA, pexprLASJ);

	// 3. create the left child (LOJ) - this has to use the original output
	//    columns and the original scalar expression
	pexprScalar->AddRef();
	CExpression *pexprLOJ = PexprLogicalJoinOverCTEs(pmp, EdxljtLeft, ulCTEIdA, pdrgpcrOutA, ulCTEIdB, pdrgpcrOutB, pexprScalar);

	// 4. create the UNION ALL expression

	// output columns of the union are the same as the outputs of the first child (LOJ)
	DrgPcr *pdrgpcrOutput = GPOS_NEW(pmp) DrgPcr(pmp);
	pdrgpcrOutput->AppendArray(pdrgpcrOutA);
	pdrgpcrOutput->AppendArray(pdrgpcrOutB);

	// input columns of the union
	DrgDrgPcr *pdrgdrgpcrInput = GPOS_NEW(pmp) DrgDrgPcr(pmp);

	// inputs from the first child (LOJ)
	pdrgpcrOutput->AddRef();
	pdrgdrgpcrInput->Append(pdrgpcrOutput);

	// inputs from the second child have to be in the correct order
	// a. add new computed columns from the project only
	CDrvdPropRelational *pdprelProject = CDrvdPropRelational::Pdprel(pexprProject->PdpDerive());
	CColRefSet *pcrsProjOnly = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsProjOnly->Include(pdprelProject->PcrsOutput());
	pcrsProjOnly->Exclude(pdrgpcrRightB);
	DrgPcr *pdrgpcrProj = pcrsProjOnly->Pdrgpcr(pmp);
	pcrsProjOnly->Release();
	// b. add columns from the LASJ expression
	pdrgpcrProj->AppendArray(pdrgpcrRightB);

	pdrgdrgpcrInput->Append(pdrgpcrProj);

	CExpression *pexprUnionAll = GPOS_NEW(pmp) CExpression
											(
											pmp,
											GPOS_NEW(pmp) CLogicalUnionAll(pmp, pdrgpcrOutput, pdrgdrgpcrInput),
											pexprLOJ,
											pexprProject
											);

	// 5. Add CTE anchor for the B subtree
	CExpression *pexprAnchorB = GPOS_NEW(pmp) CExpression
											(
											pmp,
											GPOS_NEW(pmp) CLogicalCTEAnchor(pmp, ulCTEIdB),
											pexprUnionAll
											);

	// 6. Add CTE anchor for the A subtree
	CExpression *pexprAnchorA = GPOS_NEW(pmp) CExpression
											(
											pmp,
											GPOS_NEW(pmp) CLogicalCTEAnchor(pmp, ulCTEIdA),
											pexprAnchorB
											);

	// add alternative to xform result
	pxfres->Add(pexprAnchorA);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformExpandFullOuterJoin::PexprLogicalJoinOverCTEs
//
//	@doc:
//		Construct a join expression of two CTEs using the given CTE ids
// 		and output columns
//
//---------------------------------------------------------------------------
CExpression *
CXformExpandFullOuterJoin::PexprLogicalJoinOverCTEs
	(
	IMemoryPool *pmp,
	EdxlJoinType edxljointype,
	ULONG ulLeftCTEId,
	DrgPcr *pdrgpcrLeft,
	ULONG ulRightCTEId,
	DrgPcr *pdrgpcrRight,
	CExpression *pexprScalar
	)
	const
{
	GPOS_ASSERT(NULL != pexprScalar);

	DrgPexpr *pdrgpexprChildren = GPOS_NEW(pmp) DrgPexpr(pmp);
	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();

	CLogicalCTEConsumer *popConsumerLeft = GPOS_NEW(pmp) CLogicalCTEConsumer(pmp, ulLeftCTEId, pdrgpcrLeft);
	CExpression *pexprLeft = GPOS_NEW(pmp) CExpression(pmp, popConsumerLeft);
	pcteinfo->IncrementConsumers(ulLeftCTEId);

	CLogicalCTEConsumer *popConsumerRight = GPOS_NEW(pmp) CLogicalCTEConsumer(pmp, ulRightCTEId, pdrgpcrRight);
	CExpression *pexprRight = GPOS_NEW(pmp) CExpression(pmp, popConsumerRight);
	pcteinfo->IncrementConsumers(ulRightCTEId);

	pdrgpexprChildren->Append(pexprLeft);
	pdrgpexprChildren->Append(pexprRight);
	pdrgpexprChildren->Append(pexprScalar);

	return CUtils::PexprLogicalJoin(pmp, edxljointype, pdrgpexprChildren);
}

// EOF
