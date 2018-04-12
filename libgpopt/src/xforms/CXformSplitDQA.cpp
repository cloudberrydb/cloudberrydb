//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal Inc.
//
//	@filename:
//		CXformSplitDQA.cpp
//
//	@doc:
//		Implementation of the splitting of an aggregate into a three levels -- namely,
//		local, intermediate and global, aggregate
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CColumnFactory.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/xforms/CXformSplitDQA.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "naucrates/md/IMDAggregate.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::CXformSplitDQA
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSplitDQA::CXformSplitDQA
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
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // relational child
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))  // scalar project list
					)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSplitDQA::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	// do not split aggregate if it is not a global aggregate,  has no distinct aggs, has MDQAs, has outer references,
	// or return types of Agg functions are ambiguous
	if (!CLogicalGbAgg::PopConvert(exprhdl.Pop())->FGlobal() ||
		0 == exprhdl.Pdpscalar(1 /*ulChildIndex*/)->UlDistinctAggs() ||
		exprhdl.Pdpscalar(1 /*ulChildIndex*/)->FHasMultipleDistinctAggs() ||
		0 < CDrvdPropRelational::Pdprel(exprhdl.Pdp())->PcrsOuter()->CElements() ||
		CXformUtils::FHasAmbiguousType(exprhdl.PexprScalarChild(1 /*ulChildIndex*/), COptCtxt::PoctxtFromTLS()->Pmda())
		)
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::Transform
//
//	@doc:
//		Actual transformation to expand a global aggregate into local,
//		intermediate and global aggregates
//
//---------------------------------------------------------------------------
void
CXformSplitDQA::Transform
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

	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	IMemoryPool *pmp = pxfctxt->Pmp();

	// extract components
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprProjectList = (*pexpr)[1];

	HMExprCr *phmexprcr = GPOS_NEW(pmp) HMExprCr(pmp);
	DrgPexpr *pdrgpexprChildPrEl = GPOS_NEW(pmp) DrgPexpr(pmp);
	DrgPcr *pdrgpcrArgDQA = NULL;

	ExtractDistinctCols
				(
				pmp,
				pcf,
				pmda,
				pexprProjectList,
				pdrgpexprChildPrEl,
				phmexprcr,
				&pdrgpcrArgDQA
				);

	if (NULL == pdrgpcrArgDQA)
	{
		// failed to extract distinct columns
		phmexprcr->Release();
		pdrgpexprChildPrEl->Release();

		return;
	}


	if (0 < pdrgpexprChildPrEl->UlLength())
	{
		pexprRelational->AddRef();

		// computed columns referred to in the DQA
		CExpression *pexprChildProject = CUtils::PexprLogicalProject
													(
													pmp,
													pexprRelational,
													GPOS_NEW(pmp) CExpression
																(
																pmp,
																GPOS_NEW(pmp) CScalarProjectList(pmp),
																pdrgpexprChildPrEl
																),
													true /*fNewComputedCol*/
													);
		pexprRelational = pexprChildProject;
	}

	// multi-stage for both scalar and non-scalar aggregates.
	CExpression *pexprAlt1 = PexprSplitHelper
								(
								pmp,
								pcf,
								pmda,
								pexpr,
								pexprRelational,
								phmexprcr,
								pdrgpcrArgDQA,
								false /*fSpillTo2Level*/
								);
        
	pxfres->Add(pexprAlt1);

	DrgPcr *pDrgPcr = CLogicalGbAgg::PopConvert(pexpr->Pop())->Pdrgpcr();
	BOOL fScalarDQA = (pDrgPcr == NULL || pDrgPcr->UlLength() == 0);
	BOOL fForce3StageScalarDQA = GPOS_FTRACE(EopttraceForceThreeStageScalarDQA);
	if (!(fForce3StageScalarDQA && fScalarDQA)) {
		// we skip this option if it is a Scalar DQA and we only want plans with 3-stages of aggregation

		// local/global for both scalar and non-scalar aggregates.
		CExpression *pexprAlt2 = PexprSplitIntoLocalDQAGlobalAgg
				(
				pmp,
				pcf,
				pmda,
				pexpr,
				pexprRelational,
				phmexprcr,
				pdrgpcrArgDQA
				);

		pxfres->Add(pexprAlt2);
	}

	if (fScalarDQA && !fForce3StageScalarDQA) {
		// if only want 3-stage DQA then skip this 2-stage option for scalar DQA.

		// special case for 'scalar DQA' only, transform to 2-stage aggregate.
		// It's beneficial for distinct column same as distributed column.
		CExpression *pexprAlt3 = PexprSplitHelper
				(
				pmp,
				pcf,
				pmda,
				pexpr,
				pexprRelational,
				phmexprcr,
				pdrgpcrArgDQA,
				true /*fSpillTo2Level*/
				);
		pxfres->Add(pexprAlt3);
	}
        
	pdrgpcrArgDQA->Release();

	// clean up
	if (0 < pdrgpexprChildPrEl->UlLength())
	{
		pexprRelational->Release();
	}
	else
	{
		pdrgpexprChildPrEl->Release();
	}

	phmexprcr->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::PexprSplitIntoLocalDQAGlobalAgg
//
//	@doc:
// 		Split the DQA into a local DQA and global agg function
//
//---------------------------------------------------------------------------
CExpression *
CXformSplitDQA::PexprSplitIntoLocalDQAGlobalAgg
	(
	IMemoryPool *pmp,
	CColumnFactory *pcf,
	CMDAccessor *pmda,
	CExpression *pexpr,
	CExpression *pexprRelational,
	HMExprCr *phmexprcr,
	DrgPcr *pdrgpcrArgDQA
	)
{
	CExpression *pexprPrL = (*pexpr)[1];

	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());
	DrgPcr *pdrgpcrGlobal = popAgg->Pdrgpcr();

	// array of project elements for the local, intermediate and global aggregate operator
	DrgPexpr *pdrgpexprPrElFirstStage = GPOS_NEW(pmp) DrgPexpr(pmp);
	DrgPexpr *pdrgpexprPrElLastStage = GPOS_NEW(pmp) DrgPexpr(pmp);

	const ULONG ulArity = pexprPrL->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprPrEl = (*pexprPrL)[ul];
		CScalarProjectElement *popScPrEl = CScalarProjectElement::PopConvert(pexprPrEl->Pop());

		// get the scalar aggregate function
		CExpression *pexprAggFunc = (*pexprPrEl)[0];
		CScalarAggFunc *popScAggFunc = CScalarAggFunc::PopConvert(pexprAggFunc->Pop());

		if (popScAggFunc->FDistinct())
		{
			// create a new local DQA version of the original global DQA
			popScAggFunc->Pmdid()->AddRef();
			CScalarAggFunc *popScAggFuncLocal = CUtils::PopAggFunc
												(
												pmp,
												popScAggFunc->Pmdid(),
												GPOS_NEW(pmp) CWStringConst(pmp, popScAggFunc->PstrAggFunc()->Wsz()),
												true /* fDistinct */,
												EaggfuncstageLocal /*eaggfuncstage*/,
												true /* fSplit */
												);

			GPOS_ASSERT(1 == pexprAggFunc->UlArity());
			CExpression *pexprArg = (*pexprAggFunc)[0];
			CColRef *pcrDistinctCol = phmexprcr->PtLookup(pexprArg);
			GPOS_ASSERT(NULL != pcrDistinctCol);
			DrgPexpr *pdrgpexprArgsLocal = GPOS_NEW(pmp) DrgPexpr(pmp);
			pdrgpexprArgsLocal->Append(CUtils::PexprScalarIdent(pmp, pcrDistinctCol));

			const IMDAggregate *pmdagg = pmda->Pmdagg(popScAggFunc->Pmdid());
			const IMDType *pmdtype = pmda->Pmdtype(pmdagg->PmdidTypeIntermediate());
			CColRef *pcrLocal = pcf->PcrCreate(pmdtype, IDefaultTypeModifier, OidInvalidCollation);

			CExpression *pexprPrElLocal = CUtils::PexprScalarProjectElement
													(
													pmp,
													pcrLocal,
													GPOS_NEW(pmp) CExpression(pmp, popScAggFuncLocal, pdrgpexprArgsLocal)
													);

			pdrgpexprPrElFirstStage->Append(pexprPrElLocal);

			// create a new "non-distinct" global aggregate version of the original DQA
			popScAggFunc->Pmdid()->AddRef();
			CScalarAggFunc *popScAggFuncGlobal = CUtils::PopAggFunc
													(
													pmp,
													popScAggFunc->Pmdid(),
													GPOS_NEW(pmp) CWStringConst(pmp, popScAggFunc->PstrAggFunc()->Wsz()),
													false /* fDistinct */,
													EaggfuncstageGlobal /*eaggfuncstage*/,
													true /* fSplit */
													);

			DrgPexpr *pdrgpexprArgsGlobal = GPOS_NEW(pmp) DrgPexpr(pmp);
			pdrgpexprArgsGlobal->Append(CUtils::PexprScalarIdent(pmp, pcrLocal));

			CExpression *pexprPrElGlobal = CUtils::PexprScalarProjectElement
													(
													pmp,
													popScPrEl->Pcr(),
													GPOS_NEW(pmp) CExpression(pmp, popScAggFuncGlobal, pdrgpexprArgsGlobal)
													);

			pdrgpexprPrElLastStage->Append(pexprPrElGlobal);
		}
		else
		{
			// split regular aggregate function into multi-level aggregate functions
			PopulatePrLMultiPhaseAgg
				(
				pmp,
				pcf,
				pmda,
				pexprPrEl,
				pdrgpexprPrElFirstStage,
				NULL, /* pdrgpexprPrElSecondStage*/
				pdrgpexprPrElLastStage,
				true /* fSplit2LevelsOnly */
				);
		}
	}

	CExpression *pexprGlobal = PexprMultiLevelAggregation
								(
								pmp,
								pexprRelational,
								pdrgpexprPrElFirstStage,
								NULL, /* pdrgpexprPrElSecondStage */
								pdrgpexprPrElLastStage,
								pdrgpcrArgDQA,
								pdrgpcrGlobal,
								true /* fSplit2LevelsOnly */,
								false /* fAddDistinctColToLocalGb */
								);

	return pexprGlobal;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::PexprSplitHelper
//
//	@doc:
//		Helper function to Split DQA into 2-stage or 3-stage aggregation.
//
//		If fSpillTo2Level is FALSE, split the 'group by' operator into 3-stage aggregation,
//		e.g., first, second and last aggregates.
//
//		If fSpillTo2Level is TRUE, split the 'group by' operator into 2-stage aggregation,
//		e.g., first, last aggregates. (second aggregate function becomes empty.)
//
//		In both scenarios, add the new aggregate functions to the project list of the
//		corresponding group by operator at each stage of the multi-stage aggregation.
//
//---------------------------------------------------------------------------
CExpression *
CXformSplitDQA::PexprSplitHelper
	(
	IMemoryPool *pmp,
	CColumnFactory *pcf,
	CMDAccessor *pmda,
	CExpression *pexpr,
	CExpression *pexprRelational,
	HMExprCr *phmexprcr,
	DrgPcr *pdrgpcrArgDQA,
	BOOL fSpillTo2Level
	)
{
	CExpression *pexprPrL = (*pexpr)[1];

	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());
	DrgPcr *pdrgpcrGlobal = popAgg->Pdrgpcr();

	// array of project elements for the local (first), intermediate
	// (second) and global (third) aggregate operator
	DrgPexpr *pdrgpexprPrElFirstStage = GPOS_NEW(pmp) DrgPexpr(pmp);
	DrgPexpr *pdrgpexprPrElSecondStage = GPOS_NEW(pmp) DrgPexpr(pmp);
	DrgPexpr *pdrgpexprPrElLastStage = GPOS_NEW(pmp) DrgPexpr(pmp);

	const ULONG ulArity = pexprPrL->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprPrEl = (*pexprPrL)[ul];
		CScalarProjectElement *popScPrEl = CScalarProjectElement::PopConvert(pexprPrEl->Pop());

		// get the scalar aggregate function
		CExpression *pexprAggFunc = (*pexprPrEl)[0];
		CScalarAggFunc *popScAggFunc = CScalarAggFunc::PopConvert(pexprAggFunc->Pop());

		if (popScAggFunc->FDistinct())
		{
			// create a new "non-distinct" version of the original aggregate function
			popScAggFunc->Pmdid()->AddRef();
			CScalarAggFunc *popScAggFuncNew = CUtils::PopAggFunc
												(
												pmp,
												popScAggFunc->Pmdid(),
												GPOS_NEW(pmp) CWStringConst(pmp, popScAggFunc->PstrAggFunc()->Wsz()),
												false /* fDistinct */,
												EaggfuncstageGlobal /*eaggfuncstage*/,
												false /* fSplit */
												);

			GPOS_ASSERT(1 == pexprAggFunc->UlArity());
			CExpression *pexprArg = (*pexprAggFunc)[0];

			CColRef *pcrDistinctCol = phmexprcr->PtLookup(pexprArg);
			GPOS_ASSERT(NULL != pcrDistinctCol);
			DrgPexpr *pdrgpexprArgs = GPOS_NEW(pmp) DrgPexpr(pmp);
			pdrgpexprArgs->Append(CUtils::PexprScalarIdent(pmp, pcrDistinctCol));

			CExpression *pexprPrElGlobal = CUtils::PexprScalarProjectElement
													(
													pmp,
													popScPrEl->Pcr(),
													GPOS_NEW(pmp) CExpression(pmp, popScAggFuncNew, pdrgpexprArgs)
													);

			pdrgpexprPrElLastStage->Append(pexprPrElGlobal);
		}
		else
		{
			// split the regular aggregate function into multi-level aggregate functions
			PopulatePrLMultiPhaseAgg
				(
				pmp,
				pcf,
				pmda,
				pexprPrEl,
				pdrgpexprPrElFirstStage,
				pdrgpexprPrElSecondStage,
				pdrgpexprPrElLastStage,
				fSpillTo2Level
				);
		}
	}

	CExpression *pexprGlobal = PexprMultiLevelAggregation
								(
								pmp,
								pexprRelational,
								pdrgpexprPrElFirstStage,
								pdrgpexprPrElSecondStage,
								pdrgpexprPrElLastStage,
								pdrgpcrArgDQA,
								pdrgpcrGlobal,
								fSpillTo2Level,
								true /* fAddDistinctColToLocalGb */
								);

	// clean-up the secondStage if spill to 2 level
	if (fSpillTo2Level)
	{
		pdrgpexprPrElSecondStage->Release();
	}

	return pexprGlobal;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::PexprPrElAgg
//
//	@doc:
//		Create an aggregate function  of a particular level and a project
//		project element to hold it
//
//---------------------------------------------------------------------------
CExpression *
CXformSplitDQA::PexprPrElAgg
	(
	IMemoryPool *pmp,
	CExpression *pexprAggFunc,
	EAggfuncStage eaggfuncstage,
	CColRef *pcrPreviousStage,
	CColRef *pcrCurrStage
	)
{
	GPOS_ASSERT(NULL != pexprAggFunc);
	GPOS_ASSERT(NULL != pcrCurrStage);
	GPOS_ASSERT(EaggfuncstageSentinel != eaggfuncstage);

	CScalarAggFunc *popScAggFunc = CScalarAggFunc::PopConvert(pexprAggFunc->Pop());
	GPOS_ASSERT(!popScAggFunc->FDistinct());

	// project element of global aggregation
	DrgPexpr *pdrgpexprArg = NULL;
	if (EaggfuncstageLocal == eaggfuncstage)
	{
		DrgPexpr *pdrgpexprAggOrig = pexprAggFunc->PdrgPexpr();
		pdrgpexprAggOrig->AddRef();
		pdrgpexprArg = pdrgpexprAggOrig;
	}
	else
	{
		pdrgpexprArg = GPOS_NEW(pmp) DrgPexpr(pmp);
		pdrgpexprArg->Append(CUtils::PexprScalarIdent(pmp, pcrPreviousStage));
	}

	popScAggFunc->Pmdid()->AddRef();
	CScalarAggFunc *popScAggFuncNew = CUtils::PopAggFunc
												(
												pmp,
												popScAggFunc->Pmdid(),
												GPOS_NEW(pmp) CWStringConst(pmp, popScAggFunc->PstrAggFunc()->Wsz()),
												false, /*fdistinct */
												eaggfuncstage,
												true /* fSplit */
												);

	return CUtils::PexprScalarProjectElement
					(
					pmp,
					pcrCurrStage,
					GPOS_NEW(pmp) CExpression(pmp, popScAggFuncNew, pdrgpexprArg)
					);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::PopulatePrLMultiPhaseAgg
//
//	@doc:
// 		Given a scalar aggregate generate the local, intermediate and global
// 		aggregate function. Then add it to the project list of the corresponding
// 		aggregate operator at each stage of the multi-stage aggregation
//
//---------------------------------------------------------------------------
void
CXformSplitDQA::PopulatePrLMultiPhaseAgg
	(
	IMemoryPool *pmp,
	CColumnFactory *pcf,
	CMDAccessor *pmda,
	CExpression *pexprPrEl,
	DrgPexpr *pdrgpexprPrElFirstStage,
	DrgPexpr *pdrgpexprPrElSecondStage,
	DrgPexpr *pdrgpexprPrElLastStage,
	BOOL fSplit2LevelsOnly
	)
{
	GPOS_ASSERT(NULL != pexprPrEl);
	GPOS_ASSERT(NULL != pdrgpexprPrElFirstStage);
	GPOS_ASSERT_IMP(NULL == pdrgpexprPrElSecondStage, fSplit2LevelsOnly);
	GPOS_ASSERT(NULL != pdrgpexprPrElLastStage);

	// get the components of the project element (agg func)
	CScalarProjectElement *popScPrEl = CScalarProjectElement::PopConvert(pexprPrEl->Pop());
	CExpression *pexprAggFunc = (*pexprPrEl)[0];
	CScalarAggFunc *popScAggFunc = CScalarAggFunc::PopConvert(pexprAggFunc->Pop());

	const IMDAggregate *pmdagg = pmda->Pmdagg(popScAggFunc->Pmdid());
	const IMDType *pmdtype = pmda->Pmdtype(pmdagg->PmdidTypeIntermediate());

	// create new column reference for the first stage (local) project element
	CColRef *pcrLocal = pcf->PcrCreate(pmdtype, IDefaultTypeModifier, OidInvalidCollation);

	CExpression *pexprPrElFirstStage = PexprPrElAgg(pmp, pexprAggFunc, EaggfuncstageLocal, NULL /*pcrPreviousStage*/, pcrLocal);
	pdrgpexprPrElFirstStage->Append(pexprPrElFirstStage);

	// column reference for the second stage project elements
	CColRef *pcrSecondStage = NULL;
	EAggfuncStage eaggfuncstage = EaggfuncstageIntermediate;
	if (fSplit2LevelsOnly)
	{
		eaggfuncstage = EaggfuncstageGlobal;
		pcrSecondStage = popScPrEl->Pcr();
	}
	else
	{
		// create a new column reference for the second stage (intermediate) project element
		pcrSecondStage = pcf->PcrCreate(pmdtype, IDefaultTypeModifier, OidInvalidCollation);
	}

	CExpression *pexprPrElSecondStage = PexprPrElAgg(pmp, pexprAggFunc, eaggfuncstage, pcrLocal, pcrSecondStage);
	if (fSplit2LevelsOnly)
	{
		pdrgpexprPrElLastStage->Append(pexprPrElSecondStage);
		return;
	}

	pdrgpexprPrElSecondStage->Append(pexprPrElSecondStage);

	// column reference for the third stage project elements
	CColRef *pcrGlobal = popScPrEl->Pcr();
	CExpression *pexprPrElGlobal = PexprPrElAgg(pmp, pexprAggFunc, EaggfuncstageGlobal, pcrSecondStage, pcrGlobal);

	pdrgpexprPrElLastStage->Append(pexprPrElGlobal);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::PcrAggFuncArgument
//
//	@doc:
//		Return the column reference of the argument to the aggregate function.
//		If the argument is a computed column then create a new project element
//		in the child's project list, else just return its column reference
//
//---------------------------------------------------------------------------
CColRef *
CXformSplitDQA::PcrAggFuncArgument
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	CColumnFactory *pcf,
	CExpression *pexprArg,
	DrgPexpr *pdrgpexprChildPrEl
	)
{
	GPOS_ASSERT(NULL != pexprArg);
	GPOS_ASSERT(NULL != pdrgpexprChildPrEl);

	if (COperator::EopScalarIdent == pexprArg->Pop()->Eopid())
	{
		return (const_cast<CColRef *>(CScalarIdent::PopConvert(pexprArg->Pop())->Pcr()));
	}

	CScalar *popScalar = CScalar::PopConvert(pexprArg->Pop());
	// computed argument to the input
	const IMDType *pmdtype = pmda->Pmdtype(popScalar->PmdidType());
	CColRef *pcrAdditionalGrpCol = pcf->PcrCreate(pmdtype, popScalar->ITypeModifier(), OidInvalidCollation);

	pexprArg->AddRef();
	CExpression *pexprPrElNew = CUtils::PexprScalarProjectElement(pmp, pcrAdditionalGrpCol, pexprArg);

	pdrgpexprChildPrEl->Append(pexprPrElNew);

	return pcrAdditionalGrpCol;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::PexprMultiLevelAggregation
//
//	@doc:
//		Generate an expression with multi-level aggregation
//
//---------------------------------------------------------------------------
CExpression *
CXformSplitDQA::PexprMultiLevelAggregation
	(
	IMemoryPool *pmp,
	CExpression *pexprRelational,
	DrgPexpr *pdrgpexprPrElFirstStage,
	DrgPexpr *pdrgpexprPrElSecondStage,
	DrgPexpr *pdrgpexprPrElThirdStage,
	DrgPcr *pdrgpcrArgDQA,
	DrgPcr *pdrgpcrLastStage,
	BOOL fSplit2LevelsOnly,
	BOOL fAddDistinctColToLocalGb
	)
{
	GPOS_ASSERT(NULL != pexprRelational);
	GPOS_ASSERT(NULL != pdrgpexprPrElFirstStage);
	GPOS_ASSERT(NULL != pdrgpexprPrElThirdStage);
	GPOS_ASSERT(NULL != pdrgpcrArgDQA);

	GPOS_ASSERT_IMP(!fAddDistinctColToLocalGb, fSplit2LevelsOnly);

	DrgPcr *pdrgpcrLocal = CUtils::PdrgpcrExactCopy(pmp, pdrgpcrLastStage);
	const ULONG ulLen = pdrgpcrArgDQA->UlLength();
	GPOS_ASSERT(0 < ulLen);

	if (fAddDistinctColToLocalGb)
	{
		// add the distinct column to the group by at the first stage of
		// the multi-level aggregation
		CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp, pdrgpcrLocal);
		for (ULONG ul = 0; ul < ulLen; ul++)
		{
			CColRef *pcr = (*pdrgpcrArgDQA)[ul];
			if (!pcrs->FMember(pcr))
			{
				pdrgpcrLocal->Append(pcr);
				pcrs->Include(pcr);
			}
		}
		pcrs->Release();
	}

	CLogicalGbAgg *popFirstStage = NULL;
	CLogicalGbAgg *popSecondStage = NULL;
	DrgPexpr *pdrgpexprLastStage = pdrgpexprPrElSecondStage;
	if (fSplit2LevelsOnly)
	{
		// for scalar DQA the local aggregate is responsible for removing duplicates
		BOOL fLocalAggGeneratesDuplicates = (0 < pdrgpcrLastStage->UlLength());

		pdrgpcrArgDQA->AddRef();
		popFirstStage = GPOS_NEW(pmp) CLogicalGbAgg
									(
									pmp,
									pdrgpcrLocal,
									COperator::EgbaggtypeLocal,
									fLocalAggGeneratesDuplicates,
									pdrgpcrArgDQA
									);
		pdrgpcrLastStage->AddRef();
		popSecondStage = GPOS_NEW(pmp) CLogicalGbAgg(pmp, pdrgpcrLastStage, COperator::EgbaggtypeGlobal /* egbaggtype */);
		pdrgpexprLastStage = pdrgpexprPrElThirdStage;
	}
	else
	{
		popFirstStage = GPOS_NEW(pmp) CLogicalGbAgg(pmp, pdrgpcrLocal, COperator::EgbaggtypeLocal /* egbaggtype */);
		pdrgpcrLocal->AddRef();
		pdrgpcrArgDQA->AddRef();
		popSecondStage = GPOS_NEW(pmp) CLogicalGbAgg
									(
									pmp,
									pdrgpcrLocal,
									COperator::EgbaggtypeIntermediate,
									false, /* fGeneratesDuplicates */
									pdrgpcrArgDQA
									);
	}

	pexprRelational->AddRef();
	CExpression *pexprFirstStage = GPOS_NEW(pmp) CExpression
											(
											pmp,
											popFirstStage,
											pexprRelational,
											GPOS_NEW(pmp) CExpression
														(
														pmp,
														GPOS_NEW(pmp) CScalarProjectList(pmp),
														pdrgpexprPrElFirstStage
														)
											);

	CExpression *pexprSecondStage = GPOS_NEW(pmp) CExpression
												(
												pmp,
												popSecondStage,
												pexprFirstStage,
												GPOS_NEW(pmp) CExpression
															(
															pmp,
															GPOS_NEW(pmp) CScalarProjectList(pmp),
															pdrgpexprLastStage
															)
												);

	if (fSplit2LevelsOnly)
	{
		return pexprSecondStage;
	}

	pdrgpcrLastStage->AddRef();
	return GPOS_NEW(pmp) CExpression
						(
						pmp,
						GPOS_NEW(pmp) CLogicalGbAgg(pmp, pdrgpcrLastStage, COperator::EgbaggtypeGlobal /* egbaggtype */),
						pexprSecondStage,
						GPOS_NEW(pmp) CExpression
									(
									pmp,
									GPOS_NEW(pmp) CScalarProjectList(pmp),
									pdrgpexprPrElThirdStage
									)
						);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::ExtractDistinctCols
//
//	@doc:
//		Extract arguments of distinct aggs
//
//---------------------------------------------------------------------------
void
CXformSplitDQA::ExtractDistinctCols
	(
	IMemoryPool *pmp,
	CColumnFactory *pcf,
	CMDAccessor *pmda,
	CExpression *pexpr,
	DrgPexpr *pdrgpexprChildPrEl,
	HMExprCr *phmexprcr,
	DrgPcr **ppdrgpcrArgDQA  // output: array of distinct aggs arguments
	)
{
	GPOS_ASSERT(NULL != pdrgpexprChildPrEl);
	GPOS_ASSERT(NULL != ppdrgpcrArgDQA);
	GPOS_ASSERT(NULL != phmexprcr);

	const ULONG ulArity = pexpr->UlArity();

	// use a set to deduplicate distinct aggs arguments
	CColRefSet *pcrsArgDQA = GPOS_NEW(pmp) CColRefSet(pmp);
	ULONG ulDistinct = 0;
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprPrEl = (*pexpr)[ul];

		// get the scalar child of the project element
		CExpression *pexprAggFunc = (*pexprPrEl)[0];
		CScalarAggFunc *popScAggFunc = CScalarAggFunc::PopConvert(pexprAggFunc->Pop());

		if (popScAggFunc->FDistinct() && pmda->Pmdagg(popScAggFunc->Pmdid())->FSplittable())
		{
			GPOS_ASSERT(1 == pexprAggFunc->UlArity());
			
			CExpression *pexprArg = (*pexprAggFunc)[0];
			GPOS_ASSERT(NULL != pexprArg);
			CColRef *pcrDistinctCol = phmexprcr->PtLookup(pexprArg);
			if (NULL == pcrDistinctCol)
			{
				ulDistinct++;

				// get the column reference of the DQA argument
				pcrDistinctCol = PcrAggFuncArgument(pmp, pmda, pcf, pexprArg, pdrgpexprChildPrEl);

				// insert into the map between the expression representing the DQA argument 
				// and its column reference
				pexprArg->AddRef();
#ifdef GPOS_DEBUG
				BOOL fInserted =
#endif
						phmexprcr->FInsert(pexprArg, pcrDistinctCol);
				GPOS_ASSERT(fInserted);

				// add the distinct column to the set of distinct columns
				pcrsArgDQA->Include(pcrDistinctCol);
			}
		}
	}

	if (1 == ulDistinct)
	{
		*ppdrgpcrArgDQA = pcrsArgDQA->Pdrgpcr(pmp);
	}
	else
	{
		// failed to find a single DQA, or agg is defined as non-splittable
		*ppdrgpcrArgDQA = NULL;
	}
	pcrsArgDQA->Release();
}

// EOF
