//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSplitGbAgg.cpp
//
//	@doc:
//		Implementation of the splitting of an aggregate into a pair of
//		local and global aggregate
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CColRefComputed.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/xforms/CXformSplitGbAgg.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "naucrates/md/IMDAggregate.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSplitGbAgg::CXformSplitGbAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSplitGbAgg::CXformSplitGbAgg
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
//		CXformSplitGbAgg::CXformSplitGbAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSplitGbAgg::CXformSplitGbAgg
	(
	CExpression *pexprPattern
	)
	:
	CXformExploration(pexprPattern)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitGbAgg::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSplitGbAgg::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	// do not split aggregate if it is a local aggregate, has distinct aggs, has outer references,
	// or return types of Agg functions are ambiguous
	if (!CLogicalGbAgg::PopConvert(exprhdl.Pop())->FGlobal() ||
		0 < exprhdl.Pdpscalar(1 /*ulChildIndex*/)->UlDistinctAggs() ||
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
//		CXformSplitGbAgg::Transform
//
//	@doc:
//		Actual transformation to expand a global aggregate into a pair of
//		local and global aggregate
//
//---------------------------------------------------------------------------
void
CXformSplitGbAgg::Transform
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
	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());

	// extract components
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprProjectList = (*pexpr)[1];

	// check if the transformation is applicable
	if (!FApplicable(pexprProjectList))
	{
		return;
	}

	pexprRelational->AddRef();

	CExpression *pexprProjectListLocal = NULL;
	CExpression *pexprProjectListGlobal = NULL;

	(void) PopulateLocalGlobalProjectList
			(
			pmp,
			pexprProjectList,
			&pexprProjectListLocal,
			&pexprProjectListGlobal
			);

	GPOS_ASSERT(NULL != pexprProjectListLocal && NULL != pexprProjectListLocal);

	DrgPcr *pdrgpcr = popAgg->Pdrgpcr();

	pdrgpcr->AddRef();
	DrgPcr *pdrgpcrLocal = pdrgpcr;

	pdrgpcr->AddRef();
	DrgPcr *pdrgpcrGlobal = pdrgpcr;

	DrgPcr *pdrgpcrMinimal = popAgg->PdrgpcrMinimal();
	if (NULL != pdrgpcrMinimal)
	{
		// addref minimal grouping columns twice to be used in local and global aggregate
		pdrgpcrMinimal->AddRef();
		pdrgpcrMinimal->AddRef();
	}

	CExpression *pexprLocal = GPOS_NEW(pmp) CExpression
											(
											pmp,
											GPOS_NEW(pmp) CLogicalGbAgg
														(
														pmp,
														pdrgpcrLocal,
														pdrgpcrMinimal,
														COperator::EgbaggtypeLocal /*egbaggtype*/
														),
											pexprRelational,
											pexprProjectListLocal
											);

	CExpression *pexprGlobal = GPOS_NEW(pmp) CExpression
											(
											pmp,
											GPOS_NEW(pmp) CLogicalGbAgg
														(
														pmp,
														pdrgpcrGlobal,
														pdrgpcrMinimal,
														COperator::EgbaggtypeGlobal /*egbaggtype*/
														),
											pexprLocal,
											pexprProjectListGlobal
											);

	pxfres->Add(pexprGlobal);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitGbAgg::PopulateLocalGlobalProjectList
//
//	@doc:
//		Populate the local or global project list from the input project list
//
//---------------------------------------------------------------------------
void
CXformSplitGbAgg::PopulateLocalGlobalProjectList
	(
	IMemoryPool *pmp,
	CExpression *pexprProjList,
	CExpression **ppexprProjListLocal,
	CExpression **ppexprProjListGlobal
	)
{
	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	// list of project elements for the new local and global aggregates
	DrgPexpr *pdrgpexprProjElemLocal = GPOS_NEW(pmp) DrgPexpr(pmp);
	DrgPexpr *pdrgpexprProjElemGlobal = GPOS_NEW(pmp) DrgPexpr(pmp);
	const ULONG ulArity = pexprProjList->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprProgElem = (*pexprProjList)[ul];
		CScalarProjectElement *popScPrEl =
				CScalarProjectElement::PopConvert(pexprProgElem->Pop());

		// get the scalar agg func
		CExpression *pexprAggFunc = (*pexprProgElem)[0];
		CScalarAggFunc *popScAggFunc =
				CScalarAggFunc::PopConvert(pexprAggFunc->Pop());

		popScAggFunc->Pmdid()->AddRef();
		CScalarAggFunc *popScAggFuncLocal = CUtils::PopAggFunc
														(
														pmp,
														popScAggFunc->Pmdid(),
														GPOS_NEW(pmp) CWStringConst(pmp, popScAggFunc->PstrAggFunc()->Wsz()),
														popScAggFunc->FDistinct(),
														EaggfuncstageLocal, /* fGlobal */
														true /* fSplit */
														);

		popScAggFunc->Pmdid()->AddRef();
		CScalarAggFunc *popScAggFuncGlobal = CUtils::PopAggFunc
														(
														pmp,
														popScAggFunc->Pmdid(),
														GPOS_NEW(pmp) CWStringConst(pmp, popScAggFunc->PstrAggFunc()->Wsz()),
														false /* fDistinct */,
														EaggfuncstageGlobal, /* fGlobal */
														true /* fSplit */
														);

		// determine column reference for the new project element
		const IMDAggregate *pmdagg = pmda->Pmdagg(popScAggFunc->Pmdid());
		const IMDType *pmdtype = pmda->Pmdtype(pmdagg->PmdidTypeIntermediate());
		CColRef *pcrLocal = pcf->PcrCreate(pmdtype, IDefaultTypeModifier, OidInvalidCollation);
		CColRef *pcrGlobal = popScPrEl->Pcr();

		// create a new local aggregate function
		// create array of arguments for the aggregate function
		DrgPexpr *pdrgpexprAgg = pexprAggFunc->PdrgPexpr();

		pdrgpexprAgg->AddRef();
		DrgPexpr *pdrgpexprLocal = pdrgpexprAgg;

		CExpression *pexprAggFuncLocal = GPOS_NEW(pmp) CExpression
													(
													pmp,
													popScAggFuncLocal,
													pdrgpexprLocal
													);

		// create a new global aggregate function adding the column reference of the
		// intermediate result to the arguments of the global aggregate function
		DrgPexpr *pdrgpexprGlobal = GPOS_NEW(pmp) DrgPexpr(pmp);
		CExpression *pexprArg = CUtils::PexprScalarIdent(pmp, pcrLocal);
		pdrgpexprGlobal->Append(pexprArg);

		CExpression *pexprAggFuncGlobal = GPOS_NEW(pmp) CExpression
						(
						pmp,
						popScAggFuncGlobal,
						pdrgpexprGlobal
						);

		// create new project elements for the aggregate functions
		CExpression *pexprProjElemLocal = CUtils::PexprScalarProjectElement
													(
													pmp,
													pcrLocal,
													pexprAggFuncLocal
													);

		CExpression *pexprProjElemGlobal = CUtils::PexprScalarProjectElement
													(
													pmp,
													pcrGlobal,
													pexprAggFuncGlobal
													);

		pdrgpexprProjElemLocal->Append(pexprProjElemLocal);
		pdrgpexprProjElemGlobal->Append(pexprProjElemGlobal);
	}

	// create new project lists
	*ppexprProjListLocal = GPOS_NEW(pmp) CExpression
									(
									pmp,
									GPOS_NEW(pmp) CScalarProjectList(pmp),
									pdrgpexprProjElemLocal
									);

	*ppexprProjListGlobal = GPOS_NEW(pmp) CExpression
									(
									pmp,
									GPOS_NEW(pmp) CScalarProjectList(pmp),
									pdrgpexprProjElemGlobal
									);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitGbAgg::FApplicable
//
//	@doc:
//		Check if we the transformation is applicable (no distinct qualified
//		aggregate (DQA)) present
//
//---------------------------------------------------------------------------
BOOL
CXformSplitGbAgg::FApplicable
	(
	CExpression *pexpr
	)
{
	const ULONG ulArity = pexpr->UlArity();
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprPrEl = (*pexpr)[ul];

		// get the scalar child of the project element
		CExpression *pexprAggFunc = (*pexprPrEl)[0];
		CScalarAggFunc *popScAggFunc = CScalarAggFunc::PopConvert(pexprAggFunc->Pop());

		if (popScAggFunc->FDistinct() || !pmda->Pmdagg(popScAggFunc->Pmdid())->FSplittable())
		{
			return false;
		}
	}

	return true;
}

// EOF
