//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSubqueryHandler.cpp
//
//	@doc:
//		Implementation of transforming subquery expressions to Apply
//		expressions;
//		Given a SELECT/PROJECT/Gb-Agg/SEQUENCE-PROJECT expression whose scalar
//		child involves subqueries, this class is used for unnesting subqueries by
//		creating a new expression whose logical child is an Apply tree and whose
//		scalar child is subquery-free
//
//		The default behavior is to generate regular apply expressions which are then
//		turned into regular (inner/outer/semi/anti-semi) joins,
//		only when a regular apply expression cannot be generated, a correlated apply
//		expression is generated instead, which results in a correlated join (subplan in GPDB)
//
//		The handler can also be used to always enforce generating correlated apply
//		expressions, this behavior can be enforced by setting the flag
//		fEnforceCorrelatedApply to true
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/exception.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CSubqueryHandler.h"
#include "gpopt/xforms/CXformUtils.h"

#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDTypeInt8.h"

using namespace gpopt;

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::AssertValidArguments
//
//	@doc:
//		Validity checks of arguments
//
//---------------------------------------------------------------------------
void
CSubqueryHandler::AssertValidArguments
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter,
	CExpression *pexprScalar,
	CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexprOuter);
	GPOS_ASSERT(pexprOuter->Pop()->FLogical());
	GPOS_ASSERT(NULL != pexprScalar);
	GPOS_ASSERT(pexprScalar->Pop()->FScalar());
	GPOS_ASSERT(NULL != ppexprNewOuter);
	GPOS_ASSERT(NULL != ppexprResidualScalar);
}
#endif // GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::PexprReplace
//
//	@doc:
//		Given an input expression, replace all occurrences of given column
//		with the given scalar expression
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryHandler::PexprReplace
	(
	IMemoryPool *pmp,
	CExpression *pexprInput,
	CColRef *pcr,
	CExpression *pexprScalar
	)
{
	GPOS_ASSERT(NULL != pexprInput);
	GPOS_ASSERT(NULL != pcr);

	COperator *pop = pexprInput->Pop();
	if (pop->Eopid() == CScalar::EopScalarIdent &&
		CScalarIdent::PopConvert(pop)->Pcr() == pcr)
	{
		GPOS_ASSERT(NULL != pexprScalar);
		GPOS_ASSERT(pexprScalar->Pop()->FScalar());

		pexprScalar->AddRef();
		return pexprScalar;
	}

	// process children
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);

	const ULONG ulArity = pexprInput->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = PexprReplace(pmp, (*pexprInput)[ul], pcr, pexprScalar);
		pdrgpexpr->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::PexprSubqueryPred
//
//	@doc:
//		Build a predicate expression for the quantified comparison of the
//		subquery.
//
//		This method first attempts to un-nest any subquery that may be present in
//		the Scalar part of the quantified subquery. Then, it proceeds to create a
//		scalar predicate comparison between the new Scalar and Logical children of
//		the Subquery. It returns the new Logical child via ppexprResult.
//
//		For example, for the query :
//		select * from foo where
//		    (select a from foo limit 1) in (select b from bar);
//
//		+--CLogicalSelect
//		   |--CLogicalGet "foo"
//		   +--CScalarSubqueryAny(=)["b" (10)]
//		      |--CLogicalGet "bar"
//		      +--CScalarSubquery["a" (18)]
//		         +--CLogicalLimit <empty> global
//		            |--CLogicalGet "foo"
//		            |--CScalarConst (0)
//		            +--CScalarCast
//		               +--CScalarConst (1)
//
//		will return ..
//		+--CScalarCmp (=)
//		   |--CScalarIdent "a" (18)
//		   +--CScalarIdent "b" (10)
//
//		with pexprResult as ..
//		+--CLogicalInnerApply
//		   |--CLogicalGet "bar"
//		   |--CLogicalMaxOneRow
//		   |  +--CLogicalLimit
//		   |     |--CLogicalGet "foo"
//		   |     |--CScalarConst (0)   origin: [Grp:3, GrpExpr:0]
//		   |     +--CScalarCast   origin: [Grp:5, GrpExpr:0]
//		   |        +--CScalarConst (1)   origin: [Grp:4, GrpExpr:0]
//		   +--CScalarConst (1)
//
//		If there is no such subquery, it returns a comparison expression using
//		the original Scalar expression and sets ppexprResult to the passed in
//		pexprOuter.
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryHandler::PexprSubqueryPred
	(
	CExpression *pexprOuter,
	CExpression *pexprSubquery,
	CExpression **ppexprResult
	)
{
	GPOS_ASSERT(CUtils::FQuantifiedSubquery(pexprSubquery->Pop()));

	CExpression *pexprNewScalar = NULL;
	CExpression *pexprNewLogical = NULL;

	CExpression *pexprScalarChild = (*pexprSubquery)[1];
	CSubqueryHandler::ESubqueryCtxt esqctxt = CSubqueryHandler::EsqctxtFilter;

	// If pexprScalarChild is a non-scalar subquery such as follows,
	// EXPLAIN SELECT * FROM t3 WHERE (c = ANY(SELECT c FROM t2)) IN (SELECT b from t1);
	// EXPLAIN SELECT * FROM t3 WHERE (NOT EXISTS(SELECT c FROM t2)) IN (SELECT b from t1);
	// then it must be treated in "Value" context such that the
	// corresponding subquery unnesting routines correctly return a column
	// identifier for the subquery outcome, which can then be referenced
	// correctly inside the quantified comparison expression.

	// This is not needed if pexprScalarChild is a scalar subquery such as
	// follows, since FRemoveScalarSubquery() always produces a column
	// identifier for the subquery projection.
	// EXPLAIN SELECT * FROM t3 WHERE (SELECT c FROM t2) IN (SELECT b from t1);
	if (CUtils::FQuantifiedSubquery(pexprScalarChild->Pop()) ||
	    CUtils::FExistentialSubquery(pexprScalarChild->Pop()) ||
	    CPredicateUtils::FAnd(pexprScalarChild))
	{
		esqctxt = EsqctxtValue;
	}

	if (!FProcess(pexprOuter, pexprScalarChild, esqctxt, &pexprNewLogical, &pexprNewScalar))
	{
		// subquery unnesting failed; attempt to create a predicate directly
		*ppexprResult = pexprOuter;
		pexprNewScalar = pexprScalarChild;
	}

	if (NULL != pexprNewLogical)
	{
		*ppexprResult = pexprNewLogical;
	}
	else
	{
		*ppexprResult = pexprOuter;
	}

	GPOS_ASSERT(NULL != pexprNewScalar);

	CScalarSubqueryQuantified *popSqQuantified = CScalarSubqueryQuantified::PopConvert(pexprSubquery->Pop());

	const CColRef *pcr = popSqQuantified->Pcr();
	IMDId *pmdidOp = popSqQuantified->PmdidOp();
	const CWStringConst *pstr = popSqQuantified->PstrOp();

	pmdidOp->AddRef();
	CExpression *pexprPredicate = CUtils::PexprScalarCmp(m_pmp, pexprNewScalar, pcr, *pstr, pmdidOp);

	return pexprPredicate;
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FProjectCountSubquery
//
//	@doc:
//		Detect subqueries with expressions over count aggregate similar
//		to (SELECT 'abc' || (SELECT count(*) from X))
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FProjectCountSubquery
	(
	CExpression *pexprSubquery,
	CColRef *pcrCount
	)
{
	GPOS_ASSERT(NULL != pexprSubquery);
	GPOS_ASSERT(COperator::EopScalarSubquery == pexprSubquery->Pop()->Eopid());
	GPOS_ASSERT(COperator::EopLogicalProject == (*pexprSubquery)[0]->Pop()->Eopid());
	GPOS_ASSERT(NULL != pcrCount);
#ifdef GPOS_DEBUG
	CColRef *pcr = NULL;
	GPOS_ASSERT(CUtils::FHasCountAgg((*pexprSubquery)[0], &pcr));
	GPOS_ASSERT(pcr == pcrCount);
#endif // GPOS_DEBUG

	CScalarSubquery *popScalarSubquery = CScalarSubquery::PopConvert(pexprSubquery->Pop());
	const CColRef *pcrSubquery = popScalarSubquery->Pcr();

	if (pcrCount == pcrSubquery)
	{
		// fail if subquery does not use compute a new expression using count column
		return false;
	}

	CExpression *pexprPrj = (*pexprSubquery)[0];
	CExpression *pexprPrjChild = (*pexprPrj)[0];
	CExpression *pexprPrjList = (*pexprPrj)[1];
	CDrvdPropScalar *pdpscalar = CDrvdPropScalar::Pdpscalar(pexprPrjList->PdpDerive());

	if (COperator::EopLogicalGbAgg != pexprPrjChild->Pop()->Eopid() ||
		pdpscalar->FHasNonScalarFunction() ||
		IMDFunction::EfsVolatile == pdpscalar->Pfp()->Efs())
	{
		// fail if Project child is not GbAgg, or there are non-scalar/volatile functions in project list
		return false;
	}

	CColRefSet *pcrsOutput = CDrvdPropRelational::Pdprel(pexprPrjChild->PdpDerive())->PcrsOutput();
	if (1 < pcrsOutput->CElements())
	{
		// fail if GbAgg has more than one output column
		return false;
	}

	CColRefSet *pcrsUsed = pdpscalar->PcrsUsed();
	BOOL fPrjUsesCount = (0 == pcrsUsed->CElements()) || (1 == pcrsUsed->CElements() && pcrsUsed->FMember(pcrCount));
	if (!fPrjUsesCount)
	{
		// fail if Project does not use count column
		return false;
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::SSubqueryDesc::SetCorrelatedExecution
//
//	@doc:
//		Set correlated execution flag
//
//---------------------------------------------------------------------------
void
CSubqueryHandler::SSubqueryDesc::SetCorrelatedExecution()
{
	// check conditions of correlated execution
	m_fCorrelatedExecution =
		m_fReturnSet || // subquery produces > 1 rows, we need correlated execution to check for cardinality at runtime
		m_fHasVolatileFunctions || // volatile functions cannot be decorrelated
		(m_fHasCountAgg && m_fHasSkipLevelCorrelations); // count() with skip-level correlations cannot be decorrelated due to their NULL semantics
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::Psd
//
//	@doc:
//		Create subquery descriptor
//
//---------------------------------------------------------------------------
CSubqueryHandler::SSubqueryDesc *
CSubqueryHandler::Psd
	(
	IMemoryPool *pmp,
	CExpression *pexprSubquery,
	CExpression *pexprOuter,
	ESubqueryCtxt esqctxt
	)
{
	GPOS_ASSERT(NULL != pexprSubquery);
	GPOS_ASSERT(CUtils::FSubquery(pexprSubquery->Pop()));
	GPOS_ASSERT(NULL != pexprOuter);

	CExpression *pexprInner = (*pexprSubquery)[0];
	CColRefSet *pcrsOuter = CDrvdPropRelational::Pdprel((*pexprSubquery)[0]->PdpDerive())->PcrsOuter();
	CColRefSet *pcrsOuterOutput = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput();

	SSubqueryDesc *psd = GPOS_NEW(pmp) SSubqueryDesc();
	psd->m_fReturnSet = (1 < CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->Maxcard().Ull());
	psd->m_fHasOuterRefs = pexprInner->FHasOuterRefs();
	psd->m_fHasVolatileFunctions = (IMDFunction::EfsVolatile == CDrvdPropScalar::Pdpscalar(pexprSubquery->PdpDerive())->Pfp()->Efs());
	psd->m_fHasSkipLevelCorrelations = 0 < pcrsOuter->CElements() && !pcrsOuterOutput->FSubset(pcrsOuter);

	psd->m_fHasCountAgg = CUtils::FHasCountAgg((*pexprSubquery)[0], &psd->m_pcrCountAgg);

	if (psd->m_fHasCountAgg &&
		COperator::EopLogicalProject == (*pexprSubquery)[0]->Pop()->Eopid())
	{
		psd->m_fProjectCount = FProjectCountSubquery(pexprSubquery, psd->m_pcrCountAgg);
	}

	// set flag for using subquery in a value context
	psd->m_fValueSubquery = EsqctxtValue == esqctxt || (psd->m_fHasCountAgg && psd->m_fHasOuterRefs);

	// set flag of correlated execution
	psd->SetCorrelatedExecution();

	return psd;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FRemoveScalarSubquery
//
//	@doc:
//		Replace a scalar subquery node with a column identifier, and create
//		a new Apply expression;
//
//		when subquery is defined on top of a Project node, the function simplifies
//		subquery expression by pulling-up project above subquery to facilitate
//		detecting special subquery types such as count(*) subqueries
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FRemoveScalarSubquery
	(
	CExpression *pexprOuter,
	CExpression *pexprSubquery,
	ESubqueryCtxt esqctxt,
	CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar
	)
{
	IMemoryPool *pmp = m_pmp;

#ifdef GPOS_DEBUG
	AssertValidArguments(pmp, pexprOuter, pexprSubquery, ppexprNewOuter, ppexprResidualScalar);
#endif // GPOS_DEBUG

	CScalarSubquery *popScalarSubquery = CScalarSubquery::PopConvert(pexprSubquery->Pop());
	const CColRef *pcrSubquery = popScalarSubquery->Pcr();

	SSubqueryDesc *psd = Psd(pmp, pexprSubquery, pexprOuter, esqctxt);
	BOOL fSuccess = false;
	if (psd->m_fProjectCount && !psd->m_fCorrelatedExecution)
	{
		// count(*)/count(Any) have special semantics: they produce '0' if their input is empty,
		// all other agg functions produce 'NULL' if their input is empty

		// for subqueries of the form (SELECT 'abc' || count(*) from X where x.i=outer.i),
		// we first create a LeftOuterApply expression to compute 'count' value and replace NULL
		// count values with '0' in the output of LOA expression,
		// we then pull the Project node below subquery to be above the LOA expression

		// create a new subquery to compute count(*) agg
		CExpression *pexprPrj = (*pexprSubquery)[0];
		CExpression *pexprPrjList = (*pexprPrj)[1];
		CExpression *pexprGbAgg = (*pexprPrj)[0];
		GPOS_ASSERT(COperator::EopLogicalGbAgg == pexprGbAgg->Pop()->Eopid());

		CScalarSubquery *popInnerSubq = GPOS_NEW(pmp) CScalarSubquery(pmp, psd->m_pcrCountAgg, false /*fGeneratedByExist*/, false /*fGeneratedByQuantified*/);
		pexprGbAgg->AddRef();
		CExpression *pexprNewSubq = GPOS_NEW(pmp) CExpression(pmp, popInnerSubq, pexprGbAgg);

		// unnest new subquery
		GPOS_DELETE(psd);
		CExpression *pexprNewOuter = NULL;
		CExpression *pexprResidualScalar = NULL;
		psd = Psd(pmp, pexprNewSubq, pexprOuter, esqctxt);
		fSuccess = FRemoveScalarSubqueryInternal(pmp, pexprOuter, pexprNewSubq, EsqctxtValue, psd, m_fEnforceCorrelatedApply, &pexprNewOuter, &pexprResidualScalar);

		if (fSuccess)
		{
			// unnesting succeeded -- replace all occurrences of count(*) column in project list with residual expression
			pexprPrj->Pop()->AddRef();
			CExpression *pexprPrjListNew = PexprReplace(pmp, pexprPrjList, psd->m_pcrCountAgg, pexprResidualScalar);
			*ppexprNewOuter = GPOS_NEW(pmp) CExpression(pmp, pexprPrj->Pop(), pexprNewOuter, pexprPrjListNew);
			*ppexprResidualScalar = CUtils::PexprScalarIdent(pmp, pcrSubquery);
		}

		CRefCount::SafeRelease(pexprNewSubq);
		CRefCount::SafeRelease(pexprResidualScalar);
	}
	else
	{
		fSuccess = FRemoveScalarSubqueryInternal(pmp, pexprOuter, pexprSubquery, esqctxt, psd, m_fEnforceCorrelatedApply, ppexprNewOuter, ppexprResidualScalar);
	}

	GPOS_DELETE(psd);
	return fSuccess;
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FGenerateCorrelatedApplyForScalarSubquery
//
//	@doc:
//		Helper to generate a correlated apply expression when needed
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FGenerateCorrelatedApplyForScalarSubquery
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter,
	CExpression *pexprSubquery,
	ESubqueryCtxt
#ifdef GPOS_DEBUG
		esqctxt
#endif // GPOS_DEBUG
	,
	CSubqueryHandler::SSubqueryDesc *psd,
	BOOL fEnforceCorrelatedApply,
	CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar
	)
{
#ifdef GPOS_DEBUG
	AssertValidArguments(pmp, pexprOuter, pexprSubquery, ppexprNewOuter, ppexprResidualScalar);
#endif // GPOS_DEBUG
	GPOS_ASSERT(NULL != psd);
	GPOS_ASSERT(psd->m_fCorrelatedExecution || fEnforceCorrelatedApply);

	CScalarSubquery *popScalarSubquery = CScalarSubquery::PopConvert(pexprSubquery->Pop());
	const CColRef *pcr = popScalarSubquery->Pcr();
	COperator::EOperatorId eopidSubq = popScalarSubquery->Eopid();

	CExpression *pexprInner = (*pexprSubquery)[0];
	// we always add-ref Apply's inner child since it is reused from subquery
	// inner expression
	pexprInner->AddRef();

	// in order to check cardinality of subquery output at run time, we can
	// use MaxOneRow expression, only if
	// (1) correlated execution is not enforced,
	// (2) there are no outer references below, and
	// (3) transformation converting MaxOneRow to Assert is enabled
	BOOL fUseMaxOneRow =
		!fEnforceCorrelatedApply &&
		!psd->m_fHasOuterRefs  &&
		GPOPT_FENABLED_XFORM(CXform::ExfMaxOneRow2Assert);

	if (psd->m_fValueSubquery)
	{
		if (fUseMaxOneRow)
		{
			// we need correlated execution here to check if more than one row are generated
			// by inner expression during execution, we generate a MaxOneRow expression to handle this case
			CExpression *pexprMaxOneRow = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalMaxOneRow(pmp), pexprInner);
			*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftOuterApply>(pmp, pexprOuter, pexprMaxOneRow, pcr, eopidSubq);
		}
		else
		{
			// correlated inner expression requires correlated execution
			*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftOuterCorrelatedApply>(pmp, pexprOuter, pexprInner, pcr, eopidSubq);
		}
		*ppexprResidualScalar = CUtils::PexprScalarIdent(pmp, pcr);

		return true;
	}

	GPOS_ASSERT(EsqctxtFilter == esqctxt);

	if (fUseMaxOneRow)
	{
		// we need correlated execution here to check if more than one row are generated
		// by inner expression during execution, we generate a MaxOneRow expression to handle this case
		CExpression *pexprMaxOneRow = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalMaxOneRow(pmp), pexprInner);
		*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalInnerApply>(pmp, pexprOuter, pexprMaxOneRow, pcr, eopidSubq);
	}
	else
	{
		// correlated inner expression requires correlated execution
		*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalInnerCorrelatedApply>(pmp, pexprOuter, pexprInner, pcr, eopidSubq);
	}
	*ppexprResidualScalar = CUtils::PexprScalarIdent(pmp, pcr);

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FRemoveScalarSubqueryInternal
//
//	@doc:
//		Replace a scalar subquery node with a column identifier, and create
//		a new Apply expression;
//
//		Example:
//
//			SELECT											SELECT
//			/		|									/			|
//			R		=			==>				INNER-APPLY			=
//			/		|							/		|		/	|
//			a		GrpBy						R		GrpBy	a	 x
//					/		|							/	|
//					S	x:=sum(b)						S	x:=sum(b)
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FRemoveScalarSubqueryInternal
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter,
	CExpression *pexprSubquery,
	ESubqueryCtxt esqctxt,
	CSubqueryHandler::SSubqueryDesc *psd,
	BOOL fEnforceCorrelatedApply,
	CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar
	)
{
#ifdef GPOS_DEBUG
	AssertValidArguments(pmp, pexprOuter, pexprSubquery, ppexprNewOuter, ppexprResidualScalar);
#endif // GPOS_DEBUG
	GPOS_ASSERT(NULL != psd);
	
	if (psd->m_fCorrelatedExecution || fEnforceCorrelatedApply)
	{
		return FGenerateCorrelatedApplyForScalarSubquery(pmp, pexprOuter, pexprSubquery, esqctxt, psd, fEnforceCorrelatedApply, ppexprNewOuter, ppexprResidualScalar);
	}

	CScalarSubquery *popScalarSubquery = CScalarSubquery::PopConvert(pexprSubquery->Pop());
	const CColRef *pcr = popScalarSubquery->Pcr();

	CExpression *pexprInner = (*pexprSubquery)[0];
	// we always add-ref Apply's inner child since it is reused from subquery
	// inner expression
	pexprInner->AddRef();
	BOOL fSuccess = true;
	if (psd->m_fValueSubquery)
	{
		fSuccess = FCreateOuterApply(pmp, pexprOuter, pexprInner, pexprSubquery, psd->m_fHasOuterRefs, ppexprNewOuter, ppexprResidualScalar);
		if (!fSuccess)
		{
			pexprInner->Release();
		}
		return fSuccess;
	}

	GPOS_ASSERT(EsqctxtFilter == esqctxt);

	*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalInnerApply>(pmp, pexprOuter, pexprInner, pcr, pexprSubquery->Pop()->Eopid());
	*ppexprResidualScalar = CUtils::PexprScalarIdent(pmp, pcr);

	return fSuccess;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::PexprInnerSelect
//
//	@doc:
//		Helper for creating an inner select expression when creating
//		outer apply;
//		we create a disjunctive predicate to handle null values from
//		inner expression
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryHandler::PexprInnerSelect
	(
	IMemoryPool *pmp,
	const CColRef *pcrInner,
	CExpression *pexprInner,
	CExpression *pexprPredicate
	)
{
	GPOS_ASSERT(!CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->PcrsNotNull()->FMember(pcrInner) &&
			"subquery's column is not nullable");

	CExpression *pexprIsNull = CUtils::PexprIsNull(pmp, CUtils::PexprScalarIdent(pmp, pcrInner));
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	pexprPredicate->AddRef();
	pdrgpexpr->Append(pexprPredicate);
	pdrgpexpr->Append(pexprIsNull);
	CExpression *pexprDisj = CPredicateUtils::PexprDisjunction(pmp, pdrgpexpr);
	pexprInner->AddRef();

	return CUtils::PexprLogicalSelect(pmp, pexprInner, pexprDisj);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FCreateOuterApplyForScalarSubquery
//
//	@doc:
//		Helper for creating an outer apply expression for scalar subqueries
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FCreateOuterApplyForScalarSubquery
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	CExpression *pexprSubquery,
	BOOL,  // fOuterRefsUnderInner
	CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar
	)
{
	CScalarSubquery *popSubquery = CScalarSubquery::PopConvert(pexprSubquery->Pop());
	const CColRef *pcr = popSubquery->Pcr();
	BOOL fSuccess = true;

	// generate an outer apply between outer expression and the relational child of scalar subquery
	CExpression *pexprLeftOuterApply = CUtils::PexprLogicalApply<CLogicalLeftOuterApply>(pmp, pexprOuter, pexprInner, pcr, popSubquery->Eopid());

	const CLogicalGbAgg *pgbAgg = NULL;
	BOOL fHasCountAggMatchingColumn = CUtils::FHasCountAggMatchingColumn((*pexprSubquery)[0], pcr, &pgbAgg);

	if (!fHasCountAggMatchingColumn)
	{
		// residual scalar uses the scalar subquery column
		*ppexprNewOuter = pexprLeftOuterApply;
		*ppexprResidualScalar = CUtils::PexprScalarIdent(pmp, pcr);
		return fSuccess;
	}

	// add projection for subquery column
	CExpression *pexprPrj = CUtils::PexprAddProjection(pmp, pexprLeftOuterApply, CUtils::PexprScalarIdent(pmp, pcr));
	const CColRef *pcrComputed = CScalarProjectElement::PopConvert((*(*pexprPrj)[1])[0]->Pop())->Pcr();
	*ppexprNewOuter = pexprPrj;

	BOOL fGeneratedByQuantified =  popSubquery->FGeneratedByQuantified();
	if (fGeneratedByQuantified || (fHasCountAggMatchingColumn && 0 == pgbAgg->Pdrgpcr()->UlLength()))
	{
		CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
		const IMDTypeInt8 *pmdtypeint8 = pmda->PtMDType<IMDTypeInt8>();
		IMDId *pmdidInt8 = pmdtypeint8->Pmdid();
		pmdidInt8->AddRef();
		CExpression *pexprCoalesce =
				GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CScalarCoalesce(pmp, pmdidInt8),
					CUtils::PexprScalarIdent(pmp, pcrComputed),
					CUtils::PexprScalarConstInt8(pmp, 0 /*iVal*/)
					);

		if (fGeneratedByQuantified)
		{
			// we produce Null if count(*) value is -1,
			// this case can only occur when transforming quantified subquery to
			// count(*) subquery using CXformSimplifySubquery
			pmdidInt8->AddRef();
			*ppexprResidualScalar =
				GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CScalarIf(pmp, pmdidInt8),
					CUtils::PexprScalarEqCmp(pmp, pcrComputed, CUtils::PexprScalarConstInt8(pmp, -1 /*fVal*/)),
					CUtils::PexprScalarConstInt8(pmp, 0 /*fVal*/, true /*fNull*/),
					pexprCoalesce
					);
		}
		else
		{
			// count(*) value can either be NULL (if produced by a lower outer join), or some value >= 0,
			// we return coalesce(count(*), 0) in this case

			*ppexprResidualScalar = pexprCoalesce;
		}

		return fSuccess;
	}

	// residual scalar uses the computed subquery column
	*ppexprResidualScalar = CUtils::PexprScalarIdent(pmp, pcrComputed);
	return fSuccess;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FCreateGrpCols
//
//	@doc:
//		Helper for creating an grouping columns for outer apply expression,
//		the function returns True if creation of grouping columns succeeded
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FCreateGrpCols
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	BOOL fExistential,
	BOOL fOuterRefsUnderInner,
	DrgPcr **ppdrgpcr, // output: constructed grouping columns
	BOOL *pfGbOnInner // output: is Gb created on inner expression
	)
{
	GPOS_ASSERT(NULL != pexprOuter);
	GPOS_ASSERT(NULL != pexprInner);
	GPOS_ASSERT(NULL != ppdrgpcr);
	GPOS_ASSERT(NULL != pfGbOnInner);

	CColRefSet *pcrsOuterOutput =  CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput();
	CColRefSet *pcrsInnerOutput = CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->PcrsOutput();

	BOOL fGbOnInner = false;
	CExpression *pexprScalar = NULL;
	if (!fExistential && !fOuterRefsUnderInner)
	{
		GPOS_ASSERT(COperator::EopLogicalSelect == pexprInner->Pop()->Eopid() && "expecting Select expression");

		pexprScalar = (*pexprInner)[1];
		fGbOnInner = CPredicateUtils::FSimpleEqualityUsingCols(pmp, pexprScalar, pcrsInnerOutput);
	}

	DrgPcr *pdrgpcr = NULL;
	if (fGbOnInner)
	{
		CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexprScalar->PdpDerive())->PcrsUsed();
		CColRefSet *pcrsGb = GPOS_NEW(pmp) CColRefSet(pmp);
		pcrsGb->Include(pcrsUsed);
		pcrsGb->Difference(pcrsOuterOutput);
		GPOS_ASSERT(0 < pcrsGb->CElements());

		pdrgpcr = pcrsGb->Pdrgpcr(pmp);
		pcrsGb->Release();
	}
	else
	{
		if (NULL == CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->Pkc())
		{
			// outer expression must have a key
			return false;
		}

		DrgPcr *pdrgpcrSystemCols = COptCtxt::PoctxtFromTLS()->PdrgpcrSystemCols();
		if (NULL != pdrgpcrSystemCols && 0 < pdrgpcrSystemCols->UlLength())
		{
			CColRefSet *pcrsSystemCols = GPOS_NEW(pmp) CColRefSet(pmp, pdrgpcrSystemCols);
			BOOL fOuterSystemColsReqd = !(pcrsSystemCols->FDisjoint(pcrsOuterOutput));
			pcrsSystemCols->Release();
			if (fOuterSystemColsReqd)
			{
				// bail out if system columns from outer's output are required since we
				// may introduce grouping on unsupported types
				return false;
			}
		}

		// generate a group by on outer columns
		DrgPcr *pdrgpcrKey = NULL;
		pdrgpcr = CUtils::PdrgpcrGroupingKey(pmp, pexprOuter, &pdrgpcrKey);
		pdrgpcrKey->Release(); // key is not used here
	}

	*ppdrgpcr = pdrgpcr;
	*pfGbOnInner = fGbOnInner;

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FCreateOuterApplyForExistOrQuant
//
//	@doc:
//		Helper for creating an outer apply expression for existential/quantified
//		subqueries; see below for an example:
//
//		For queries similar to
//			select (select T.i in (select R.i from R)) from T;
//
//		the expectation is to return for each row in T one of three possible values
//		{TRUE, FALSE, NULL) based on the semantics of IN subquery.
//		For example,
//			- if T.i=1 and R.i = {1,2,3}, return TRUE
//			- if T.i=1 and R.i = {2,3}, return FALSE
//			- if T.i=1 and R.i = {2,3, NULL}, return NULL
//			- if T.i=1 and R.i = {}, return FALSE
//			- if T.i=1 and R.i = {NULL}, return NULL
//
//		To implement these semantics (without the need for correlated execution), the
//		optimizer can generate a special plan alternative during subquery decorrelation
//		that can be simplified as follows:
//
//		+-If (c1 > 0)
//			|    |---{return TRUE}
//			|    +-- else if (c2  > 0)
//			|                   |---{return NULL}
//			|                   +---else {return FALSE}
//			|
//			+--Gb[T.i] , c1 = count(*), c2 = count(NULL values in R.i)
//				+---LOJ_(T.i=R.i  OR  R.i is NULL)
//					|---T
//					+---R
//
//		The reasoning behind this alternative is the following:
//
//		- LOJ will return T.i values along with their matching R.i values.
//			The LOJ will also join a T tuple with any R tuple if R.i is NULL
//
//		- The Gb on top of LOJ groups join results based on T.i values, and for each
//			group, it computes the size of the group  (count(*)), as well as the number
//			of NULL values in R.i
//
//		- After the Gb, the optimizer generates an If operator that checks the values
//		of the two computed aggregates (c1 and c2) and determines what value
//		(TRUE/FALSE/NULL) should be generated for each T tuple based on the IN subquery
//		semantics described above.
//
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FCreateOuterApplyForExistOrQuant
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	CExpression *pexprSubquery,
	BOOL fOuterRefsUnderInner,
	CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar
	)
{
	BOOL fExistential = CUtils::FExistentialSubquery(pexprSubquery->Pop());

	DrgPcr *pdrgpcr = NULL;
	BOOL fGbOnInner = false;
	if (!FCreateGrpCols(pmp, pexprOuter, pexprInner, fExistential, fOuterRefsUnderInner, &pdrgpcr, &fGbOnInner))
	{
		// creating outer apply expression has failed
		return false;
	}
	GPOS_ASSERT(NULL != pdrgpcr);
	GPOS_ASSERT(0 < pdrgpcr->UlLength());

	// add a project node on top of inner expression
	CExpression *pexprPrj = NULL;
	AddProjectNode(pmp, pexprInner, pexprSubquery, &pexprPrj);
	CExpression *pexprPrjList = (*pexprPrj)[1];
	CColRef *pcr = CScalarProjectElement::PopConvert((*pexprPrjList)[0]->Pop())->Pcr();

	// create project list of group by expression
	pexprPrjList = NULL;
	CColRef *pcrBool = pcr;
	CColRef *pcrCount = NULL;
	CColRef *pcrSum = NULL;
	if (fExistential)
	{
		// add the new column introduced by project node
		pdrgpcr->Append(pcr);
		pexprPrjList = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp));
	}
	else
	{
		// quantified subqueries -- generate count(*) and sum(null indicator) expressions
		CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();
		CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

		CExpression *pexprCount = CUtils::PexprCountStar(pmp);
		CScalarAggFunc *popCount = CScalarAggFunc::PopConvert(pexprCount->Pop());
		const IMDType *pmdtypeCount = pmda->Pmdtype(popCount->PmdidType());
		pcrCount = pcf->PcrCreate(pmdtypeCount, popCount->ITypeModifier());
		CExpression *pexprPrjElemCount = CUtils::PexprScalarProjectElement(pmp, pcrCount, pexprCount);

		CExpression *pexprSum = CUtils::PexprSum(pmp, pcr);
		CScalarAggFunc *popSum = CScalarAggFunc::PopConvert(pexprSum->Pop());
		const IMDType *pmdtypeSum = pmda->Pmdtype(popSum->PmdidType());
		pcrSum = pcf->PcrCreate(pmdtypeSum, popSum->ITypeModifier());
		CExpression *pexprPrjElemSum = CUtils::PexprScalarProjectElement(pmp, pcrSum, pexprSum);
		pexprPrjList = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp), pexprPrjElemCount, pexprPrjElemSum);
	}

	if (fGbOnInner)
	{
		CExpression *pexprGb =
			GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalGbAgg(pmp, pdrgpcr, COperator::EgbaggtypeGlobal /*egbaggtype*/),
				pexprPrj,
				pexprPrjList
				);

		// generate an outer apply between outer expression and a group by on inner expression
		*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftOuterApply>(pmp, pexprOuter, pexprGb, pcr, COperator::EopScalarSubquery);
	}
	else
	{
		// generate an outer apply between outer expression and the new project expression
		CExpression *pexprLeftOuterApply = CUtils::PexprLogicalApply<CLogicalLeftOuterApply>(pmp, pexprOuter, pexprPrj, pcr, COperator::EopScalarSubquery);

		*ppexprNewOuter =
			GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalGbAgg(pmp, pdrgpcr, COperator::EgbaggtypeGlobal /*egbaggtype*/),
				pexprLeftOuterApply,
				pexprPrjList
				);
	}

	// residual scalar examines introduced columns
	*ppexprResidualScalar = PexprScalarIf(pmp, pcrBool, pcrSum, pcrCount, pexprSubquery);

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FCreateOuterApply
//
//	@doc:
//		Helper for creating an outer apply expression
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FCreateOuterApply
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	CExpression *pexprSubquery,
	BOOL fOuterRefsUnderInner,
	CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar
	)
{
	COperator *popSubquery = pexprSubquery->Pop();
	BOOL fExistential = CUtils::FExistentialSubquery(popSubquery);
	BOOL fQuantified = CUtils::FQuantifiedSubquery(popSubquery);

	if (fExistential || fQuantified)
	{
		return FCreateOuterApplyForExistOrQuant(pmp, pexprOuter, pexprInner, pexprSubquery, fOuterRefsUnderInner, ppexprNewOuter, ppexprResidualScalar);
	}

	return FCreateOuterApplyForScalarSubquery(pmp, pexprOuter, pexprInner, pexprSubquery, fOuterRefsUnderInner, ppexprNewOuter, ppexprResidualScalar);
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FCreateCorrelatedApplyForQuantifiedSubquery
//
//	@doc:
//		Helper for creating a correlated apply expression for
//		quantified subquery
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FCreateCorrelatedApplyForQuantifiedSubquery
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter,
	CExpression *pexprSubquery,
	ESubqueryCtxt esqctxt,
	CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar
	)
{
	COperator::EOperatorId eopidSubq = pexprSubquery->Pop()->Eopid();
	GPOS_ASSERT(COperator::EopScalarSubqueryAny == eopidSubq ||
			COperator::EopScalarSubqueryAll == eopidSubq);

	// get the logical child of subquery
	CExpression *pexprInner = (*pexprSubquery)[0];
	CScalarSubqueryQuantified *popSubquery = CScalarSubqueryQuantified::PopConvert(pexprSubquery->Pop());
	CColRef *pcr = const_cast<CColRef *>(popSubquery->Pcr());

	// build subquery quantified comparison
	CExpression *pexprResult = NULL;
	CSubqueryHandler sh(pmp, true /* fEnforceCorrelatedApply */);
	CExpression *pexprPredicate = sh.PexprSubqueryPred(pexprInner, pexprSubquery, &pexprResult);

	pexprInner->AddRef();
	if (EsqctxtFilter == esqctxt)
	{
		// we can use correlated semi-IN/anti-semi-NOT-IN apply here since the subquery is used in filtering context
		if (COperator::EopScalarSubqueryAny == eopidSubq)
		{
			*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftSemiCorrelatedApplyIn>(pmp, pexprOuter, pexprResult, pcr, eopidSubq, pexprPredicate);
		}
		else
		{
			*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftAntiSemiCorrelatedApplyNotIn>(pmp, pexprOuter, pexprResult, pcr, eopidSubq, pexprPredicate);
		}
		*ppexprResidualScalar = CUtils::PexprScalarConstBool(pmp, true /*fVal*/);

		return true;
	}

	// subquery occurs in a value context or disjunction, we need to create an outer apply expression
	// add a project node with constant true to be used as subplan place holder
	CExpression *pexprProjectConstTrue =
		CUtils::PexprAddProjection(pmp, pexprResult, CUtils::PexprScalarConstBool(pmp, true /*fVal*/));
	CColRef *pcrBool = CScalarProjectElement::PopConvert((*(*pexprProjectConstTrue)[1])[0]->Pop())->Pcr();

	// add the created column and subquery column to required inner columns
	DrgPcr *pdrgpcrInner = GPOS_NEW(pmp) DrgPcr(pmp);
	pdrgpcrInner->Append(pcrBool);
	pdrgpcrInner->Append(pcr);

	*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftOuterCorrelatedApply>(pmp, pexprOuter, pexprProjectConstTrue, pdrgpcrInner, eopidSubq, pexprPredicate);

	// we replace existential subquery with the boolean column we created,
	// Expr2DXL translator replaces this column with a subplan node
	*ppexprResidualScalar = CUtils::PexprScalarIdent(pmp, pcrBool);

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FCreateCorrelatedApplyForExistentialSubquery
//
//	@doc:
//		Helper for creating a correlated apply expression for
//		existential subquery
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FCreateCorrelatedApplyForExistentialSubquery
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter,
	CExpression *pexprSubquery,
	ESubqueryCtxt esqctxt,
	CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar
	)
{
	COperator::EOperatorId eopidSubq = pexprSubquery->Pop()->Eopid();
	GPOS_ASSERT(COperator::EopScalarSubqueryExists == eopidSubq ||
			COperator::EopScalarSubqueryNotExists == eopidSubq);

	// get the logical child of subquery
	CExpression *pexprInner = (*pexprSubquery)[0];

	// for existential subqueries, any column produced by inner expression
	// can be used to check for empty answers; we use first column for that
	CColRef *pcr = CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->PcrsOutput()->PcrFirst();

	pexprInner->AddRef();
	if (EsqctxtFilter == esqctxt)
	{
		// we can use correlated semi/anti-semi apply here since the subquery is used in filtering context
		if (COperator::EopScalarSubqueryExists == eopidSubq)
		{
			*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftSemiCorrelatedApply>(pmp, pexprOuter, pexprInner, pcr, eopidSubq);
		}
		else
		{
			*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftAntiSemiCorrelatedApply>(pmp, pexprOuter, pexprInner, pcr, eopidSubq);
		}
		*ppexprResidualScalar = CUtils::PexprScalarConstBool(pmp, true /*fVal*/);

		return true;
	}

	// subquery occurs in a value context or disjunction, we need to create an outer apply expression
	// add a project node with constant true to be used as subplan place holder
	CExpression *pexprProjectConstTrue =
		CUtils::PexprAddProjection(pmp, pexprInner, CUtils::PexprScalarConstBool(pmp, true /*fVal*/));
	CColRef *pcrBool = CScalarProjectElement::PopConvert((*(*pexprProjectConstTrue)[1])[0]->Pop())->Pcr();

	// add the created column and subquery column to required inner columns
	DrgPcr *pdrgpcrInner = GPOS_NEW(pmp) DrgPcr(pmp);
	pdrgpcrInner->Append(pcrBool);
	pdrgpcrInner->Append(pcr);

	*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftOuterCorrelatedApply>(pmp, pexprOuter, pexprProjectConstTrue, pdrgpcrInner, eopidSubq);

	// we replace existential subquery with the boolean column we created,
	// Expr2DXL translator replaces this column with a subplan node
	*ppexprResidualScalar = CUtils::PexprScalarIdent(pmp, pcrBool);

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FCreateCorrelatedApplyForExistOrQuant
//
//	@doc:
//		Helper for creating a correlated apply expression for
//		quantified/existential subqueries
//
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FCreateCorrelatedApplyForExistOrQuant
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter,
	CExpression *pexprSubquery,
	ESubqueryCtxt esqctxt,
	CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar
	)
{
	BOOL fExistential = CUtils::FExistentialSubquery(pexprSubquery->Pop());
#ifdef GPOS_DEBUG
	BOOL fQuantified = CUtils::FQuantifiedSubquery(pexprSubquery->Pop());
#endif // GPOS_DEBUG
	GPOS_ASSERT(fExistential || fQuantified);

	if (fExistential)
	{
		return FCreateCorrelatedApplyForExistentialSubquery
				(
				pmp,
				pexprOuter,
				pexprSubquery,
				esqctxt,
				ppexprNewOuter,
				ppexprResidualScalar
				);
	}

	return FCreateCorrelatedApplyForQuantifiedSubquery
		(
		pmp,
		pexprOuter,
		pexprSubquery,
		esqctxt,
		ppexprNewOuter,
		ppexprResidualScalar
		);
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FRemoveAnySubquery
//
//	@doc:
//		Replace a subquery ANY node with a constant True, and create
//		a new Apply expression
//
//		Example:
//
//			SELECT									SELECT
//			/		|								/		|
//			R		=			==>		LEFT-SEMI-APPLY		true
//				/	|					/		|
//				R.a	ANY					R		SELECT
//					/	|						/	 |
//					S	S.b						S		=
//													/	|
//													R.a	S.b
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FRemoveAnySubquery
	(
	CExpression *pexprOuter,
	CExpression *pexprSubquery,
	ESubqueryCtxt esqctxt,
	CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar
	)
{
	IMemoryPool *pmp = m_pmp;

#ifdef GPOS_DEBUG
	AssertValidArguments(pmp, pexprOuter, pexprSubquery, ppexprNewOuter, ppexprResidualScalar);
	COperator *popSubqChild = (*pexprSubquery)[0]->Pop();
	GPOS_ASSERT_IMP(COperator::EopLogicalConstTableGet == popSubqChild->Eopid(),
			0 == CLogicalConstTableGet::PopConvert(popSubqChild)->Pdrgpdrgpdatum()->UlLength() &&
			"Constant subqueries must be unnested during preprocessing");
#endif // GPOS_DEBUG

	if (m_fEnforceCorrelatedApply)
	{
		return FCreateCorrelatedApplyForExistOrQuant(pmp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter, ppexprResidualScalar);
	}

	// get the logical child of subquery
	CExpression *pexprInner = (*pexprSubquery)[0];
	BOOL fOuterRefsUnderInner = pexprInner->FHasOuterRefs();
	const CColRef *pcr = CScalarSubqueryAny::PopConvert(pexprSubquery->Pop())->Pcr();
	COperator::EOperatorId eopidSubq = pexprSubquery->Pop()->Eopid();

	// build subquery quantified comparison
	CExpression *pexprResult = NULL;
	CExpression *pexprPredicate = PexprSubqueryPred(pexprInner, pexprSubquery, &pexprResult);

	// generate a select for the quantified predicate
	pexprInner->AddRef();
	CExpression *pexprSelect = CUtils::PexprLogicalSelect(pmp, pexprResult, pexprPredicate);

	BOOL fSuccess = true;
	if (EsqctxtValue == esqctxt)
	{
		if (!CDrvdPropRelational::Pdprel(pexprResult->PdpDerive())->PcrsNotNull()->FMember(pcr))
		{
			// if inner column is nullable, we create a disjunction to handle null values
			CExpression *pexprNewSelect = PexprInnerSelect(pmp, pcr, pexprResult, pexprPredicate);
			pexprSelect->Release();
			pexprSelect = pexprNewSelect;
		}

		fSuccess = FCreateOuterApply(pmp, pexprOuter, pexprSelect, pexprSubquery, fOuterRefsUnderInner, ppexprNewOuter, ppexprResidualScalar);
		if (!fSuccess)
		{
			pexprSelect->Release();
			fSuccess = FCreateCorrelatedApplyForExistOrQuant(pmp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter, ppexprResidualScalar);
		}
	}
	else
	{
		GPOS_ASSERT(EsqctxtFilter == esqctxt);

		*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftSemiApplyIn>(pmp, pexprOuter, pexprSelect, pcr, eopidSubq);
		*ppexprResidualScalar = CUtils::PexprScalarConstBool(pmp, true /*fVal*/);
	}

	return fSuccess;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::PexprIsNotNull
//
//	@doc:
//		Helper for adding nullness check, only if needed, to the given
//		scalar expression
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryHandler::PexprIsNotNull
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter,
	CExpression *pexprLogical, // the logical parent of scalar expression
	CExpression *pexprScalar
	)
{
	pexprScalar->AddRef();
	BOOL fUsesInnerNullable = CUtils::FUsesNullableCol(pmp, pexprScalar, pexprLogical);

	if (fUsesInnerNullable)
	{
		CColRefSet *pcrsUsed = GPOS_NEW(pmp) CColRefSet(pmp);
		pcrsUsed->Include(CDrvdPropScalar::Pdpscalar(pexprScalar->PdpDerive())->PcrsUsed());
		pcrsUsed->Intersection(CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput());
		BOOL fHasOuterRefs = (0 < pcrsUsed->CElements());
		pcrsUsed->Release();

		if (fHasOuterRefs)
		{
			return CUtils::PexprIsNotNull(pmp, pexprScalar);
		}
	}

	return pexprScalar;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FRemoveAllSubquery
//
//	@doc:
//		Replace a subquery ALL node with a constant True, and create
//		a new Apply expression
//
//		Example:
//
//		SELECT									SELECT
//		/		|								/		|
//		R		<>			==>			LAS-APPLY		true
//				/	|				 /				|
//			   R.a	ALL			SELECT				|
//			  		/	|		/	|				SELECT
//					S	S.b		R	IS_NOT_NULL		/	|
//											|		S	IS_NOT_FALSE
//											R.a				|
//															=
//															/	|
//															R.a	S.b
//
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FRemoveAllSubquery
	(
	CExpression *pexprOuter,
	CExpression *pexprSubquery,
	ESubqueryCtxt esqctxt,
	CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar
	)
{
	IMemoryPool *pmp = m_pmp;
#ifdef GPOS_DEBUG
	AssertValidArguments(pmp, pexprOuter, pexprSubquery, ppexprNewOuter, ppexprResidualScalar);
	COperator *popSubqChild = (*pexprSubquery)[0]->Pop();
	GPOS_ASSERT_IMP(COperator::EopLogicalConstTableGet == popSubqChild->Eopid(),
			0 == CLogicalConstTableGet::PopConvert(popSubqChild)->Pdrgpdrgpdatum()->UlLength() &&
			"Constant subqueries must be unnested during preprocessing");
#endif // GPOS_DEBUG

	if (m_fEnforceCorrelatedApply)
	{
		return FCreateCorrelatedApplyForExistOrQuant(pmp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter, ppexprResidualScalar);
	}

	BOOL fSuccess = true;
	CExpression *pexprInnerSelect = NULL;
	CExpression *pexprPredicate = NULL;
	CExpression *pexprInner = (*pexprSubquery)[0];
	COperator::EOperatorId eopidSubq = pexprSubquery->Pop()->Eopid();
	const CColRef *pcr = CScalarSubqueryAll::PopConvert(pexprSubquery->Pop())->Pcr();

	BOOL fOuterRefsUnderInner = pexprInner->FHasOuterRefs();
	pexprInner->AddRef();

	if (fOuterRefsUnderInner)
	{
		// outer references in SubqueryAll necessitate correlated execution for correctness

		// build subquery quantified comparison
		CExpression *pexprResult = NULL;
		CExpression *pexprPredicate = PexprSubqueryPred(pexprInner, pexprSubquery, &pexprResult);

		*ppexprResidualScalar = CUtils::PexprScalarConstBool(pmp, true /*fVal*/);
		*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftAntiSemiCorrelatedApplyNotIn>(pmp, pexprOuter, pexprResult, pcr, eopidSubq, pexprPredicate);

		return fSuccess;
	}

	CExpression *pexprInversePred = CXformUtils::PexprInversePred(pmp, pexprSubquery);
	// generate a select with the inverse predicate as the selection predicate
	pexprPredicate = pexprInversePred;
	pexprInnerSelect = CUtils::PexprLogicalSelect(pmp, pexprInner, pexprPredicate);

	if (EsqctxtValue == esqctxt)
	{
		const CColRef *pcr = CScalarSubqueryAll::PopConvert(pexprSubquery->Pop())->Pcr();
		if (!CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->PcrsNotNull()->FMember(pcr))
		{
			// if inner column is nullable, we create a disjunction to handle null values
			CExpression *pexprNewInnerSelect = PexprInnerSelect(pmp, pcr, pexprInner, pexprPredicate);
			pexprInnerSelect->Release();
			pexprInnerSelect = pexprNewInnerSelect;
		}

		fSuccess = FCreateOuterApply(pmp, pexprOuter, pexprInnerSelect, pexprSubquery, fOuterRefsUnderInner, ppexprNewOuter, ppexprResidualScalar);
		if (!fSuccess)
		{
			pexprInnerSelect->Release();
			fSuccess = FCreateCorrelatedApplyForExistOrQuant(pmp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter, ppexprResidualScalar);
		}
	}
	else
	{
		GPOS_ASSERT(EsqctxtFilter == esqctxt);

		*ppexprResidualScalar = CUtils::PexprScalarConstBool(pmp, true);
		*ppexprNewOuter =
			CUtils::PexprLogicalApply<CLogicalLeftAntiSemiApplyNotIn>(pmp, pexprOuter, pexprInnerSelect, pcr, eopidSubq);
	}

	return fSuccess;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::AddProjectNode
//
//	@doc:
//		Helper for adding a Project node with a const TRUE on top of
//		the given expression;
//		this is needed as part of unnesting to outer Apply
//
//
//---------------------------------------------------------------------------
void
CSubqueryHandler::AddProjectNode
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CExpression *pexprSubquery,
	CExpression **ppexprResult
	)
{
	GPOS_ASSERT(NULL != ppexprResult);

	GPOS_ASSERT(CUtils::FExistentialSubquery(pexprSubquery->Pop()) ||
			CUtils::FQuantifiedSubquery(pexprSubquery->Pop()));

	CExpression *pexprProjected = NULL;
	if (CUtils::FExistentialSubquery(pexprSubquery->Pop()))
	{
		pexprProjected = CUtils::PexprScalarConstBool(pmp, true /*fVal*/);
	}
	else
	{
		// quantified subquery -- generate a NULL indicator for inner column
		const CColRef *pcrInner = CScalarSubqueryQuantified::PopConvert(pexprSubquery->Pop())->Pcr();
		pexprProjected = CXformUtils::PexprNullIndicator(pmp, CUtils::PexprScalarIdent(pmp, pcrInner));
	}

	*ppexprResult = CUtils::PexprAddProjection(pmp, pexpr, pexprProjected);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::PexprScalarIf
//
//	@doc:
//		Helper for creating a scalar if expression used when generating
//		an outer apply
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryHandler::PexprScalarIf
	(
	IMemoryPool *pmp,
	CColRef *pcrBool,
	CColRef *pcrSum,
	CColRef *pcrCount,
	CExpression *pexprSubquery
	)
{
	COperator *popSubquery = pexprSubquery->Pop();
	BOOL fExistential = CUtils::FExistentialSubquery(popSubquery);
#ifdef GPOS_DEBUG
	BOOL fQuantified = CUtils::FQuantifiedSubquery(popSubquery);
#endif // GPOS_DEBUG
	COperator::EOperatorId eopid = popSubquery->Eopid();

	GPOS_ASSERT(fExistential || fQuantified);
	GPOS_ASSERT_IMP(fExistential, NULL != pcrBool);
	GPOS_ASSERT_IMP(fQuantified, NULL != pcrSum && NULL != pcrCount);

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDTypeBool *pmdtypebool = pmda->PtMDType<IMDTypeBool>();
	IMDId *pmdid = pmdtypebool->Pmdid();

	BOOL fVal = true;
	if (COperator::EopScalarSubqueryNotExists == eopid || COperator::EopScalarSubqueryAll == eopid)
	{
		fVal = false;
	}

	if (fExistential)
	{
		CExpression *pexprIsNotNull = CUtils::PexprIsNotNull(pmp, CUtils::PexprScalarIdent(pmp, pcrBool));
		pmdid->AddRef();
		return GPOS_NEW(pmp) CExpression
						(
						pmp,
						GPOS_NEW(pmp) CScalarIf(pmp, pmdid),
						pexprIsNotNull,
						CUtils::PexprScalarConstBool(pmp, fVal),
						CUtils::PexprScalarConstBool(pmp, !fVal)
						);
	}

	// quantified subquery
	CExpression *pexprEquality = CUtils::PexprScalarEqCmp(pmp, pcrSum, pcrCount);
	CExpression *pexprSumIsNotNull = CUtils::PexprIsNotNull(pmp, CUtils::PexprScalarIdent(pmp, pcrSum));
	pmdid->AddRef();
	pmdid->AddRef();

	// if sum(null indicators) = count(*), all joins involved null values from inner side,
	// in this case, we need to produce a null value in the join result,
	// otherwise we examine nullness of sum to see if a full join result was produced by outer join

	CExpression *pexprScalarIf =
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CScalarIf(pmp, pmdid),
			pexprEquality,
			CUtils::PexprScalarConstBool(pmp, false /*fVal*/, true /*fNull*/),
			GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CScalarIf(pmp, pmdid),
				pexprSumIsNotNull,
				CUtils::PexprScalarConstBool(pmp, fVal),
				CUtils::PexprScalarConstBool(pmp, !fVal)
				)
			);

	// add an outer ScalarIf to check nullness of outer value
	CExpression *pexprScalar = (*pexprSubquery)[1];
	pexprScalar->AddRef();
	pmdid->AddRef();
	return GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CScalarIf(pmp, pmdid),
					CUtils::PexprIsNotNull(pmp, pexprScalar),
					pexprScalarIf,
					CUtils::PexprScalarConstBool(pmp, false /*fVal*/, true /*fNull*/)
					);

}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FRemoveExistentialSubquery
//
//	@doc:
//		Replace a subquery EXISTS/NOT EXISTS node with a constant True, and
//		create a new Apply expression
//
//
//		Example:
//
//				SELECT										SELECT
//				/		|									/			|
//				R		EXISTS		==>				LEFT-SEMI-APPLY		true
//							|						/		|
//							S						R		S
//
//
//			SELECT											SELECT
//			/		|										/			|
//			R		NOT EXISTS		==>			LEFT-ANTI-SEMI-APPLY	true
//						|						/			|
//						S						R			S
//
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FRemoveExistentialSubquery
	(
	IMemoryPool *pmp,
	COperator::EOperatorId eopid,
	CExpression *pexprOuter,
	CExpression *pexprSubquery,
	ESubqueryCtxt esqctxt,
	CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar
	)
{
#ifdef GPOS_DEBUG
	AssertValidArguments(pmp, pexprOuter, pexprSubquery, ppexprNewOuter, ppexprResidualScalar);
	GPOS_ASSERT(COperator::EopScalarSubqueryExists == eopid || COperator::EopScalarSubqueryNotExists == eopid);
	GPOS_ASSERT(eopid == pexprSubquery->Pop()->Eopid());
#endif // GPOS_DEBUG

	CExpression *pexprInner = (*pexprSubquery)[0];
	BOOL fOuterRefsUnderInner = pexprInner->FHasOuterRefs();

	// we always add-ref Apply's inner child since it is reused from subquery
	// inner expression

	pexprInner->AddRef();

	BOOL fSuccess = true;
	if (EsqctxtValue == esqctxt)
	{
		fSuccess = FCreateOuterApply(pmp, pexprOuter, pexprInner, pexprSubquery, fOuterRefsUnderInner, ppexprNewOuter, ppexprResidualScalar);
		if (!fSuccess)
		{
			pexprInner->Release();
			fSuccess = FCreateCorrelatedApplyForExistOrQuant(pmp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter, ppexprResidualScalar);
		}
	}
	else
	{
		GPOS_ASSERT(EsqctxtFilter == esqctxt);

		CDrvdPropRelational *pdpInner = CDrvdPropRelational::Pdprel(pexprInner->PdpDerive());
		// for existential subqueries, any column produced by inner expression
		// can be used to check for empty answers; we use first column for that
		CColRef *pcr = pdpInner->PcrsOutput()->PcrFirst();

		if (COperator::EopScalarSubqueryExists == eopid)
		{
			CColRefSet *pcrsOuterRefs = pdpInner->PcrsOuter();

			if (0 == pcrsOuterRefs->CElements())
			{
				// add a limit operator on top of the inner child if the subquery does not have
				// any outer references. Adding Limit for the correlated case hinders pulling up
				// predicates into an EXISTS join
				pexprInner = CUtils::PexprLimit(pmp, pexprInner, 0, 1);
			}

			*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftSemiApply>(pmp, pexprOuter, pexprInner, pcr, eopid);
		}
		else
		{
			*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftAntiSemiApply>(pmp, pexprOuter, pexprInner, pcr, eopid);
		}
		*ppexprResidualScalar = CUtils::PexprScalarConstBool(pmp, true /*fVal*/);
	}

	return fSuccess;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FRemoveExistsSubquery
//
//	@doc:
//		Replace a subquery EXISTS node with a constant True, and
//		create a new Apply expression

//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FRemoveExistsSubquery
	(
	CExpression *pexprOuter,
	CExpression *pexprSubquery,
	ESubqueryCtxt esqctxt,
	CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar
	)
{
	if (m_fEnforceCorrelatedApply)
	{
		return FCreateCorrelatedApplyForExistOrQuant(m_pmp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter, ppexprResidualScalar);
	}

	return FRemoveExistentialSubquery
		(
		m_pmp,
		COperator::EopScalarSubqueryExists,
		pexprOuter,
		pexprSubquery,
		esqctxt,
		ppexprNewOuter,
		ppexprResidualScalar
		);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FRemoveNotExistsSubquery
//
//	@doc:
//		Replace a subquery NOT EXISTS node with a constant True, and
//		create a new Apply expression
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FRemoveNotExistsSubquery
	(
	CExpression *pexprOuter,
	CExpression *pexprSubquery,
	ESubqueryCtxt esqctxt,
	CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar
	)
{
	if (m_fEnforceCorrelatedApply)
	{
		return FCreateCorrelatedApplyForExistOrQuant(m_pmp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter, ppexprResidualScalar);
	}

	return FRemoveExistentialSubquery
		(
		m_pmp,
		COperator::EopScalarSubqueryNotExists,
		pexprOuter,
		pexprSubquery,
		esqctxt,
		ppexprNewOuter,
		ppexprResidualScalar
		);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FRecursiveHandler
//
//	@doc:
//		Handle subqueries in scalar tree recursively
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FRecursiveHandler
	(
	CExpression *pexprOuter,
	CExpression *pexprScalar,
	ESubqueryCtxt esqctxt,
	CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;

	IMemoryPool *pmp = m_pmp;

#ifdef GPOS_DEBUG
	AssertValidArguments(pmp, pexprOuter, pexprScalar, ppexprNewOuter, ppexprResidualScalar);
#endif // GPOS_DEBUG

	COperator *popScalar = pexprScalar->Pop();

	if (CPredicateUtils::FOr(pexprScalar) ||
	    CPredicateUtils::FNot(pexprScalar) ||
	    COperator::EopScalarProjectElement == popScalar->Eopid())
	{
		// set subquery context to Value
		esqctxt = EsqctxtValue;
	}

	// save the current logical expression
	CExpression *pexprCurrentOuter = pexprOuter;
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	const ULONG ulArity = pexprScalar->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprScalarChild = (*pexprScalar)[ul];
		COperator *popScalarChild = pexprScalarChild->Pop();
		CExpression *pexprNewLogical = NULL;
		CExpression *pexprNewScalar = NULL;

		// Set the subquery context to Value for a non-scalar subquery nested in a
		// scalar expression such that the corresponding subquery unnesting routines
		// will return a column identifier. The identifier is then used to replace the
		// subquery node in the scalar expression.
		//
		// Note that conjunction is special cased here. Multiple non-scalar subqueries
		// will produce nested Apply expressions which are implicitly AND-ed. Thus,
		// unlike disjuctions or negations, it is not neccesary to use the Value
		// context for disjuctions.  This enables us to generate optimal plans with
		// nested joins for queries like:
		// SELECT * FROM t3 WHERE EXISTS(SELECT c FROM t2) AND NOT EXISTS (SELECT c from t3);
		//
		// However, if the AND is nested inside another scalar expression then it must
		// be treated in Value context from this point onwards because an implicit AND
		// does not apply within a scalar expression:
		// SELECT * FROM t1 WHERE b = (EXISTS(SELECT c FROM t2) AND NOT EXISTS (SELECT c from t3));
		//
		// Also note that at this stage we would never have a AND expression whose
		// immediate child is another AND since the conjuncts are always unnested
		// during preprocessing - but this case is handled in anyway.
		if (!CPredicateUtils::FAnd(pexprScalar) &&
		    (CPredicateUtils::FAnd(pexprScalarChild) ||
		     CUtils::FExistentialSubquery(popScalarChild) ||
		     CUtils::FQuantifiedSubquery(popScalarChild)))
		{
			esqctxt = EsqctxtValue;
		}

		if (!FProcess(pexprCurrentOuter, pexprScalarChild, esqctxt, &pexprNewLogical, &pexprNewScalar))
		{
			// subquery unnesting failed, cleanup created expressions
			*ppexprNewOuter = pexprCurrentOuter;
			pdrgpexpr->Release();

			return false;
		}

		GPOS_ASSERT(NULL != pexprNewScalar);

		if (CDrvdPropScalar::Pdpscalar(pexprScalarChild->PdpDerive())->FHasSubquery())
		{
			// the logical expression must have been updated during recursion
			GPOS_ASSERT(NULL != pexprNewLogical);

			// update current logical expression based on recursion results
			pexprCurrentOuter = pexprNewLogical;
		}
		pdrgpexpr->Append(pexprNewScalar);
	}

	*ppexprNewOuter = pexprCurrentOuter;
	COperator *pop = pexprScalar->Pop();
	pop->AddRef();
	*ppexprResidualScalar = GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexpr);

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FProcessScalarOperator
//
//	@doc:
//		Handle subqueries on a case-by-case basis
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FProcessScalarOperator
	(
	CExpression *pexprOuter,
	CExpression *pexprScalar,
	ESubqueryCtxt esqctxt,
	CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar
	)
{
	IMemoryPool *pmp = m_pmp;

#ifdef GPOS_DEBUG
	AssertValidArguments(pmp, pexprOuter, pexprScalar, ppexprNewOuter, ppexprResidualScalar);
#endif // GPOS_DEBUG

	BOOL fSuccess = false;
	COperator::EOperatorId eopid = pexprScalar->Pop()->Eopid();
	switch(eopid)
	{
		case COperator::EopScalarSubquery:
			fSuccess = FRemoveScalarSubquery
							(
							pexprOuter,
							pexprScalar,
							esqctxt,
							ppexprNewOuter,
							ppexprResidualScalar
							);
			break;
		case COperator::EopScalarSubqueryAny:
			fSuccess = FRemoveAnySubquery
							(
							pexprOuter,
							pexprScalar,
							esqctxt,
							ppexprNewOuter,
							ppexprResidualScalar
							);
			break;
		case COperator::EopScalarSubqueryAll:
			fSuccess = FRemoveAllSubquery
							(
							pexprOuter,
							pexprScalar,
							esqctxt,
							ppexprNewOuter,
							ppexprResidualScalar
							);
			break;
		case COperator::EopScalarSubqueryExists:
			fSuccess = FRemoveExistsSubquery
							(
							pexprOuter,
							pexprScalar,
							esqctxt,
							ppexprNewOuter,
							ppexprResidualScalar
							);
			break;
		case COperator::EopScalarSubqueryNotExists:
			fSuccess = FRemoveNotExistsSubquery
							(
							pexprOuter,
							pexprScalar,
							esqctxt,
							ppexprNewOuter,
							ppexprResidualScalar
							);
			break;
		case COperator::EopScalarBoolOp:
		case COperator::EopScalarProjectList:
		case COperator::EopScalarProjectElement:
		case COperator::EopScalarCmp:
		case COperator::EopScalarOp:
		case COperator::EopScalarIsDistinctFrom:
		case COperator::EopScalarNullTest:
		case COperator::EopScalarBooleanTest:
		case COperator::EopScalarIf:
		case COperator::EopScalarFunc:
		case COperator::EopScalarCast:
		case COperator::EopScalarCoerceToDomain:
		case COperator::EopScalarCoerceViaIO:
		case COperator::EopScalarArrayCoerceExpr:
		case COperator::EopScalarAggFunc:
		case COperator::EopScalarWindowFunc:
		case COperator::EopScalarArray:
		case COperator::EopScalarArrayCmp:
		case COperator::EopScalarCoalesce:
		case COperator::EopScalarCaseTest:
		case COperator::EopScalarNullIf:
		case COperator::EopScalarSwitch:
		case COperator::EopScalarSwitchCase:
			fSuccess = FRecursiveHandler
							(
							pexprOuter,
							pexprScalar,
							esqctxt,
							ppexprNewOuter,
							ppexprResidualScalar
							);
			break;
		default:
			GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnexpectedOp, GPOS_WSZ_LIT("Subquery in unexpected context"));

	}

	if (fSuccess)
	{
		// clean-up unnecessary equality operations
		CExpression *pexprPruned = CPredicateUtils::PexprPruneSuperfluosEquality(pmp, *ppexprResidualScalar);
		(*ppexprResidualScalar)->Release();
		*ppexprResidualScalar = pexprPruned;

		// cleanup unncessary conjuncts
		DrgPexpr *pdrgpexpr = CPredicateUtils::PdrgpexprConjuncts(pmp, *ppexprResidualScalar);
		(*ppexprResidualScalar)->Release();
		*ppexprResidualScalar = CPredicateUtils::PexprConjunction(pmp, pdrgpexpr);
	}

	return fSuccess;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FProcess
//
//	@doc:
//		Main driver;
//
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FProcess
	(
	CExpression *pexprOuter, // logical child of a SELECT node
	CExpression *pexprScalar, // scalar child of a SELECT node
	ESubqueryCtxt esqctxt,	// context in which subquery occurs
	CExpression **ppexprNewOuter, // an Apply logical expression produced as output
	CExpression **ppexprResidualScalar // residual scalar expression produced as output
	)
{
#ifdef GPOS_DEBUG
	AssertValidArguments(m_pmp, pexprOuter, pexprScalar, ppexprNewOuter, ppexprResidualScalar);
#endif // GPOS_DEBUG

	if (!CDrvdPropScalar::Pdpscalar(pexprScalar->PdpDerive())->FHasSubquery())
	{
		// no subqueries, add-ref root node and return immediately
		pexprScalar->AddRef();
		*ppexprResidualScalar = pexprScalar;

		return true;
	}

	 return FProcessScalarOperator(pexprOuter, pexprScalar, esqctxt, ppexprNewOuter, ppexprResidualScalar);
}


// EOF

