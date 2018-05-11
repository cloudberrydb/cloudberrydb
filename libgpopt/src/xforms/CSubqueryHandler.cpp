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
	IMemoryPool *mp,
	CExpression *pexprOuter,
	CExpression *pexprScalar,
	CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar
	)
{
	GPOS_ASSERT(NULL != mp);
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
	IMemoryPool *mp,
	CExpression *pexprInput,
	CColRef *colref,
	CExpression *pexprScalar
	)
{
	GPOS_ASSERT(NULL != pexprInput);
	GPOS_ASSERT(NULL != colref);

	COperator *pop = pexprInput->Pop();
	if (pop->Eopid() == CScalar::EopScalarIdent &&
		CScalarIdent::PopConvert(pop)->Pcr() == colref)
	{
		GPOS_ASSERT(NULL != pexprScalar);
		GPOS_ASSERT(pexprScalar->Pop()->FScalar());

		pexprScalar->AddRef();
		return pexprScalar;
	}

	// process children
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);

	const ULONG arity = pexprInput->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = PexprReplace(mp, (*pexprInput)[ul], colref, pexprScalar);
		pdrgpexpr->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexpr);
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

	const CColRef *colref = popSqQuantified->Pcr();
	IMDId *mdid_op = popSqQuantified->MdIdOp();
	const CWStringConst *str = popSqQuantified->PstrOp();

	mdid_op->AddRef();
	CExpression *pexprPredicate = CUtils::PexprScalarCmp(m_mp, pexprNewScalar, colref, *str, mdid_op);

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
	CColRef *colref = NULL;
	GPOS_ASSERT(CUtils::FHasCountAgg((*pexprSubquery)[0], &colref));
	GPOS_ASSERT(colref == pcrCount);
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
	CDrvdPropScalar *pdpscalar = CDrvdPropScalar::GetDrvdScalarProps(pexprPrjList->PdpDerive());

	if (COperator::EopLogicalGbAgg != pexprPrjChild->Pop()->Eopid() ||
		pdpscalar->FHasNonScalarFunction() ||
		IMDFunction::EfsVolatile == pdpscalar->Pfp()->Efs())
	{
		// fail if Project child is not GbAgg, or there are non-scalar/volatile functions in project list
		return false;
	}

	CColRefSet *pcrsOutput = CDrvdPropRelational::GetRelationalProperties(pexprPrjChild->PdpDerive())->PcrsOutput();
	if (1 < pcrsOutput->Size())
	{
		// fail if GbAgg has more than one output column
		return false;
	}

	CColRefSet *pcrsUsed = pdpscalar->PcrsUsed();
	BOOL fPrjUsesCount = (0 == pcrsUsed->Size()) || (1 == pcrsUsed->Size() && pcrsUsed->FMember(pcrCount));
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
		m_returns_set || // subquery produces > 1 rows, we need correlated execution to check for cardinality at runtime
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
	IMemoryPool *mp,
	CExpression *pexprSubquery,
	CExpression *pexprOuter,
	ESubqueryCtxt esqctxt
	)
{
	GPOS_ASSERT(NULL != pexprSubquery);
	GPOS_ASSERT(CUtils::FSubquery(pexprSubquery->Pop()));
	GPOS_ASSERT(NULL != pexprOuter);

	CExpression *pexprInner = (*pexprSubquery)[0];
	CColRefSet *outer_refs = CDrvdPropRelational::GetRelationalProperties((*pexprSubquery)[0]->PdpDerive())->PcrsOuter();
	CColRefSet *pcrsOuterOutput = CDrvdPropRelational::GetRelationalProperties(pexprOuter->PdpDerive())->PcrsOutput();

	SSubqueryDesc *psd = GPOS_NEW(mp) SSubqueryDesc();
	psd->m_returns_set = (1 < CDrvdPropRelational::GetRelationalProperties(pexprInner->PdpDerive())->Maxcard().Ull());
	psd->m_fHasOuterRefs = pexprInner->HasOuterRefs();
	psd->m_fHasVolatileFunctions = (IMDFunction::EfsVolatile == CDrvdPropScalar::GetDrvdScalarProps(pexprSubquery->PdpDerive())->Pfp()->Efs());
	psd->m_fHasSkipLevelCorrelations = 0 < outer_refs->Size() && !pcrsOuterOutput->ContainsAll(outer_refs);

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
	IMemoryPool *pmp = m_mp;

#ifdef GPOS_DEBUG
	AssertValidArguments(m_mp, pexprOuter, pexprSubquery, ppexprNewOuter, ppexprResidualScalar);
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

		CScalarSubquery *popInnerSubq = GPOS_NEW(m_mp) CScalarSubquery(m_mp, psd->m_pcrCountAgg, false /*fGeneratedByExist*/, false /*fGeneratedByQuantified*/);
		pexprGbAgg->AddRef();
		CExpression *pexprNewSubq = GPOS_NEW(m_mp) CExpression(m_mp, popInnerSubq, pexprGbAgg);

		// unnest new subquery
		GPOS_DELETE(psd);
		CExpression *pexprNewOuter = NULL;
		CExpression *pexprResidualScalar = NULL;
		psd = Psd(m_mp, pexprNewSubq, pexprOuter, esqctxt);
		fSuccess = FRemoveScalarSubqueryInternal(m_mp, pexprOuter, pexprNewSubq, EsqctxtValue, psd, m_fEnforceCorrelatedApply, &pexprNewOuter, &pexprResidualScalar);

		if (fSuccess)
		{
			// unnesting succeeded -- replace all occurrences of count(*) column in project list with residual expression
			pexprPrj->Pop()->AddRef();
			CExpression *pexprPrjListNew = PexprReplace(m_mp, pexprPrjList, psd->m_pcrCountAgg, pexprResidualScalar);
			*ppexprNewOuter = GPOS_NEW(m_mp) CExpression(m_mp, pexprPrj->Pop(), pexprNewOuter, pexprPrjListNew);
			*ppexprResidualScalar = CUtils::PexprScalarIdent(m_mp, pcrSubquery);
		}

		CRefCount::SafeRelease(pexprNewSubq);
		CRefCount::SafeRelease(pexprResidualScalar);
	}
	else
	{
		fSuccess = FRemoveScalarSubqueryInternal(m_mp, pexprOuter, pexprSubquery, esqctxt, psd, m_fEnforceCorrelatedApply, ppexprNewOuter, ppexprResidualScalar);
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
	IMemoryPool *mp,
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
	AssertValidArguments(mp, pexprOuter, pexprSubquery, ppexprNewOuter, ppexprResidualScalar);
#endif // GPOS_DEBUG
	GPOS_ASSERT(NULL != psd);
	GPOS_ASSERT(psd->m_fCorrelatedExecution || fEnforceCorrelatedApply);

	CScalarSubquery *popScalarSubquery = CScalarSubquery::PopConvert(pexprSubquery->Pop());
	const CColRef *colref = popScalarSubquery->Pcr();
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
			CExpression *pexprMaxOneRow = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalMaxOneRow(mp), pexprInner);
			*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftOuterApply>(mp, pexprOuter, pexprMaxOneRow, colref, eopidSubq);
		}
		else
		{
			// correlated inner expression requires correlated execution
			*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftOuterCorrelatedApply>(mp, pexprOuter, pexprInner, colref, eopidSubq);
		}
		*ppexprResidualScalar = CUtils::PexprScalarIdent(mp, colref);

		return true;
	}

	GPOS_ASSERT(EsqctxtFilter == esqctxt);

	if (fUseMaxOneRow)
	{
		// we need correlated execution here to check if more than one row are generated
		// by inner expression during execution, we generate a MaxOneRow expression to handle this case
		CExpression *pexprMaxOneRow = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalMaxOneRow(mp), pexprInner);
		*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalInnerApply>(mp, pexprOuter, pexprMaxOneRow, colref, eopidSubq);
	}
	else
	{
		// correlated inner expression requires correlated execution
		*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalInnerCorrelatedApply>(mp, pexprOuter, pexprInner, colref, eopidSubq);
	}
	*ppexprResidualScalar = CUtils::PexprScalarIdent(mp, colref);

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
	IMemoryPool *mp,
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
	AssertValidArguments(mp, pexprOuter, pexprSubquery, ppexprNewOuter, ppexprResidualScalar);
#endif // GPOS_DEBUG
	GPOS_ASSERT(NULL != psd);
	
	if (psd->m_fCorrelatedExecution || fEnforceCorrelatedApply)
	{
		return FGenerateCorrelatedApplyForScalarSubquery(mp, pexprOuter, pexprSubquery, esqctxt, psd, fEnforceCorrelatedApply, ppexprNewOuter, ppexprResidualScalar);
	}

	CScalarSubquery *popScalarSubquery = CScalarSubquery::PopConvert(pexprSubquery->Pop());
	const CColRef *colref = popScalarSubquery->Pcr();

	CExpression *pexprInner = (*pexprSubquery)[0];
	// we always add-ref Apply's inner child since it is reused from subquery
	// inner expression
	pexprInner->AddRef();
	BOOL fSuccess = true;
	if (psd->m_fValueSubquery)
	{
		fSuccess = FCreateOuterApply(mp, pexprOuter, pexprInner, pexprSubquery, psd->m_fHasOuterRefs, ppexprNewOuter, ppexprResidualScalar);
		if (!fSuccess)
		{
			pexprInner->Release();
		}
		return fSuccess;
	}

	GPOS_ASSERT(EsqctxtFilter == esqctxt);

	*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalInnerApply>(mp, pexprOuter, pexprInner, colref, pexprSubquery->Pop()->Eopid());
	*ppexprResidualScalar = CUtils::PexprScalarIdent(mp, colref);

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
	IMemoryPool *mp,
	const CColRef *pcrInner,
	CExpression *pexprInner,
	CExpression *pexprPredicate
	)
{
	GPOS_ASSERT(!CDrvdPropRelational::GetRelationalProperties(pexprInner->PdpDerive())->PcrsNotNull()->FMember(pcrInner) &&
			"subquery's column is not nullable");

	CExpression *pexprIsNull = CUtils::PexprIsNull(mp, CUtils::PexprScalarIdent(mp, pcrInner));
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	pexprPredicate->AddRef();
	pdrgpexpr->Append(pexprPredicate);
	pdrgpexpr->Append(pexprIsNull);
	CExpression *pexprDisj = CPredicateUtils::PexprDisjunction(mp, pdrgpexpr);
	pexprInner->AddRef();

	return CUtils::PexprLogicalSelect(mp, pexprInner, pexprDisj);
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
	IMemoryPool *mp,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	CExpression *pexprSubquery,
	BOOL,  // fOuterRefsUnderInner
	CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar
	)
{
	CScalarSubquery *popSubquery = CScalarSubquery::PopConvert(pexprSubquery->Pop());
	const CColRef *colref = popSubquery->Pcr();
	BOOL fSuccess = true;

	// generate an outer apply between outer expression and the relational child of scalar subquery
	CExpression *pexprLeftOuterApply = CUtils::PexprLogicalApply<CLogicalLeftOuterApply>(mp, pexprOuter, pexprInner, colref, popSubquery->Eopid());

	const CLogicalGbAgg *pgbAgg = NULL;
	BOOL fHasCountAggMatchingColumn = CUtils::FHasCountAggMatchingColumn((*pexprSubquery)[0], colref, &pgbAgg);

	if (!fHasCountAggMatchingColumn)
	{
		// residual scalar uses the scalar subquery column
		*ppexprNewOuter = pexprLeftOuterApply;
		*ppexprResidualScalar = CUtils::PexprScalarIdent(mp, colref);
		return fSuccess;
	}

	// add projection for subquery column
	CExpression *pexprPrj = CUtils::PexprAddProjection(mp, pexprLeftOuterApply, CUtils::PexprScalarIdent(mp, colref));
	const CColRef *pcrComputed = CScalarProjectElement::PopConvert((*(*pexprPrj)[1])[0]->Pop())->Pcr();
	*ppexprNewOuter = pexprPrj;

	BOOL fGeneratedByQuantified =  popSubquery->FGeneratedByQuantified();
	if (fGeneratedByQuantified || (fHasCountAggMatchingColumn && 0 == pgbAgg->Pdrgpcr()->Size()))
	{
		CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
		const IMDTypeInt8 *pmdtypeint8 = md_accessor->PtMDType<IMDTypeInt8>();
		IMDId *pmdidInt8 = pmdtypeint8->MDId();
		pmdidInt8->AddRef();
		CExpression *pexprCoalesce =
				GPOS_NEW(mp) CExpression
					(
					mp,
					GPOS_NEW(mp) CScalarCoalesce(mp, pmdidInt8),
					CUtils::PexprScalarIdent(mp, pcrComputed),
					CUtils::PexprScalarConstInt8(mp, 0 /*val*/)
					);

		if (fGeneratedByQuantified)
		{
			// we produce Null if count(*) value is -1,
			// this case can only occur when transforming quantified subquery to
			// count(*) subquery using CXformSimplifySubquery
			pmdidInt8->AddRef();
			*ppexprResidualScalar =
				GPOS_NEW(mp) CExpression
					(
					mp,
					GPOS_NEW(mp) CScalarIf(mp, pmdidInt8),
					CUtils::PexprScalarEqCmp(mp, pcrComputed, CUtils::PexprScalarConstInt8(mp, -1 /*value*/)),
					CUtils::PexprScalarConstInt8(mp, 0 /*value*/, true /*is_null*/),
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
	*ppexprResidualScalar = CUtils::PexprScalarIdent(mp, pcrComputed);
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
	IMemoryPool *mp,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	BOOL fExistential,
	BOOL fOuterRefsUnderInner,
	CColRefArray **ppdrgpcr, // output: constructed grouping columns
	BOOL *pfGbOnInner // output: is Gb created on inner expression
	)
{
	GPOS_ASSERT(NULL != pexprOuter);
	GPOS_ASSERT(NULL != pexprInner);
	GPOS_ASSERT(NULL != ppdrgpcr);
	GPOS_ASSERT(NULL != pfGbOnInner);

	CColRefSet *pcrsOuterOutput =  CDrvdPropRelational::GetRelationalProperties(pexprOuter->PdpDerive())->PcrsOutput();
	CColRefSet *pcrsInnerOutput = CDrvdPropRelational::GetRelationalProperties(pexprInner->PdpDerive())->PcrsOutput();

	BOOL fGbOnInner = false;
	CExpression *pexprScalar = NULL;
	if (!fExistential && !fOuterRefsUnderInner)
	{
		GPOS_ASSERT(COperator::EopLogicalSelect == pexprInner->Pop()->Eopid() && "expecting Select expression");

		pexprScalar = (*pexprInner)[1];
		fGbOnInner = CPredicateUtils::FSimpleEqualityUsingCols(mp, pexprScalar, pcrsInnerOutput);
	}

	CColRefArray *colref_array = NULL;
	if (fGbOnInner)
	{
		CColRefSet *pcrsUsed = CDrvdPropScalar::GetDrvdScalarProps(pexprScalar->PdpDerive())->PcrsUsed();
		CColRefSet *pcrsGb = GPOS_NEW(mp) CColRefSet(mp);
		pcrsGb->Include(pcrsUsed);
		pcrsGb->Difference(pcrsOuterOutput);
		GPOS_ASSERT(0 < pcrsGb->Size());

		colref_array = pcrsGb->Pdrgpcr(mp);
		pcrsGb->Release();
	}
	else
	{
		if (NULL == CDrvdPropRelational::GetRelationalProperties(pexprOuter->PdpDerive())->Pkc())
		{
			// outer expression must have a key
			return false;
		}

		CColRefArray *pdrgpcrSystemCols = COptCtxt::PoctxtFromTLS()->PdrgpcrSystemCols();
		if (NULL != pdrgpcrSystemCols && 0 < pdrgpcrSystemCols->Size())
		{
			CColRefSet *pcrsSystemCols = GPOS_NEW(mp) CColRefSet(mp, pdrgpcrSystemCols);
			BOOL fOuterSystemColsReqd = !(pcrsSystemCols->IsDisjoint(pcrsOuterOutput));
			pcrsSystemCols->Release();
			if (fOuterSystemColsReqd)
			{
				// bail out if system columns from outer's output are required since we
				// may introduce grouping on unsupported types
				return false;
			}
		}

		// generate a group by on outer columns
		CColRefArray *pdrgpcrKey = NULL;
		colref_array = CUtils::PdrgpcrGroupingKey(mp, pexprOuter, &pdrgpcrKey);
		pdrgpcrKey->Release(); // key is not used here
	}

	*ppdrgpcr = colref_array;
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
	IMemoryPool *mp,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	CExpression *pexprSubquery,
	BOOL fOuterRefsUnderInner,
	CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar
	)
{
	BOOL fExistential = CUtils::FExistentialSubquery(pexprSubquery->Pop());

	CColRefArray *colref_array = NULL;
	BOOL fGbOnInner = false;
	if (!FCreateGrpCols(mp, pexprOuter, pexprInner, fExistential, fOuterRefsUnderInner, &colref_array, &fGbOnInner))
	{
		// creating outer apply expression has failed
		return false;
	}
	GPOS_ASSERT(NULL != colref_array);
	GPOS_ASSERT(0 < colref_array->Size());

	// add a project node on top of inner expression
	CExpression *pexprPrj = NULL;
	AddProjectNode(mp, pexprInner, pexprSubquery, &pexprPrj);
	CExpression *pexprPrjList = (*pexprPrj)[1];
	CColRef *colref = CScalarProjectElement::PopConvert((*pexprPrjList)[0]->Pop())->Pcr();

	// create project list of group by expression
	pexprPrjList = NULL;
	CColRef *pcrBool = colref;
	CColRef *pcrCount = NULL;
	CColRef *pcrSum = NULL;
	if (fExistential)
	{
		// add the new column introduced by project node
		colref_array->Append(colref);
		pexprPrjList = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp));
	}
	else
	{
		// quantified subqueries -- generate count(*) and sum(null indicator) expressions
		CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
		CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

		CExpression *pexprCount = CUtils::PexprCountStar(mp);
		CScalarAggFunc *popCount = CScalarAggFunc::PopConvert(pexprCount->Pop());
		const IMDType *pmdtypeCount = md_accessor->RetrieveType(popCount->MdidType());
		pcrCount = col_factory->PcrCreate(pmdtypeCount, popCount->TypeModifier());
		CExpression *pexprPrjElemCount = CUtils::PexprScalarProjectElement(mp, pcrCount, pexprCount);

		CExpression *pexprSum = CUtils::PexprSum(mp, colref);
		CScalarAggFunc *popSum = CScalarAggFunc::PopConvert(pexprSum->Pop());
		const IMDType *pmdtypeSum = md_accessor->RetrieveType(popSum->MdidType());
		pcrSum = col_factory->PcrCreate(pmdtypeSum, popSum->TypeModifier());
		CExpression *pexprPrjElemSum = CUtils::PexprScalarProjectElement(mp, pcrSum, pexprSum);
		pexprPrjList = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp), pexprPrjElemCount, pexprPrjElemSum);
	}

	if (fGbOnInner)
	{
		CExpression *pexprGb =
			GPOS_NEW(mp) CExpression
				(
				mp,
				GPOS_NEW(mp) CLogicalGbAgg(mp, colref_array, COperator::EgbaggtypeGlobal /*egbaggtype*/),
				pexprPrj,
				pexprPrjList
				);

		// generate an outer apply between outer expression and a group by on inner expression
		*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftOuterApply>(mp, pexprOuter, pexprGb, colref, COperator::EopScalarSubquery);
	}
	else
	{
		// generate an outer apply between outer expression and the new project expression
		CExpression *pexprLeftOuterApply = CUtils::PexprLogicalApply<CLogicalLeftOuterApply>(mp, pexprOuter, pexprPrj, colref, COperator::EopScalarSubquery);

		*ppexprNewOuter =
			GPOS_NEW(mp) CExpression
				(
				mp,
				GPOS_NEW(mp) CLogicalGbAgg(mp, colref_array, COperator::EgbaggtypeGlobal /*egbaggtype*/),
				pexprLeftOuterApply,
				pexprPrjList
				);
	}

	// residual scalar examines introduced columns
	*ppexprResidualScalar = PexprScalarIf(mp, pcrBool, pcrSum, pcrCount, pexprSubquery);

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
	IMemoryPool *mp,
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
		return FCreateOuterApplyForExistOrQuant(mp, pexprOuter, pexprInner, pexprSubquery, fOuterRefsUnderInner, ppexprNewOuter, ppexprResidualScalar);
	}

	return FCreateOuterApplyForScalarSubquery(mp, pexprOuter, pexprInner, pexprSubquery, fOuterRefsUnderInner, ppexprNewOuter, ppexprResidualScalar);
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
	IMemoryPool *mp,
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
	CColRef *colref = const_cast<CColRef *>(popSubquery->Pcr());

	// build subquery quantified comparison
	CExpression *pexprResult = NULL;
	CSubqueryHandler sh(mp, true /* fEnforceCorrelatedApply */);
	CExpression *pexprPredicate = sh.PexprSubqueryPred(pexprInner, pexprSubquery, &pexprResult);

	pexprInner->AddRef();
	if (EsqctxtFilter == esqctxt)
	{
		// we can use correlated semi-IN/anti-semi-NOT-IN apply here since the subquery is used in filtering context
		if (COperator::EopScalarSubqueryAny == eopidSubq)
		{
			*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftSemiCorrelatedApplyIn>(mp, pexprOuter, pexprResult, colref, eopidSubq, pexprPredicate);
		}
		else
		{
			*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftAntiSemiCorrelatedApplyNotIn>(mp, pexprOuter, pexprResult, colref, eopidSubq, pexprPredicate);
		}
		*ppexprResidualScalar = CUtils::PexprScalarConstBool(mp, true /*value*/);

		return true;
	}

	// subquery occurs in a value context or disjunction, we need to create an outer apply expression
	// add a project node with constant true to be used as subplan place holder
	CExpression *pexprProjectConstTrue =
		CUtils::PexprAddProjection(mp, pexprResult, CUtils::PexprScalarConstBool(mp, true /*value*/));
	CColRef *pcrBool = CScalarProjectElement::PopConvert((*(*pexprProjectConstTrue)[1])[0]->Pop())->Pcr();

	// add the created column and subquery column to required inner columns
	CColRefArray *pdrgpcrInner = GPOS_NEW(mp) CColRefArray(mp);
	pdrgpcrInner->Append(pcrBool);
	pdrgpcrInner->Append(colref);

	*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftOuterCorrelatedApply>(mp, pexprOuter, pexprProjectConstTrue, pdrgpcrInner, eopidSubq, pexprPredicate);

	// we replace existential subquery with the boolean column we created,
	// Expr2DXL translator replaces this column with a subplan node
	*ppexprResidualScalar = CUtils::PexprScalarIdent(mp, pcrBool);

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
	IMemoryPool *mp,
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
	CColRef *colref = CDrvdPropRelational::GetRelationalProperties(pexprInner->PdpDerive())->PcrsOutput()->PcrFirst();

	pexprInner->AddRef();
	if (EsqctxtFilter == esqctxt)
	{
		// we can use correlated semi/anti-semi apply here since the subquery is used in filtering context
		if (COperator::EopScalarSubqueryExists == eopidSubq)
		{
			*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftSemiCorrelatedApply>(mp, pexprOuter, pexprInner, colref, eopidSubq);
		}
		else
		{
			*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftAntiSemiCorrelatedApply>(mp, pexprOuter, pexprInner, colref, eopidSubq);
		}
		*ppexprResidualScalar = CUtils::PexprScalarConstBool(mp, true /*value*/);

		return true;
	}

	// subquery occurs in a value context or disjunction, we need to create an outer apply expression
	// add a project node with constant true to be used as subplan place holder
	CExpression *pexprProjectConstTrue =
		CUtils::PexprAddProjection(mp, pexprInner, CUtils::PexprScalarConstBool(mp, true /*value*/));
	CColRef *pcrBool = CScalarProjectElement::PopConvert((*(*pexprProjectConstTrue)[1])[0]->Pop())->Pcr();

	// add the created column and subquery column to required inner columns
	CColRefArray *pdrgpcrInner = GPOS_NEW(mp) CColRefArray(mp);
	pdrgpcrInner->Append(pcrBool);
	pdrgpcrInner->Append(colref);

	*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftOuterCorrelatedApply>(mp, pexprOuter, pexprProjectConstTrue, pdrgpcrInner, eopidSubq);

	// we replace existential subquery with the boolean column we created,
	// Expr2DXL translator replaces this column with a subplan node
	*ppexprResidualScalar = CUtils::PexprScalarIdent(mp, pcrBool);

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
	IMemoryPool *mp,
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
				mp,
				pexprOuter,
				pexprSubquery,
				esqctxt,
				ppexprNewOuter,
				ppexprResidualScalar
				);
	}

	return FCreateCorrelatedApplyForQuantifiedSubquery
		(
		mp,
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
	IMemoryPool *mp = m_mp;

#ifdef GPOS_DEBUG
	AssertValidArguments(mp, pexprOuter, pexprSubquery, ppexprNewOuter, ppexprResidualScalar);
	COperator *popSubqChild = (*pexprSubquery)[0]->Pop();
	GPOS_ASSERT_IMP(COperator::EopLogicalConstTableGet == popSubqChild->Eopid(),
			0 == CLogicalConstTableGet::PopConvert(popSubqChild)->Pdrgpdrgpdatum()->Size() &&
			"Constant subqueries must be unnested during preprocessing");
#endif // GPOS_DEBUG

	if (m_fEnforceCorrelatedApply)
	{
		return FCreateCorrelatedApplyForExistOrQuant(mp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter, ppexprResidualScalar);
	}

	// get the logical child of subquery
	CExpression *pexprInner = (*pexprSubquery)[0];
	BOOL fOuterRefsUnderInner = pexprInner->HasOuterRefs();
	const CColRef *colref = CScalarSubqueryAny::PopConvert(pexprSubquery->Pop())->Pcr();
	COperator::EOperatorId eopidSubq = pexprSubquery->Pop()->Eopid();

	// build subquery quantified comparison
	CExpression *pexprResult = NULL;
	CExpression *pexprPredicate = PexprSubqueryPred(pexprInner, pexprSubquery, &pexprResult);

	// generate a select for the quantified predicate
	pexprInner->AddRef();
	CExpression *pexprSelect = CUtils::PexprLogicalSelect(mp, pexprResult, pexprPredicate);

	BOOL fSuccess = true;
	if (EsqctxtValue == esqctxt)
	{
		if (!CDrvdPropRelational::GetRelationalProperties(pexprResult->PdpDerive())->PcrsNotNull()->FMember(colref))
		{
			// if inner column is nullable, we create a disjunction to handle null values
			CExpression *pexprNewSelect = PexprInnerSelect(mp, colref, pexprResult, pexprPredicate);
			pexprSelect->Release();
			pexprSelect = pexprNewSelect;
		}

		fSuccess = FCreateOuterApply(mp, pexprOuter, pexprSelect, pexprSubquery, fOuterRefsUnderInner, ppexprNewOuter, ppexprResidualScalar);
		if (!fSuccess)
		{
			pexprSelect->Release();
			fSuccess = FCreateCorrelatedApplyForExistOrQuant(mp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter, ppexprResidualScalar);
		}
	}
	else
	{
		GPOS_ASSERT(EsqctxtFilter == esqctxt);

		*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftSemiApplyIn>(mp, pexprOuter, pexprSelect, colref, eopidSubq);
		*ppexprResidualScalar = CUtils::PexprScalarConstBool(mp, true /*value*/);
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
	IMemoryPool *mp,
	CExpression *pexprOuter,
	CExpression *pexprLogical, // the logical parent of scalar expression
	CExpression *pexprScalar
	)
{
	pexprScalar->AddRef();
	BOOL fUsesInnerNullable = CUtils::FUsesNullableCol(mp, pexprScalar, pexprLogical);

	if (fUsesInnerNullable)
	{
		CColRefSet *pcrsUsed = GPOS_NEW(mp) CColRefSet(mp);
		pcrsUsed->Include(CDrvdPropScalar::GetDrvdScalarProps(pexprScalar->PdpDerive())->PcrsUsed());
		pcrsUsed->Intersection(CDrvdPropRelational::GetRelationalProperties(pexprOuter->PdpDerive())->PcrsOutput());
		BOOL fHasOuterRefs = (0 < pcrsUsed->Size());
		pcrsUsed->Release();

		if (fHasOuterRefs)
		{
			return CUtils::PexprIsNotNull(mp, pexprScalar);
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
	IMemoryPool *mp = m_mp;
#ifdef GPOS_DEBUG
	AssertValidArguments(mp, pexprOuter, pexprSubquery, ppexprNewOuter, ppexprResidualScalar);
	COperator *popSubqChild = (*pexprSubquery)[0]->Pop();
	GPOS_ASSERT_IMP(COperator::EopLogicalConstTableGet == popSubqChild->Eopid(),
			0 == CLogicalConstTableGet::PopConvert(popSubqChild)->Pdrgpdrgpdatum()->Size() &&
			"Constant subqueries must be unnested during preprocessing");
#endif // GPOS_DEBUG

	if (m_fEnforceCorrelatedApply)
	{
		return FCreateCorrelatedApplyForExistOrQuant(mp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter, ppexprResidualScalar);
	}

	BOOL fSuccess = true;
	CExpression *pexprInnerSelect = NULL;
	CExpression *pexprPredicate = NULL;
	CExpression *pexprInner = (*pexprSubquery)[0];
	COperator::EOperatorId eopidSubq = pexprSubquery->Pop()->Eopid();
	const CColRef *colref = CScalarSubqueryAll::PopConvert(pexprSubquery->Pop())->Pcr();

	BOOL fOuterRefsUnderInner = pexprInner->HasOuterRefs();
	pexprInner->AddRef();

	if (fOuterRefsUnderInner)
	{
		// outer references in SubqueryAll necessitate correlated execution for correctness

		// build subquery quantified comparison
		CExpression *pexprResult = NULL;
		CExpression *pexprPredicate = PexprSubqueryPred(pexprInner, pexprSubquery, &pexprResult);

		*ppexprResidualScalar = CUtils::PexprScalarConstBool(mp, true /*value*/);
		*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftAntiSemiCorrelatedApplyNotIn>(mp, pexprOuter, pexprResult, colref, eopidSubq, pexprPredicate);

		return fSuccess;
	}

	CExpression *pexprInversePred = CXformUtils::PexprInversePred(mp, pexprSubquery);
	// generate a select with the inverse predicate as the selection predicate
	pexprPredicate = pexprInversePred;
	pexprInnerSelect = CUtils::PexprLogicalSelect(mp, pexprInner, pexprPredicate);

	if (EsqctxtValue == esqctxt)
	{
		const CColRef *colref = CScalarSubqueryAll::PopConvert(pexprSubquery->Pop())->Pcr();
		if (!CDrvdPropRelational::GetRelationalProperties(pexprInner->PdpDerive())->PcrsNotNull()->FMember(colref))
		{
			// if inner column is nullable, we create a disjunction to handle null values
			CExpression *pexprNewInnerSelect = PexprInnerSelect(mp, colref, pexprInner, pexprPredicate);
			pexprInnerSelect->Release();
			pexprInnerSelect = pexprNewInnerSelect;
		}

		fSuccess = FCreateOuterApply(mp, pexprOuter, pexprInnerSelect, pexprSubquery, fOuterRefsUnderInner, ppexprNewOuter, ppexprResidualScalar);
		if (!fSuccess)
		{
			pexprInnerSelect->Release();
			fSuccess = FCreateCorrelatedApplyForExistOrQuant(mp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter, ppexprResidualScalar);
		}
	}
	else
	{
		GPOS_ASSERT(EsqctxtFilter == esqctxt);

		*ppexprResidualScalar = CUtils::PexprScalarConstBool(mp, true);
		*ppexprNewOuter =
			CUtils::PexprLogicalApply<CLogicalLeftAntiSemiApplyNotIn>(mp, pexprOuter, pexprInnerSelect, colref, eopidSubq);
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
	IMemoryPool *mp,
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
		pexprProjected = CUtils::PexprScalarConstBool(mp, true /*value*/);
	}
	else
	{
		// quantified subquery -- generate a NULL indicator for inner column
		const CColRef *pcrInner = CScalarSubqueryQuantified::PopConvert(pexprSubquery->Pop())->Pcr();
		pexprProjected = CXformUtils::PexprNullIndicator(mp, CUtils::PexprScalarIdent(mp, pcrInner));
	}

	*ppexprResult = CUtils::PexprAddProjection(mp, pexpr, pexprProjected);
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
	IMemoryPool *mp,
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
	COperator::EOperatorId op_id = popSubquery->Eopid();

	GPOS_ASSERT(fExistential || fQuantified);
	GPOS_ASSERT_IMP(fExistential, NULL != pcrBool);
	GPOS_ASSERT_IMP(fQuantified, NULL != pcrSum && NULL != pcrCount);

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDTypeBool *pmdtypebool = md_accessor->PtMDType<IMDTypeBool>();
	IMDId *mdid = pmdtypebool->MDId();

	BOOL value = true;
	if (COperator::EopScalarSubqueryNotExists == op_id || COperator::EopScalarSubqueryAll == op_id)
	{
		value = false;
	}

	if (fExistential)
	{
		CExpression *pexprIsNotNull = CUtils::PexprIsNotNull(mp, CUtils::PexprScalarIdent(mp, pcrBool));
		mdid->AddRef();
		return GPOS_NEW(mp) CExpression
						(
						mp,
						GPOS_NEW(mp) CScalarIf(mp, mdid),
						pexprIsNotNull,
						CUtils::PexprScalarConstBool(mp, value),
						CUtils::PexprScalarConstBool(mp, !value)
						);
	}

	// quantified subquery
	CExpression *pexprEquality = CUtils::PexprScalarEqCmp(mp, pcrSum, pcrCount);
	CExpression *pexprSumIsNotNull = CUtils::PexprIsNotNull(mp, CUtils::PexprScalarIdent(mp, pcrSum));
	mdid->AddRef();
	mdid->AddRef();

	// if sum(null indicators) = count(*), all joins involved null values from inner side,
	// in this case, we need to produce a null value in the join result,
	// otherwise we examine nullness of sum to see if a full join result was produced by outer join

	CExpression *pexprScalarIf =
		GPOS_NEW(mp) CExpression
			(
			mp,
			GPOS_NEW(mp) CScalarIf(mp, mdid),
			pexprEquality,
			CUtils::PexprScalarConstBool(mp, false /*value*/, true /*is_null*/),
			GPOS_NEW(mp) CExpression
				(
				mp,
				GPOS_NEW(mp) CScalarIf(mp, mdid),
				pexprSumIsNotNull,
				CUtils::PexprScalarConstBool(mp, value),
				CUtils::PexprScalarConstBool(mp, !value)
				)
			);

	// add an outer ScalarIf to check nullness of outer value
	CExpression *pexprScalar = (*pexprSubquery)[1];
	pexprScalar->AddRef();
	mdid->AddRef();
	return GPOS_NEW(mp) CExpression
					(
					mp,
					GPOS_NEW(mp) CScalarIf(mp, mdid),
					CUtils::PexprIsNotNull(mp, pexprScalar),
					pexprScalarIf,
					CUtils::PexprScalarConstBool(mp, false /*value*/, true /*is_null*/)
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
	IMemoryPool *mp,
	COperator::EOperatorId op_id,
	CExpression *pexprOuter,
	CExpression *pexprSubquery,
	ESubqueryCtxt esqctxt,
	CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar
	)
{
#ifdef GPOS_DEBUG
	AssertValidArguments(mp, pexprOuter, pexprSubquery, ppexprNewOuter, ppexprResidualScalar);
	GPOS_ASSERT(COperator::EopScalarSubqueryExists == op_id || COperator::EopScalarSubqueryNotExists == op_id);
	GPOS_ASSERT(op_id == pexprSubquery->Pop()->Eopid());
#endif // GPOS_DEBUG

	CExpression *pexprInner = (*pexprSubquery)[0];
	BOOL fOuterRefsUnderInner = pexprInner->HasOuterRefs();

	// we always add-ref Apply's inner child since it is reused from subquery
	// inner expression

	pexprInner->AddRef();

	BOOL fSuccess = true;
	if (EsqctxtValue == esqctxt)
	{
		fSuccess = FCreateOuterApply(mp, pexprOuter, pexprInner, pexprSubquery, fOuterRefsUnderInner, ppexprNewOuter, ppexprResidualScalar);
		if (!fSuccess)
		{
			pexprInner->Release();
			fSuccess = FCreateCorrelatedApplyForExistOrQuant(mp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter, ppexprResidualScalar);
		}
	}
	else
	{
		GPOS_ASSERT(EsqctxtFilter == esqctxt);

		CDrvdPropRelational *pdpInner = CDrvdPropRelational::GetRelationalProperties(pexprInner->PdpDerive());
		// for existential subqueries, any column produced by inner expression
		// can be used to check for empty answers; we use first column for that
		CColRef *colref = pdpInner->PcrsOutput()->PcrFirst();

		if (COperator::EopScalarSubqueryExists == op_id)
		{
			CColRefSet *outer_refs = pdpInner->PcrsOuter();

			if (0 == outer_refs->Size())
			{
				// add a limit operator on top of the inner child if the subquery does not have
				// any outer references. Adding Limit for the correlated case hinders pulling up
				// predicates into an EXISTS join
				pexprInner = CUtils::PexprLimit(mp, pexprInner, 0, 1);
			}

			*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftSemiApply>(mp, pexprOuter, pexprInner, colref, op_id);
		}
		else
		{
			*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftAntiSemiApply>(mp, pexprOuter, pexprInner, colref, op_id);
		}
		*ppexprResidualScalar = CUtils::PexprScalarConstBool(mp, true /*value*/);
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
		return FCreateCorrelatedApplyForExistOrQuant(m_mp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter, ppexprResidualScalar);
	}

	return FRemoveExistentialSubquery
		(
		m_mp,
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
		return FCreateCorrelatedApplyForExistOrQuant(m_mp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter, ppexprResidualScalar);
	}

	return FRemoveExistentialSubquery
		(
		m_mp,
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

	IMemoryPool *mp = m_mp;

#ifdef GPOS_DEBUG
	AssertValidArguments(mp, pexprOuter, pexprScalar, ppexprNewOuter, ppexprResidualScalar);
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
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG arity = pexprScalar->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
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

		if (CDrvdPropScalar::GetDrvdScalarProps(pexprScalarChild->PdpDerive())->FHasSubquery())
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
	*ppexprResidualScalar = GPOS_NEW(mp) CExpression(mp, pop, pdrgpexpr);

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
	IMemoryPool *mp = m_mp;

#ifdef GPOS_DEBUG
	AssertValidArguments(mp, pexprOuter, pexprScalar, ppexprNewOuter, ppexprResidualScalar);
#endif // GPOS_DEBUG

	BOOL fSuccess = false;
	COperator::EOperatorId op_id = pexprScalar->Pop()->Eopid();
	switch(op_id)
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
		CExpression *pexprPruned = CPredicateUtils::PexprPruneSuperfluosEquality(mp, *ppexprResidualScalar);
		(*ppexprResidualScalar)->Release();
		*ppexprResidualScalar = pexprPruned;

		// cleanup unncessary conjuncts
		CExpressionArray *pdrgpexpr = CPredicateUtils::PdrgpexprConjuncts(mp, *ppexprResidualScalar);
		(*ppexprResidualScalar)->Release();
		*ppexprResidualScalar = CPredicateUtils::PexprConjunction(mp, pdrgpexpr);
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
	AssertValidArguments(m_mp, pexprOuter, pexprScalar, ppexprNewOuter, ppexprResidualScalar);
#endif // GPOS_DEBUG

	if (!CDrvdPropScalar::GetDrvdScalarProps(pexprScalar->PdpDerive())->FHasSubquery())
	{
		// no subqueries, add-ref root node and return immediately
		pexprScalar->AddRef();
		*ppexprResidualScalar = pexprScalar;

		return true;
	}

	 return FProcessScalarOperator(pexprOuter, pexprScalar, esqctxt, ppexprNewOuter, ppexprResidualScalar);
}


// EOF

