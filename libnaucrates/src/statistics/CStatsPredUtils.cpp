//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2014 Pivotal Inc.
//
//	@filename:
//		CStatsPredUtils.cpp
//
//	@doc:
//		Statistics predicate helper routines
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CCastUtils.h"
#include "gpopt/exception.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CExpressionUtils.h"
#include "gpopt/operators/CPredicateUtils.h"

#include "naucrates/statistics/CStatsPredUtils.h"
#include "naucrates/statistics/CStatisticsUtils.h"
#include "naucrates/statistics/CStatsPredLike.h"
#include "naucrates/statistics/CHistogram.h"

#include "gpopt/mdcache/CMDAccessor.h"

#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CStatsPredDisj.h"
#include "naucrates/statistics/CStatsPredConj.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::StatsCmpType
//
//	@doc:
//		For the purpose of statistics computation, what is the comparison
//		type of an operator. Note that this has different, looser semantics
//		than CUtils::ParseCmpType
//
//---------------------------------------------------------------------------
CStatsPred::EStatsCmpType
CStatsPredUtils::StatsCmpType
	(
	const CWStringConst *str_opname
	)
{
	GPOS_ASSERT(NULL != str_opname);

	CStatsPred::EStatsCmpType stats_cmp_type = CStatsPred::EstatscmptOther;

	CWStringConst str_lt(GPOS_WSZ_LIT("<"));
	CWStringConst str_leq(GPOS_WSZ_LIT("<="));
	CWStringConst str_eq(GPOS_WSZ_LIT("="));
	CWStringConst str_geq(GPOS_WSZ_LIT(">="));
	CWStringConst str_gt(GPOS_WSZ_LIT(">"));
	CWStringConst str_neq(GPOS_WSZ_LIT("<>"));

	if (str_opname->Equals(&str_lt))
	{
		stats_cmp_type = CStatsPred::EstatscmptL;
	}
	if (str_opname->Equals(&str_leq))
	{
		stats_cmp_type = CStatsPred::EstatscmptLEq;
	}
	if (str_opname->Equals(&str_eq))
	{
		stats_cmp_type = CStatsPred::EstatscmptEq;
	}
	if (str_opname->Equals(&str_geq))
	{
		stats_cmp_type = CStatsPred::EstatscmptGEq;
	}
	if (str_opname->Equals(&str_gt))
	{
		stats_cmp_type = CStatsPred::EstatscmptG;
	}
	if (str_opname->Equals(&str_neq))
	{
		stats_cmp_type = CStatsPred::EstatscmptNEq;
	}

	return stats_cmp_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::StatsCmpType
//
//	@doc:
//		For the purpose of statistics computation, what is the comparison
//		type of an operator. Note that this has different, looser semantics
//		than CUtils::ParseCmpType
//
//---------------------------------------------------------------------------
CStatsPred::EStatsCmpType
CStatsPredUtils::StatsCmpType
	(
	IMDId *mdid
	)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDScalarOp *md_scalar_op = md_accessor->RetrieveScOp(mdid);

	// Simply go by operator name.
	// If the name of the operator is "<", then it is a LessThan etc.
	const CWStringConst *str_opname = md_scalar_op->Mdname().GetMDName();

	return StatsCmpType(str_opname);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::CreateStatsPredUnsupported
//
//	@doc:
//		Create an unsupported statistics predicate
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::CreateStatsPredUnsupported
	(
	IMemoryPool *mp,
	CExpression *, // predicate_expr,
	CColRefSet * //outer_refs
	)
{
	return GPOS_NEW(mp)
		CStatsPredUnsupported(gpos::ulong_max, CStatsPred::EstatscmptOther);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::GetStatsPredNullTest
//
//	@doc:
//		Extract statistics filtering information from a null test
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::GetStatsPredNullTest
	(
	IMemoryPool *mp,
	CExpression *predicate_expr,
	CColRefSet * //outer_refs
	)
{
	GPOS_ASSERT(NULL != predicate_expr);
	GPOS_ASSERT(IsPredScalarIdentIsNull(predicate_expr) || IsPredScalarIdentIsNotNull(predicate_expr));

	CExpression *expr_null_test = predicate_expr;
	CStatsPred::EStatsCmpType stats_cmp_type = CStatsPred::EstatscmptEq; // 'is null'

	if (IsPredScalarIdentIsNotNull(predicate_expr))
	{
		expr_null_test = (*predicate_expr)[0];
		stats_cmp_type = CStatsPred::EstatscmptNEq; // 'is not null'
	}

	CScalarIdent *scalar_ident_op = CScalarIdent::PopConvert((*expr_null_test)[0]->Pop());
	const CColRef *col_ref = scalar_ident_op->Pcr();

	IDatum *datum = CStatisticsUtils::DatumNull(col_ref);
	if (!datum->StatsAreComparable(datum))
	{
		// stats calculations on such datums unsupported
		datum->Release();

		return GPOS_NEW(mp) CStatsPredUnsupported(col_ref->Id(), stats_cmp_type);
	}

	CPoint *point = GPOS_NEW(mp) CPoint(datum);
	CStatsPredPoint *pred_stats = GPOS_NEW(mp) CStatsPredPoint(col_ref->Id(), stats_cmp_type, point);

	return pred_stats;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::GetStatsPredPoint
//
//	@doc:
//		Extract statistics filtering information from a point comparison
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::GetStatsPredPoint
	(
	IMemoryPool *mp,
	CExpression *predicate_expr,
	CColRefSet *//outer_refs,
	)
{
	GPOS_ASSERT(NULL != predicate_expr);

	CStatsPred *pred_stats = GetPredStats(mp, predicate_expr);
	GPOS_ASSERT (NULL != pred_stats);

	return pred_stats;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::GetStatsCmpType
//
//	@doc:
// 		Return the statistics predicate comparison type
//---------------------------------------------------------------------------
CStatsPred::EStatsCmpType
CStatsPredUtils::GetStatsCmpType
	(
	IMDId *mdid
	)
{
	GPOS_ASSERT(NULL != mdid);
	CStatsPred::EStatsCmpType stats_cmp_type = StatsCmpType(mdid);

	if (CStatsPred::EstatscmptOther != stats_cmp_type)
	{
		return stats_cmp_type;
	}

	if (CPredicateUtils::FLikePredicate(mdid))
	{
		return CStatsPred::EstatscmptLike;
	}

	return CStatsPred::EstatscmptOther;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::GetPredStats
//
//	@doc:
// 		Generate a statistics point predicate for expressions that are supported.
//		Else an unsupported predicate. Note that the comparison semantics extracted are
//		for statistics computation only.
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::GetPredStats
	(
	IMemoryPool *mp,
	CExpression *expr
	)
{
	GPOS_ASSERT(NULL != expr);

	CExpression *expr_left = NULL;
	CExpression *expr_right = NULL;

	CStatsPred::EStatsCmpType stats_cmp_type = CStatsPred::EstatscmptOther;
	if (CPredicateUtils::FIdentIDFConstIgnoreCast(expr))
	{
		expr_left = (*expr)[0];
		expr_right = (*expr)[1];

		stats_cmp_type = CStatsPred::EstatscmptIDF;
	}
	else if (CPredicateUtils::FIdentINDFConstIgnoreCast(expr))
	{
		CExpression *expr_child = (*expr)[0];
		expr_left = (*expr_child)[0];
		expr_right = (*expr_child)[1];

		stats_cmp_type = CStatsPred::EstatscmptINDF;
	}
	else
	{
		expr_left = (*expr)[0];
		expr_right = (*expr)[1];

		GPOS_ASSERT(CPredicateUtils::FIdentCompareConstIgnoreCast(expr, COperator::EopScalarCmp));

		COperator *expr_operator = expr->Pop();
		CScalarCmp *scalar_cmp_op = CScalarCmp::PopConvert(expr_operator);

		// Comparison semantics for statistics purposes is looser
		// than regular comparison
		stats_cmp_type = GetStatsCmpType(scalar_cmp_op->MdIdOp());
	}

	GPOS_ASSERT(COperator::EopScalarIdent == expr_left->Pop()->Eopid() || CScalarIdent::FCastedScId(expr_left));
	GPOS_ASSERT(COperator::EopScalarConst == expr_right->Pop()->Eopid() || CScalarConst::FCastedConst(expr_right));

	const CColRef *col_ref = CCastUtils::PcrExtractFromScIdOrCastScId(expr_left);
	CScalarConst *scalar_const_op = CScalarConst::PopExtractFromConstOrCastConst(expr_right);
	GPOS_ASSERT(NULL != scalar_const_op);

	IDatum *datum = scalar_const_op->GetDatum();
	if (!CHistogram::SupportsFilter(stats_cmp_type) || !IMDType::StatsAreComparable(col_ref->RetrieveType(), datum))
	{
		// case 1: unsupported predicate for stats calculations
		// example: SELECT 1 FROM pg_catalog.pg_class c WHERE c.relname ~ '^(t36)$';
		// case 2: unsupported stats comparison between the column and datum

		return GPOS_NEW(mp) CStatsPredUnsupported(col_ref->Id(), stats_cmp_type);
	}

	return GPOS_NEW(mp) CStatsPredPoint(mp, col_ref, stats_cmp_type, datum);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::IsPredCmpColsOrIgnoreCast
//
//	@doc:
// 		Is the expression a comparison of scalar ident or cast of a scalar ident?
//		Extract relevant info.
//
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::IsPredCmpColsOrIgnoreCast
	(
	CExpression *expr,
	const CColRef **col_ref_left,
	CStatsPred::EStatsCmpType *stats_pred_cmp_type,
	const CColRef **col_ref_right
	)
{
	GPOS_ASSERT(NULL != col_ref_left);
	GPOS_ASSERT(NULL != col_ref_right);
	COperator *expr_op = expr->Pop();

	BOOL is_INDF = CPredicateUtils::FINDF(expr);
	BOOL is_IDF = CPredicateUtils::FIDF(expr);
	BOOL is_scalar_cmp = (COperator::EopScalarCmp == expr_op->Eopid());
	if (!is_scalar_cmp && !is_INDF && !is_IDF)
	{
		return false;
	}

	CExpression *expr_left = NULL;
	CExpression *expr_right = NULL;

	if (is_INDF)
	{
		(*stats_pred_cmp_type) = CStatsPred::EstatscmptINDF;
		CExpression *expr_IDF = (*expr)[0];
		expr_left = (*expr_IDF)[0];
		expr_right = (*expr_IDF)[1];
	}
	else if (is_IDF)
	{
		(*stats_pred_cmp_type) = CStatsPred::EstatscmptIDF;
		expr_left = (*expr)[0];
		expr_right = (*expr)[1];
	}
	else
	{
		GPOS_ASSERT(is_scalar_cmp);

		CScalarCmp *sc_cmp_op = CScalarCmp::PopConvert(expr_op);

		// Comparison semantics for stats purposes is looser
		// than regular comparison.
		(*stats_pred_cmp_type) = CStatsPredUtils::StatsCmpType(sc_cmp_op->MdIdOp());

		expr_left = (*expr)[0];
		expr_right = (*expr)[1];
	}

	(*col_ref_left) = CCastUtils::PcrExtractFromScIdOrCastScId(expr_left);
	(*col_ref_right) = CCastUtils::PcrExtractFromScIdOrCastScId(expr_right);

	if (NULL == *col_ref_left || NULL == *col_ref_right)
	{
		// failed to extract a scalar ident
		return false;
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::ExtractPredStats
//
//	@doc:
//		Extract scalar expression for statistics filtering
//
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::ExtractPredStats
	(
	IMemoryPool *mp,
	CExpression *scalar_expr,
	CColRefSet *outer_refs
	)
{
	GPOS_ASSERT(NULL != scalar_expr);
	if (CPredicateUtils::FOr(scalar_expr))
	{
		CStatsPred *disjunctive_pred_stats = CreateStatsPredDisj(mp, scalar_expr, outer_refs);
		if (NULL != disjunctive_pred_stats)
		{
			return disjunctive_pred_stats;
		}
	}
	else
	{
		CStatsPred *conjunctive_pred_stats = CreateStatsPredConj(mp, scalar_expr, outer_refs);
		if (NULL != conjunctive_pred_stats)
		{
			return conjunctive_pred_stats;
		}
	}

	return GPOS_NEW(mp) CStatsPredConj(GPOS_NEW(mp) CStatsPredPtrArry(mp));
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::CreateStatsPredConj
//
//	@doc:
//		Create conjunctive statistics filter composed of the extracted
//		components of the conjunction
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::CreateStatsPredConj
	(
	IMemoryPool *mp,
	CExpression *scalar_expr,
	CColRefSet *outer_refs
	)
{
	GPOS_ASSERT(NULL != scalar_expr);
	CExpressionArray *pred_expr_conjuncts = CPredicateUtils::PdrgpexprConjuncts(mp, scalar_expr);
	const ULONG size = pred_expr_conjuncts->Size();

	CStatsPredPtrArry *pred_stats_array = GPOS_NEW(mp) CStatsPredPtrArry(mp);
	for (ULONG ul = 0; ul < size; ul++)
	{
		CExpression *predicate_expr = (*pred_expr_conjuncts)[ul];
		CColRefSet *col_refset_used = CDrvdPropScalar::GetDrvdScalarProps(predicate_expr->PdpDerive())->PcrsUsed();
		if (NULL != outer_refs && outer_refs->ContainsAll(col_refset_used))
		{
			// skip predicate with outer references
			continue;
		}

		if (CPredicateUtils::FOr(predicate_expr))
		{
			CStatsPred *disjunctive_pred_stats = CreateStatsPredDisj(mp, predicate_expr, outer_refs);
			if (NULL != disjunctive_pred_stats)
			{
				pred_stats_array->Append(disjunctive_pred_stats);
			}
		}
		else
		{
			AddSupportedStatsFilters(mp, pred_stats_array, predicate_expr, outer_refs);
		}
	}

	pred_expr_conjuncts->Release();

	if (0 < pred_stats_array->Size())
	{
		return GPOS_NEW(mp) CStatsPredConj(pred_stats_array);
	}

	pred_stats_array->Release();

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::CreateStatsPredDisj
//
//	@doc:
//		Create disjunctive statistics filter composed of the extracted
//		components of the disjunction
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::CreateStatsPredDisj
	(
	IMemoryPool *mp,
	CExpression *predicate_expr,
	CColRefSet *outer_refs
	)
{
	GPOS_ASSERT(NULL != predicate_expr);
	GPOS_ASSERT(CPredicateUtils::FOr(predicate_expr));

	CStatsPredPtrArry *pred_stats_disj_child = GPOS_NEW(mp) CStatsPredPtrArry(mp);

	// remove duplicate components of the OR tree
	CExpression *expr_copy = CExpressionUtils::PexprDedupChildren(mp, predicate_expr);

	// extract the components of the OR tree
	CExpressionArray *disjunct_expr = CPredicateUtils::PdrgpexprDisjuncts(mp, expr_copy);
	const ULONG size = disjunct_expr->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CExpression *expr = (*disjunct_expr)[ul];
		CColRefSet *col_refset_used = CDrvdPropScalar::GetDrvdScalarProps(expr->PdpDerive())->PcrsUsed();
		if (NULL != outer_refs && outer_refs->ContainsAll(col_refset_used))
		{
			// skip predicate with outer references
			continue;
		}

		AddSupportedStatsFilters(mp, pred_stats_disj_child, expr, outer_refs);
	}

	// clean up
	expr_copy->Release();
	disjunct_expr->Release();

	if (0 < pred_stats_disj_child->Size())
	{
		return GPOS_NEW(mp) CStatsPredDisj(pred_stats_disj_child);
	}

	pred_stats_disj_child->Release();

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::AddSupportedStatsFilters
//
//	@doc:
//		Add supported filter for statistics computation
//---------------------------------------------------------------------------
void
CStatsPredUtils::AddSupportedStatsFilters
	(
	IMemoryPool *mp,
	CStatsPredPtrArry *pred_stats_array,
	CExpression *predicate_expr,
	CColRefSet *outer_refs
	)
{
	GPOS_ASSERT(NULL != predicate_expr);
	GPOS_ASSERT(NULL != pred_stats_array);

	CColRefSet *col_refset_used = CDrvdPropScalar::GetDrvdScalarProps(predicate_expr->PdpDerive())->PcrsUsed();
	if (NULL != outer_refs && outer_refs->ContainsAll(col_refset_used))
	{
		// skip predicates with outer references
		return;
	}

	if (COperator::EopScalarConst == predicate_expr->Pop()->Eopid())
	{
        pred_stats_array->Append(GPOS_NEW(mp) CStatsPredUnsupported(
            gpos::ulong_max, CStatsPred::EstatscmptOther,
            CHistogram::NeutralScaleFactor));

		return;
	}

	if (COperator::EopScalarArrayCmp == predicate_expr->Pop()->Eopid())
	{
		ProcessArrayCmp(mp, predicate_expr, pred_stats_array);
	}
	else
	{
		CStatsPredUtils::EPredicateType ept = GetPredTypeForExpr(mp, predicate_expr);
		GPOS_ASSERT(CStatsPredUtils::EptSentinel != ept);

		CStatsPred *pred_stats;
		switch (ept) {
			case CStatsPredUtils::EptDisj:
				pred_stats = CStatsPredUtils::CreateStatsPredDisj(mp, predicate_expr, outer_refs);
				break;
			case CStatsPredUtils::EptScIdent:
				pred_stats = CStatsPredUtils::GetStatsPredFromBoolExpr(mp, predicate_expr, outer_refs);
				break;
			case CStatsPredUtils::EptLike:
				pred_stats = CStatsPredUtils::GetStatsPredLike(mp, predicate_expr, outer_refs);
				break;
			case CStatsPredUtils::EptPoint:
				pred_stats = CStatsPredUtils::GetStatsPredPoint(mp, predicate_expr, outer_refs);
				break;
			case CStatsPredUtils::EptConj:
				pred_stats = CStatsPredUtils::CreateStatsPredConj(mp, predicate_expr, outer_refs);
				break;
			case CStatsPredUtils::EptNullTest:
				pred_stats = CStatsPredUtils::GetStatsPredNullTest(mp, predicate_expr, outer_refs);
				break;
			default:
				pred_stats = CStatsPredUtils::CreateStatsPredUnsupported(mp, predicate_expr, outer_refs);
				break;
		}

		if (NULL != pred_stats)
		{
			pred_stats_array->Append(pred_stats);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::GetPredTypeForExpr
//
//	@doc:
//		Return statistics filter type of the given expression
//---------------------------------------------------------------------------
CStatsPredUtils::EPredicateType
CStatsPredUtils::GetPredTypeForExpr
	(
	IMemoryPool *mp,
	CExpression *predicate_expr
	)
{
	GPOS_ASSERT(NULL != predicate_expr);
	if (CPredicateUtils::FOr(predicate_expr))
	{
		return CStatsPredUtils::EptDisj;
	}

	if (IsPredBooleanScIdent(predicate_expr))
	{
		return CStatsPredUtils::EptScIdent;
	}

	if (CPredicateUtils::FLikePredicate(predicate_expr))
	{
		return CStatsPredUtils::EptLike;
	}

	if (IsPointPredicate(predicate_expr))
	{
		return CStatsPredUtils::EptPoint;
	}

	if (IsPredPointIDF(predicate_expr))
	{
		return CStatsPredUtils::EptPoint;
	}

	if (IsPredPointINDF(predicate_expr))
	{
		return CStatsPredUtils::EptPoint;
	}

	if (IsConjunction(mp, predicate_expr))
	{
		return CStatsPredUtils::EptConj;
	}

	if (IsPredScalarIdentIsNull(predicate_expr) || IsPredScalarIdentIsNotNull(predicate_expr))
	{
		return CStatsPredUtils::EptNullTest;
	}

	return CStatsPredUtils::EptUnsupported;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::IsConjunction
//
//	@doc:
//		Is the condition a conjunctive predicate
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::IsConjunction
	(
	IMemoryPool *mp,
	CExpression *predicate_expr
	)
{
	GPOS_ASSERT(NULL != predicate_expr);
	CExpressionArray *expr_conjuncts = CPredicateUtils::PdrgpexprConjuncts(mp, predicate_expr);
	const ULONG size = expr_conjuncts->Size();
	expr_conjuncts->Release();

	return (1 < size);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::IsPredBooleanScIdent
//
//	@doc:
//		Is the condition a boolean predicate
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::IsPredBooleanScIdent
	(
	CExpression *predicate_expr
	)
{
	GPOS_ASSERT(NULL != predicate_expr);
	return CPredicateUtils::FBooleanScalarIdent(predicate_expr) || CPredicateUtils::FNegatedBooleanScalarIdent(predicate_expr);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::IsPointPredicate
//
//	@doc:
//		Is the condition a point predicate
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::IsPointPredicate
	(
	CExpression *predicate_expr
	)
{
	GPOS_ASSERT(NULL != predicate_expr);
	return (CPredicateUtils::FIdentCompareConstIgnoreCast(predicate_expr, COperator::EopScalarCmp));
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::IsPredPointIDF
//
//	@doc:
//		Is the condition an IDF point predicate
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::IsPredPointIDF
	(
	CExpression *predicate_expr
	)
{
	GPOS_ASSERT(NULL != predicate_expr);
	return CPredicateUtils::FIdentIDFConstIgnoreCast(predicate_expr);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::IsPredPointINDF
//
//	@doc:
//		Is the condition an INDF point predicate
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::IsPredPointINDF
	(
	CExpression *predicate_expr
	)
{
	GPOS_ASSERT(NULL != predicate_expr);

	if (!CPredicateUtils::FNot(predicate_expr))
	{
		return false;
	}

	return IsPredPointIDF((*predicate_expr)[0]);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::IsPredScalarIdentIsNull
//
//	@doc:
//		Is the condition a 'is null' test on top a scalar ident
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::IsPredScalarIdentIsNull
	(
	CExpression *predicate_expr
	)
{
	GPOS_ASSERT(NULL != predicate_expr);

	if (0 == predicate_expr->Arity())
	{
		return false;
	}
	// currently we support null test on scalar ident only
	return CUtils::FScalarNullTest(predicate_expr) && CUtils::FScalarIdent((*predicate_expr)[0]);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::FScalarIdentNullTest
//
//	@doc:
//		Is the condition a not-null test
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::IsPredScalarIdentIsNotNull
	(
	CExpression *predicate_expr
	)
{
	GPOS_ASSERT(NULL != predicate_expr);

	if (0 == predicate_expr->Arity())
	{
		return false;
	}
	CExpression *expr_null_test = (*predicate_expr)[0];

	// currently we support not-null test on scalar ident only
	return CUtils::FScalarBoolOp(predicate_expr, CScalarBoolOp::EboolopNot) && IsPredScalarIdentIsNull(expr_null_test);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::PstatspredLikeHandleCasting
//
//	@doc:
//		Create a LIKE statistics filter
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::GetStatsPredLike
	(
	IMemoryPool *mp,
	CExpression *predicate_expr,
	CColRefSet *//outer_refs,
	)
{
	GPOS_ASSERT(NULL != predicate_expr);
	GPOS_ASSERT(CPredicateUtils::FLikePredicate(predicate_expr));

	CExpression *expr_left = (*predicate_expr)[0];
	CExpression *expr_right = (*predicate_expr)[1];

	// we support LIKE predicate of the following patterns
	// CAST(ScIdent) LIKE Const
	// CAST(ScIdent) LIKE CAST(Const)
	// ScIdent LIKE Const
	// ScIdent LIKE CAST(Const)
	// CAST(Const) LIKE ScIdent
	// CAST(Const) LIKE CAST(ScIdent)
	// const LIKE ScIdent
	// const LIKE CAST(ScIdent)

	CExpression *expr_scalar_ident = NULL;
	CExpression *expr_scalar_const = NULL;

	CPredicateUtils::ExtractLikePredComponents(predicate_expr, &expr_scalar_ident, &expr_scalar_const);

	if (NULL == expr_scalar_ident || NULL == expr_scalar_const)
	{
		return GPOS_NEW(mp)
			CStatsPredUnsupported(gpos::ulong_max, CStatsPred::EstatscmptLike);
	}

	CScalarIdent *scalar_ident_op = CScalarIdent::PopConvert(expr_scalar_ident->Pop());
	ULONG colid = scalar_ident_op->Pcr()->Id();

	CScalarConst *scalar_const_op = CScalarConst::PopConvert(expr_scalar_const->Pop());
	IDatum  *datum_literal = scalar_const_op->GetDatum();

	const CColRef *col_ref = scalar_ident_op->Pcr();
	if (!IMDType::StatsAreComparable(col_ref->RetrieveType(), datum_literal))
	{
		// unsupported stats comparison between the column and datum
		return GPOS_NEW(mp) CStatsPredUnsupported(col_ref->Id(), CStatsPred::EstatscmptLike);
	}

	CDouble default_scale_factor(1.0);
	if (datum_literal->SupportsLikePredicate())
	{
		default_scale_factor = datum_literal->GetLikePredicateScaleFactor();
	}

	expr_left->AddRef();
	expr_right->AddRef();

	return GPOS_NEW(mp) CStatsPredLike(colid, expr_left, expr_right, default_scale_factor);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::ProcessArrayCmp
//
//	@doc:
//		Extract statistics filtering information from scalar array comparison
//---------------------------------------------------------------------------
void
CStatsPredUtils::ProcessArrayCmp
	(
	IMemoryPool *mp,
	CExpression *predicate_expr,
	CStatsPredPtrArry *pred_stats_array
	)
{
	GPOS_ASSERT(NULL != pred_stats_array);
	GPOS_ASSERT(NULL != predicate_expr);
	GPOS_ASSERT(2 == predicate_expr->Arity());
	CExpression *expr_ident = NULL;
	CScalarArrayCmp *scalar_array_cmp_op = CScalarArrayCmp::PopConvert(predicate_expr->Pop());
	if (CUtils::FScalarIdent((*predicate_expr)[0]))
	{
		expr_ident = (*predicate_expr)[0];
	}
	else if (CCastUtils::FBinaryCoercibleCast((*predicate_expr)[0]))
	{
		expr_ident = (*(*predicate_expr)[0])[0];
	}
	CExpression *expr_scalar_array = CUtils::PexprScalarArrayChild(predicate_expr);

	BOOL is_cmp_to_const_and_scalar_idents = CPredicateUtils::FCompareCastIdentToConstArray(predicate_expr) ||
										  CPredicateUtils::FCompareScalarIdentToConstAndScalarIdentArray(predicate_expr);

	if (!is_cmp_to_const_and_scalar_idents)
	{
		// unsupported predicate for stats calculations
		pred_stats_array->Append(GPOS_NEW(mp) CStatsPredUnsupported(
			gpos::ulong_max, CStatsPred::EstatscmptOther));

		return;
	}

	CStatsPredPtrArry *pred_stats_child_array = pred_stats_array;

	const ULONG constants = CUtils::UlScalarArrayArity(expr_scalar_array);
	// comparison semantics for statistics purposes is looser than regular comparison.
	CStatsPred::EStatsCmpType stats_cmp_type = GetStatsCmpType(scalar_array_cmp_op->MdIdOp());

	CScalarIdent *scalar_ident_op = CScalarIdent::PopConvert(expr_ident->Pop());
	const CColRef *col_ref = scalar_ident_op->Pcr();

	if (!CHistogram::SupportsFilter(stats_cmp_type))
	{
		// unsupported predicate for stats calculations
		pred_stats_array->Append(GPOS_NEW(mp) CStatsPredUnsupported(col_ref->Id(), stats_cmp_type));

		return;
	}

	BOOL is_array_cmp_any = (CScalarArrayCmp::EarrcmpAny == scalar_array_cmp_op->Earrcmpt());
	if (is_array_cmp_any)
	{
		pred_stats_child_array = GPOS_NEW(mp) CStatsPredPtrArry(mp);
	}

	for (ULONG ul = 0; ul < constants; ul++)
	{
		CExpression *expr_const = CUtils::PScalarArrayExprChildAt(mp, expr_scalar_array, ul);
		if (COperator::EopScalarConst == expr_const->Pop()->Eopid())
		{
			CScalarConst *scalar_const_op = CScalarConst::PopConvert(expr_const->Pop());
			IDatum *datum_literal = scalar_const_op->GetDatum();
			CStatsPred *child_pred_stats = NULL;
			if (!datum_literal->StatsAreComparable(datum_literal))
			{
				// stats calculations on such datums unsupported
				child_pred_stats = GPOS_NEW(mp) CStatsPredUnsupported(col_ref->Id(), stats_cmp_type);
			}
			else
			{
				child_pred_stats = GPOS_NEW(mp) CStatsPredPoint(mp, col_ref, stats_cmp_type, datum_literal);
			}

			pred_stats_child_array->Append(child_pred_stats);
		}
		expr_const->Release();
	}

	if (is_array_cmp_any)
	{
		CStatsPredDisj *pstatspredOr = GPOS_NEW(mp) CStatsPredDisj(pred_stats_child_array);
		pred_stats_array->Append(pstatspredOr);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::GetStatsPredFromBoolExpr
//
//	@doc:
//		Extract statistics filtering information from boolean predicate
//		in the form of scalar id or negated scalar id
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::GetStatsPredFromBoolExpr
	(
	IMemoryPool *mp,
	CExpression *predicate_expr,
	CColRefSet * //outer_refs
	)
{
	GPOS_ASSERT(NULL != predicate_expr);
	GPOS_ASSERT(CPredicateUtils::FBooleanScalarIdent(predicate_expr) || CPredicateUtils::FNegatedBooleanScalarIdent(predicate_expr));

	COperator *predicate_op = predicate_expr->Pop();

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	IDatum *datum = NULL;
	ULONG colid = gpos::ulong_max;

	if (CPredicateUtils::FBooleanScalarIdent(predicate_expr))
	{
		CScalarIdent *scalar_ident_op = CScalarIdent::PopConvert(predicate_op);
		datum = md_accessor->PtMDType<IMDTypeBool>()->CreateBoolDatum(mp, true /* fValue */, false /* is_null */);
		colid = scalar_ident_op->Pcr()->Id();
	}
	else
	{
		CExpression *child_expr = (*predicate_expr)[0];
		datum = md_accessor->PtMDType<IMDTypeBool>()->CreateBoolDatum(mp, false /* fValue */, false /* is_null */);
		colid = CScalarIdent::PopConvert(child_expr->Pop())->Pcr()->Id();
	}

	if (!datum->StatsAreComparable(datum))
	{
		// stats calculations on such datums unsupported
		datum->Release();

		return GPOS_NEW(mp) CStatsPredUnsupported(colid, CStatsPred::EstatscmptEq);
	}


	GPOS_ASSERT(NULL != datum && gpos::ulong_max != colid);

	return GPOS_NEW(mp) CStatsPredPoint(colid, CStatsPred::EstatscmptEq, GPOS_NEW(mp) CPoint(datum));
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::ExtractJoinStatsFromJoinPred
//
//	@doc:
//		Helper function to extract the statistics join filter from a given join predicate
//
//---------------------------------------------------------------------------
CStatsPredJoin *
CStatsPredUtils::ExtractJoinStatsFromJoinPred
	(
	IMemoryPool *mp,
	CExpression *join_pred_expr,
	CColRefSetArray *output_col_refsets,  // array of output columns of join's relational inputs
	CColRefSet *outer_refs,
	CExpressionArray *unsupported_expr_array
	)
{
	GPOS_ASSERT(NULL != join_pred_expr);
	GPOS_ASSERT(NULL != output_col_refsets);
	GPOS_ASSERT(NULL != unsupported_expr_array);

	CColRefSet *col_refset_used = CDrvdPropScalar::GetDrvdScalarProps(join_pred_expr->PdpDerive())->PcrsUsed();

	if (outer_refs->ContainsAll(col_refset_used))
	{
		return NULL;
	}

	const CColRef *col_ref_left = NULL;
	const CColRef *col_ref_right = NULL;
	CStatsPred::EStatsCmpType stats_cmp_type = CStatsPred::EstatscmptOther;

	BOOL fSupportedScIdentComparison = IsPredCmpColsOrIgnoreCast(join_pred_expr, &col_ref_left, &stats_cmp_type, &col_ref_right);
	if (fSupportedScIdentComparison && CStatsPred::EstatscmptOther != stats_cmp_type)
	{
		if (!IMDType::StatsAreComparable(col_ref_left->RetrieveType(), col_ref_right->RetrieveType()))
		{
			// unsupported statistics comparison between the histogram boundaries of the columns
			join_pred_expr->AddRef();
			unsupported_expr_array->Append(join_pred_expr);
			return NULL;
		}

		ULONG index_left = CUtils::UlPcrIndexContainingSet(output_col_refsets, col_ref_left);
		ULONG index_right = CUtils::UlPcrIndexContainingSet(output_col_refsets, col_ref_right);

		if (gpos::ulong_max != index_left && gpos::ulong_max != index_right &&
			index_left != index_right)
		{
			if (index_left < index_right)
			{
				return GPOS_NEW(mp) CStatsPredJoin(col_ref_left->Id(), stats_cmp_type, col_ref_right->Id());
			}

			return GPOS_NEW(mp) CStatsPredJoin(col_ref_right->Id(), stats_cmp_type, col_ref_left->Id());
		}
	}

	if (CColRefSet::FCovered(output_col_refsets, col_refset_used))
	{
		// unsupported join predicate
		join_pred_expr->AddRef();
		unsupported_expr_array->Append(join_pred_expr);
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::ExtractJoinStatsFromJoinPredArray
//
//	@doc:
//		Helper function to extract array of statistics join filter
//		from an array of join predicates
//
//---------------------------------------------------------------------------
CStatsPredJoinArray *
CStatsPredUtils::ExtractJoinStatsFromJoinPredArray
	(
	IMemoryPool *mp,
	CExpression *scalar_expr,
	CColRefSetArray *output_col_refsets,  // array of output columns of join's relational inputs
	CColRefSet *outer_refs,
	CStatsPred **unsupported_stats_pred_array
	)
{
	GPOS_ASSERT(NULL != scalar_expr);
	GPOS_ASSERT(NULL != output_col_refsets);

	CStatsPredJoinArray *join_preds_stats = GPOS_NEW(mp) CStatsPredJoinArray(mp);

	CExpressionArray *unsupported_expr_array = GPOS_NEW(mp) CExpressionArray(mp);

	// extract all the conjuncts
	CExpressionArray *expr_conjuncts = CPredicateUtils::PdrgpexprConjuncts(mp, scalar_expr);
	const ULONG size = expr_conjuncts->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CExpression *predicate_expr = (*expr_conjuncts) [ul];
		CStatsPredJoin *join_stats = ExtractJoinStatsFromJoinPred
										(
										mp,
										predicate_expr,
										output_col_refsets,
										outer_refs,
										unsupported_expr_array
										);
		if (NULL != join_stats)
		{
			join_preds_stats->Append(join_stats);
		}
	}

	const ULONG unsupported_pred_count = unsupported_expr_array->Size();
	if (1 == unsupported_pred_count)
	{
		*unsupported_stats_pred_array = CStatsPredUtils::ExtractPredStats(mp, (*unsupported_expr_array)[0], outer_refs);
	}
	else if (1 < unsupported_pred_count)
	{
		unsupported_expr_array->AddRef();
		CExpression *expr_conjunct = CPredicateUtils::PexprConjDisj(mp, unsupported_expr_array, true /* fConjunction */);
		*unsupported_stats_pred_array = CStatsPredUtils::ExtractPredStats(mp, expr_conjunct, outer_refs);
		expr_conjunct->Release();
	}

	// clean up
	unsupported_expr_array->Release();
	expr_conjuncts->Release();

	return join_preds_stats;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::ExtractJoinStatsFromExpr
//
//	@doc:
//		Helper function to extract array of statistics join filter from
//		an expression
//---------------------------------------------------------------------------
CStatsPredJoinArray *
CStatsPredUtils::ExtractJoinStatsFromExpr
	(
	IMemoryPool *mp,
	CExpressionHandle &expr_handle,
	CExpression *pexprScalarInput,
	CColRefSetArray *output_col_refsets, // array of output columns of join's relational inputs
	CColRefSet *outer_refs
	)
{
	GPOS_ASSERT(NULL != output_col_refsets);

	// remove implied predicates from join condition to avoid cardinality under-estimation
	CExpression *scalar_expr = CPredicateUtils::PexprRemoveImpliedConjuncts(mp, pexprScalarInput, expr_handle);

	// extract all the conjuncts
	CStatsPred *unsupported_pred_stats = NULL;
	CStatsPredJoinArray *join_pred_stats = ExtractJoinStatsFromJoinPredArray
										(
										mp,
										scalar_expr,
										output_col_refsets,
										outer_refs,
										&unsupported_pred_stats
										);

	// TODO:  May 15 2014, handle unsupported predicates for LASJ, LOJ and LS joins
	// clean up
	CRefCount::SafeRelease(unsupported_pred_stats);
	scalar_expr->Release();

	return join_pred_stats;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::ExtractJoinStatsFromExprHandle
//
//	@doc:
//		Extract statistics join information
//
//---------------------------------------------------------------------------
CStatsPredJoinArray *
CStatsPredUtils::ExtractJoinStatsFromExprHandle
	(
	IMemoryPool *mp,
	CExpressionHandle &expr_handle
	)
{
	// in case of subquery in join predicate, we return empty stats
	if (expr_handle.GetDrvdScalarProps(expr_handle.Arity() - 1)->FHasSubquery())
	{
		return GPOS_NEW(mp) CStatsPredJoinArray(mp);
	}

	CColRefSetArray *output_col_refsets = GPOS_NEW(mp) CColRefSetArray(mp);
	const ULONG size = expr_handle.Arity();
	for (ULONG ul = 0; ul < size - 1; ul++)
	{
		CColRefSet *output_col_ref_set = expr_handle.GetRelationalProperties(ul)->PcrsOutput();
		output_col_ref_set->AddRef();
		output_col_refsets->Append(output_col_ref_set);
	}

	// TODO:  02/29/2012 replace with constraint property info once available
	CExpression *scalar_expr = expr_handle.PexprScalarChild(expr_handle.Arity() - 1);
	CColRefSet *outer_refs = expr_handle.GetRelationalProperties()->PcrsOuter();

	CStatsPredJoinArray *join_pred_stats = ExtractJoinStatsFromExpr(mp, expr_handle, scalar_expr, output_col_refsets, outer_refs);

	// clean up
	output_col_refsets->Release();

	return join_pred_stats;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::IsConjOrDisjPred
//
//	@doc:
//		Is the predicate a conjunctive or disjunctive predicate
//
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::IsConjOrDisjPred
	(
	CStatsPred *pred_stats
	)
{
	return ((CStatsPred::EsptConj == pred_stats->GetPredStatsType()) || (CStatsPred::EsptDisj == pred_stats->GetPredStatsType()));
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::IsUnsupportedPredOnDefinedCol
//
//	@doc:
//		Is unsupported predicate on defined column
//
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::IsUnsupportedPredOnDefinedCol
	(
	CStatsPred *pred_stats
	)
{
	return ((CStatsPred::EsptUnsupported == pred_stats->GetPredStatsType()) &&
			(gpos::ulong_max == pred_stats->GetColId()));
}


// EOF
