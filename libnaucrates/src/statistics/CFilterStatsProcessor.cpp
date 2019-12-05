//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2018 Pivotal, Inc.
//
//	@filename:
//		CFilterStatsProcessor.cpp
//
//	@doc:
//		Statistics helper routines for processing limit operations
//---------------------------------------------------------------------------

#include "gpopt/operators/ops.h"
#include "gpopt/optimizer/COptimizerConfig.h"

#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CFilterStatsProcessor.h"
#include "naucrates/statistics/CJoinStatsProcessor.h"
#include "naucrates/statistics/CStatisticsUtils.h"
#include "naucrates/statistics/CScaleFactorUtils.h"

using namespace gpopt;

// derive statistics for filter operation based on given scalar expression
IStatistics *
CFilterStatsProcessor::MakeStatsFilterForScalarExpr
	(
	CMemoryPool *mp,
	CExpressionHandle &exprhdl,
	IStatistics *child_stats,
	CExpression *local_scalar_expr, // filter expression on local columns only
	CExpression *outer_refs_scalar_expr, // filter expression involving outer references
	IStatisticsArray *all_outer_stats
	)
{
	GPOS_ASSERT(NULL != child_stats);
	GPOS_ASSERT(NULL != local_scalar_expr);
	GPOS_ASSERT(NULL != outer_refs_scalar_expr);
	GPOS_ASSERT(NULL != all_outer_stats);

	CColRefSet *outer_refs = exprhdl.DeriveOuterReferences();

	// TODO  June 13 2014, we currently only cap ndvs when we have a filter
	// immediately on top of tables
	BOOL do_cap_NDVs = (1 == exprhdl.DeriveJoinDepth());

	// extract local filter
	CStatsPred *pred_stats = CStatsPredUtils::ExtractPredStats(mp, local_scalar_expr, outer_refs);

	// derive stats based on local filter
	IStatistics *result_stats = CFilterStatsProcessor::MakeStatsFilter(mp, dynamic_cast<CStatistics *>(child_stats), pred_stats, do_cap_NDVs);
	pred_stats->Release();

	if (exprhdl.HasOuterRefs() && 0 < all_outer_stats->Size())
	{
		// derive stats based on outer references
		IStatistics *stats = CJoinStatsProcessor::DeriveStatsWithOuterRefs
													(
													mp,
													exprhdl,
													outer_refs_scalar_expr,
													result_stats,
													all_outer_stats,
													IStatistics::EsjtInnerJoin
													);
		result_stats->Release();
		result_stats = stats;
	}

	return result_stats;
}

// compute the selectivity of a predicate, applied to a base table,
// making some simple default assumptions about outer refs
CDouble
CFilterStatsProcessor::SelectivityOfPredicate
	(
	 CMemoryPool *mp,
	 CExpression *pred,
	 CTableDescriptor *ptabdesc,
	 CColRefSet *outer_refs
	)
{
	// separate the outer refs
	CExpression *local_expr = NULL;
	CExpression *expr_with_outer_refs = NULL;

	CColRefSet *used_col_refs = pred->DeriveUsedColumns();
	CColRefSet *used_local_col_refs = GPOS_NEW(mp) CColRefSet(mp, *used_col_refs);
	ULONG num_outer_ref_preds = 0;

	if (NULL != outer_refs)
	{
		used_local_col_refs->Exclude(outer_refs);
		CPredicateUtils::SeparateOuterRefs(mp, pred, outer_refs, &local_expr, &expr_with_outer_refs);
	}
	else
	{
		pred->AddRef();
		local_expr = pred;
	}

	// extract local filter
	CStatsPred *pred_stats = CStatsPredUtils::ExtractPredStats(mp, local_expr, outer_refs);

	const COptCtxt *poctxt = COptCtxt::PoctxtFromTLS();
	CMDAccessor *md_accessor = poctxt->Pmda();
	// grab default stats config
	CStatisticsConfig *stats_config = poctxt->GetOptimizerConfig()->GetStatsConf();
	// we don't care about the width of the columns, just the row count
	CColRefSet *dummy_width_set = GPOS_NEW(mp) CColRefSet(mp);

	IStatistics *base_table_stats = md_accessor->Pstats
					(
					 mp,
					 ptabdesc->MDId(),
					 used_local_col_refs,
					 dummy_width_set,
					 stats_config
					 );

	// derive stats based on local filter
	IStatistics *result_stats = CFilterStatsProcessor::MakeStatsFilter(mp, dynamic_cast<CStatistics *>(base_table_stats), pred_stats, false);

	CDouble result = result_stats->Rows() / base_table_stats->Rows();
	BOOL have_local_preds = (result < 1.0);
	pred_stats->Release();
	used_local_col_refs->Release();
	base_table_stats->Release();
	dummy_width_set->Release();

	// handle outer_refs
	if (NULL != expr_with_outer_refs)
	{
		CExpressionArray *outer_ref_exprs = CPredicateUtils::PdrgpexprConjuncts(mp, expr_with_outer_refs);

		const ULONG size = outer_ref_exprs->Size();
		for (ULONG ul = 0; ul < size; ul++)
		{
			CExpression *pexpr = (*outer_ref_exprs)[ul];
			CColRef *local_col_ref = NULL;

			if (CPredicateUtils::FIdentCompareOuterRefExprIgnoreCast(pexpr, outer_refs, &local_col_ref))
			{
				CScalarCmp *sc_cmp = CScalarCmp::PopConvert(pexpr->Pop());
				// Use more accurate NDV calculation if the comparison is an equality type
				if (IMDType::EcmptEq == sc_cmp->ParseCmpType())
				{
					GPOS_ASSERT(NULL != local_col_ref);
					CDouble ndv = result_stats->GetNDVs(local_col_ref);

					if (ndv < 1.0)
					{
						// An NDV of less than 1 means that we have no stats on this column
						result = result * CHistogram::DefaultSelectivity;
					}
					else
					{
						result = result * (1/ndv);
					}
				}
				else
				{
					// a comparison col op <outer ref> other than an equals
					result = result * CHistogram::DefaultSelectivity;
				}
				num_outer_ref_preds++;
			}
			else
			{
				// if it is a true filter, then we had no expressions with outer refs
				if (!CUtils::FScalarConstTrue(pexpr))
				{
					// some other expression, not of the form col op <outer ref>,
					// e.g. an OR expression
					result = result * CHistogram::DefaultSelectivity;
					num_outer_ref_preds++;
				}
			}
		}

		expr_with_outer_refs->Release();
		outer_ref_exprs->Release();
	}

	// apply damping factor to the outer ref predicates whose selectivities we multiplied above
	if (have_local_preds)
	{
		// add one for the combined non-outer refs which were dampened internally,
		// but not in combination with the preds on outer refs
		num_outer_ref_preds++;
	}
	if (1 < num_outer_ref_preds)
	{
		CStatisticsConfig *stats_config = CStatisticsConfig::PstatsconfDefault(mp);

		result = std::min(result.Get() / CScaleFactorUtils::DampedFilterScaleFactor(stats_config, num_outer_ref_preds).Get(),
						  1.0);

		stats_config->Release();
	}
	result_stats->Release();
	local_expr->Release();

	return result;
}

// create new structure from a list of statistics filters
CStatistics *
CFilterStatsProcessor::MakeStatsFilter
	(
	CMemoryPool *mp,
	const CStatistics *input_stats,
	CStatsPred *base_pred_stats,
	BOOL do_cap_NDVs
	)
{
	GPOS_ASSERT(NULL != base_pred_stats);

	CDouble input_rows = std::max(CStatistics::MinRows.Get(), input_stats->Rows().Get());
	CDouble scale_factor(1.0);
	ULONG num_predicates = 1;
	CDouble rows_filter = CStatistics::MinRows;
	UlongToHistogramMap *histograms_new = NULL;

	UlongToHistogramMap *histograms_copy = input_stats->CopyHistograms(mp);

	CStatisticsConfig *stats_config = input_stats->GetStatsConfig();
	if (input_stats->IsEmpty())
	{
		histograms_new = GPOS_NEW(mp) UlongToHistogramMap(mp);
		CHistogram::AddEmptyHistogram(mp, histograms_new, histograms_copy);
	}
	else
	{
		if (CStatsPred::EsptDisj == base_pred_stats->GetPredStatsType())
		{
			CStatsPredDisj *pred_stats = CStatsPredDisj::ConvertPredStats(base_pred_stats);

			histograms_new  = MakeHistHashMapDisjFilter
								(
								mp,
								stats_config,
								histograms_copy,
								input_rows,
								pred_stats,
								&scale_factor
								);
		}
		else
		{
			GPOS_ASSERT(CStatsPred::EsptConj == base_pred_stats->GetPredStatsType());
			CStatsPredConj *pred_stats = CStatsPredConj::ConvertPredStats(base_pred_stats);
			num_predicates = pred_stats->GetNumPreds();
			histograms_new = MakeHistHashMapConjFilter
							(
							mp,
							stats_config,
							histograms_copy,
							input_rows,
							pred_stats,
							&scale_factor
							);
		}
		GPOS_ASSERT(CStatistics::MinRows.Get() <= scale_factor.Get());
		rows_filter = input_rows / scale_factor;
		rows_filter = std::max(CStatistics::MinRows.Get(), rows_filter.Get());
	}

	histograms_copy->Release();

	GPOS_ASSERT(rows_filter.Get() <= input_rows.Get());

	if (do_cap_NDVs)
	{
		CStatistics::CapNDVs(rows_filter, histograms_new);
	}

	CStatistics *filter_stats = GPOS_NEW(mp) CStatistics
												(
												mp,
												histograms_new,
												input_stats->CopyWidths(mp),
												rows_filter,
												input_stats->IsEmpty(),
												input_stats->GetNumberOfPredicates() + num_predicates
												);

	// since the filter operation is reductive, we choose the bounding method that takes
	// the minimum of the cardinality upper bound of the source column (in the input hash map)
	// and estimated output cardinality
	CStatisticsUtils::ComputeCardUpperBounds(mp, input_stats, filter_stats, rows_filter, CStatistics::EcbmMin /* card_bounding_method */);

	return filter_stats;
}

// create a new hash map of histograms after applying a conjunctive
// or a disjunctive filter
UlongToHistogramMap *
CFilterStatsProcessor::MakeHistHashMapConjOrDisjFilter
	(
	CMemoryPool *mp,
	const CStatisticsConfig *stats_config,
	UlongToHistogramMap *input_histograms,
	CDouble input_rows,
	CStatsPred *pred_stats,
	CDouble *scale_factor
	)
{
	GPOS_ASSERT(NULL != pred_stats);
	GPOS_ASSERT(NULL != stats_config);
	GPOS_ASSERT(NULL != input_histograms);

	UlongToHistogramMap *result_histograms = NULL;

	if (CStatsPred::EsptConj == pred_stats->GetPredStatsType())
	{
		CStatsPredConj *conjunctive_pred_stats = CStatsPredConj::ConvertPredStats(pred_stats);
		return MakeHistHashMapConjFilter
				(
				mp,
				stats_config,
				input_histograms,
				input_rows,
				conjunctive_pred_stats,
				scale_factor
				);
	}

	CStatsPredDisj *disjunctive_pred_stats = CStatsPredDisj::ConvertPredStats(pred_stats);
	result_histograms  = MakeHistHashMapDisjFilter
						(
						mp,
						stats_config,
						input_histograms,
						input_rows,
						disjunctive_pred_stats,
						scale_factor
						);

	GPOS_ASSERT(NULL != result_histograms);

	return result_histograms;
}

// create new hash map of histograms after applying conjunctive predicates
UlongToHistogramMap *
CFilterStatsProcessor::MakeHistHashMapConjFilter
	(
	CMemoryPool *mp,
	const CStatisticsConfig *stats_config,
												 UlongToHistogramMap *input_histograms,
	CDouble input_rows,
	CStatsPredConj *conjunctive_pred_stats,
	CDouble *scale_factor
	)
{
	GPOS_ASSERT(NULL != stats_config);
	GPOS_ASSERT(NULL != input_histograms);
	GPOS_ASSERT(NULL != conjunctive_pred_stats);

	conjunctive_pred_stats->Sort();

	CBitSet *filter_colids = GPOS_NEW(mp) CBitSet(mp);
	CDoubleArray *scale_factors = GPOS_NEW(mp) CDoubleArray(mp);

	// create copy of the original hash map of colid -> histogram
	UlongToHistogramMap *result_histograms = CStatisticsUtils::CopyHistHashMap(mp, input_histograms);

	// properties of last seen column
	CDouble last_scale_factor(1.0);
	ULONG last_colid = gpos::ulong_max;

	// iterate over filters and update corresponding histograms
	const ULONG filters = conjunctive_pred_stats->GetNumPreds();
	for (ULONG ul = 0; ul < filters; ul++)
	{
		CStatsPred *child_pred_stats = conjunctive_pred_stats->GetPredStats(ul);

		GPOS_ASSERT(CStatsPred::EsptConj != child_pred_stats->GetPredStatsType());

		// get the components of the statistics filter
		ULONG colid = child_pred_stats->GetColId();

		if (CStatsPredUtils::IsUnsupportedPredOnDefinedCol(child_pred_stats))
		{
			// for example, (expression OP const) where expression is a defined column like (a+b)
			CStatsPredUnsupported *unsupported_pred_stats = CStatsPredUnsupported::ConvertPredStats(child_pred_stats);
			scale_factors->Append(GPOS_NEW(mp) CDouble(unsupported_pred_stats->ScaleFactor()));

			continue;
		}

		// the histogram to apply filter on
		CHistogram *hist_before = NULL;
		if (IsNewStatsColumn(colid, last_colid))
		{
			scale_factors->Append( GPOS_NEW(mp) CDouble(last_scale_factor));
			last_scale_factor = CDouble(1.0);
		}

		if (CStatsPred::EsptDisj != child_pred_stats->GetPredStatsType())
		{
			GPOS_ASSERT(gpos::ulong_max != colid);
			hist_before = result_histograms->Find(&colid)->CopyHistogram();
			GPOS_ASSERT(NULL != hist_before);

			CHistogram *result_histogram = NULL;
			result_histogram = MakeHistSimpleFilter(child_pred_stats, filter_colids, hist_before, &last_scale_factor, &last_colid);
			GPOS_DELETE(hist_before);

			GPOS_ASSERT(NULL != result_histogram);

			CHistogram *input_histogram = input_histograms->Find(&colid);
			GPOS_ASSERT(NULL != input_histogram);
			if (input_histogram->IsEmpty())
			{
				// input histogram is empty so scaling factor does not make sense.
				// if the input itself is empty, then scaling factor is of no effect
				last_scale_factor = 1 / CHistogram::DefaultSelectivity;
			}

			CStatisticsUtils::AddHistogram(mp, colid, result_histogram, result_histograms, true /* fReplaceOld */);
			GPOS_DELETE(result_histogram);
		}
		else
		{
			CStatsPredDisj *disjunctive_pred_stats = CStatsPredDisj::ConvertPredStats(child_pred_stats);

			result_histograms->AddRef();
			UlongToHistogramMap *disjunctive_input_histograms = result_histograms;

			CDouble disjunctive_scale_factor(1.0);
			CDouble num_disj_input_rows(CStatistics::MinRows.Get());

			if (gpos::ulong_max != colid)
			{
				// The disjunction predicate uses a single column. The input rows to the disjunction
				// is obtained by scaling attained so far on that column
				num_disj_input_rows = std::max(CStatistics::MinRows.Get(), (input_rows / last_scale_factor).Get());
			}
			else
			{
				// the disjunction uses multiple columns therefore cannot reason about the number of input rows
				// to the disjunction
				num_disj_input_rows = input_rows.Get();
			}

			UlongToHistogramMap *disjunctive_histograms_after = MakeHistHashMapDisjFilter
											(
											mp,
											stats_config,
											result_histograms,
											num_disj_input_rows,
											disjunctive_pred_stats,
											&disjunctive_scale_factor
											);

			// replace intermediate result with the newly generated result from the disjunction
			if (gpos::ulong_max != colid)
			{
				CHistogram *result_histogram = disjunctive_histograms_after->Find(&colid);
				CStatisticsUtils::AddHistogram(mp, colid, result_histogram, result_histograms, true /* fReplaceOld */);
				disjunctive_histograms_after->Release();

				last_scale_factor = last_scale_factor * disjunctive_scale_factor;
			}
			else
			{
				last_scale_factor = disjunctive_scale_factor.Get();
				result_histograms->Release();
				result_histograms = disjunctive_histograms_after;
			}

			last_colid = colid;
			disjunctive_input_histograms->Release();
		}
	}

	// scaling factor of the last predicate
	scale_factors->Append(GPOS_NEW(mp) CDouble(last_scale_factor));

	GPOS_ASSERT(NULL != scale_factors);
	CScaleFactorUtils::SortScalingFactor(scale_factors, true /* fDescending */);

	*scale_factor = CScaleFactorUtils::CalcScaleFactorCumulativeConj(stats_config, scale_factors);

	// clean up
	scale_factors->Release();
	filter_colids->Release();

	return result_histograms;
}

// create new hash map of histograms after applying disjunctive predicates
UlongToHistogramMap *
CFilterStatsProcessor::MakeHistHashMapDisjFilter
	(
	CMemoryPool *mp,
	const CStatisticsConfig *stats_config,
	UlongToHistogramMap *input_histograms,
	CDouble input_rows,
	CStatsPredDisj *disjunctive_pred_stats,
	CDouble *scale_factor
	)
{
	GPOS_ASSERT(NULL != stats_config);
	GPOS_ASSERT(NULL != input_histograms);
	GPOS_ASSERT(NULL != disjunctive_pred_stats);

	CBitSet *non_updatable_cols = CStatisticsUtils::GetColsNonUpdatableHistForDisj(mp, disjunctive_pred_stats);

	disjunctive_pred_stats->Sort();

	CBitSet *filter_colids = GPOS_NEW(mp) CBitSet(mp);
	CDoubleArray *scale_factors = GPOS_NEW(mp) CDoubleArray(mp);

	UlongToHistogramMap *disjunctive_result_histograms = GPOS_NEW(mp) UlongToHistogramMap(mp);

	CHistogram *previous_histogram = NULL;
	ULONG previous_colid = gpos::ulong_max;
	CDouble previous_scale_factor(input_rows);

	CDouble cumulative_rows(CStatistics::MinRows.Get());

	// iterate over filters and update corresponding histograms
	const ULONG filters = disjunctive_pred_stats->GetNumPreds();
	for (ULONG ul = 0; ul < filters; ul++)
	{
		CStatsPred *child_pred_stats = disjunctive_pred_stats->GetPredStats(ul);

		// get the components of the statistics filter
		ULONG colid = child_pred_stats->GetColId();

		if (CStatsPredUtils::IsUnsupportedPredOnDefinedCol(child_pred_stats))
		{
			CStatsPredUnsupported *unsupported_pred_stats = CStatsPredUnsupported::ConvertPredStats(child_pred_stats);
			scale_factors->Append(GPOS_NEW(mp) CDouble(unsupported_pred_stats->ScaleFactor()));

			continue;
		}

		if (IsNewStatsColumn(colid, previous_colid))
		{
			scale_factors->Append(GPOS_NEW(mp) CDouble(previous_scale_factor.Get()));
			CStatisticsUtils::UpdateDisjStatistics
								(
								mp,
								non_updatable_cols,
								input_rows,
								cumulative_rows,
								previous_histogram,
								disjunctive_result_histograms,
								previous_colid
								);
			previous_histogram = NULL;
		}

		CHistogram *histogram = input_histograms->Find(&colid);
		CHistogram *disjunctive_child_col_histogram = NULL;

		BOOL is_pred_simple = !CStatsPredUtils::IsConjOrDisjPred(child_pred_stats);
		BOOL is_colid_present = (gpos::ulong_max != colid);
		UlongToHistogramMap *child_histograms = NULL;
		CDouble child_scale_factor(1.0);

		if (is_pred_simple)
		{
			GPOS_ASSERT(NULL != histogram);
			disjunctive_child_col_histogram = MakeHistSimpleFilter(child_pred_stats, filter_colids, histogram, &child_scale_factor, &previous_colid);

			CHistogram *input_histogram = input_histograms->Find(&colid);
			GPOS_ASSERT(NULL != input_histogram);
			if (input_histogram->IsEmpty())
			{
				// input histogram is empty so scaling factor does not make sense.
				// if the input itself is empty, then scaling factor is of no effect
				child_scale_factor = 1 / CHistogram::DefaultSelectivity;
			}
		}
		else
		{
			child_histograms = MakeHistHashMapConjOrDisjFilter
								(
								mp,
								stats_config,
								input_histograms,
								input_rows,
								child_pred_stats,
								&child_scale_factor
								);

			GPOS_ASSERT_IMP(CStatsPred::EsptDisj == child_pred_stats->GetPredStatsType(),
							gpos::ulong_max != colid);

			if (is_colid_present)
			{
				// conjunction or disjunction uses only a single column
				disjunctive_child_col_histogram = child_histograms->Find(&colid)->CopyHistogram();
			}
		}

		CDouble num_rows_disj_child = input_rows / child_scale_factor;
		if (is_colid_present)
		{
			// 1. a simple predicate (a == 5), (b LIKE "%%GOOD%%")
			// 2. conjunctive / disjunctive predicate where each of its component are predicates on the same column
			// e.g. (a <= 5 AND a >= 1), a in (5, 1)
			GPOS_ASSERT(NULL != disjunctive_child_col_histogram);

			if (NULL == previous_histogram)
			{
				previous_histogram = disjunctive_child_col_histogram;
				cumulative_rows = num_rows_disj_child;
			}
			else
			{
				// statistics operation already conducted on this column
				CDouble output_rows(0.0);
				CHistogram *new_histogram = previous_histogram->MakeUnionHistogramNormalize(cumulative_rows, disjunctive_child_col_histogram, num_rows_disj_child, &output_rows);
				cumulative_rows = output_rows;

				GPOS_DELETE(previous_histogram);
				GPOS_DELETE(disjunctive_child_col_histogram);
				previous_histogram = new_histogram;
			}

			previous_scale_factor = input_rows / std::max(CStatistics::MinRows.Get(), cumulative_rows.Get());
			previous_colid = colid;
		}
		else
		{
			// conjunctive predicate where each of it component are predicates on different columns
			// e.g. ((a <= 5) AND (b LIKE "%%GOOD%%"))
			GPOS_ASSERT(NULL != child_histograms);
			GPOS_ASSERT(NULL == disjunctive_child_col_histogram);

			CDouble current_rows_estimate = input_rows / CScaleFactorUtils::CalcScaleFactorCumulativeDisj(stats_config, scale_factors, input_rows);
			UlongToHistogramMap *merged_histograms = CStatisticsUtils::CreateHistHashMapAfterMergingDisjPreds
													  	  (
													  	  mp,
													  	  non_updatable_cols,
													  	  disjunctive_result_histograms,
													  	  child_histograms,
													  	  current_rows_estimate,
													  	  num_rows_disj_child
													  	  );
			disjunctive_result_histograms->Release();
			disjunctive_result_histograms = merged_histograms;

			previous_histogram = NULL;
			previous_scale_factor = child_scale_factor;
			previous_colid = colid;
		}

		CRefCount::SafeRelease(child_histograms);
	}

	// process the result and scaling factor of the last predicate
	CStatisticsUtils::UpdateDisjStatistics
						(
						mp,
						non_updatable_cols,
						input_rows,
						cumulative_rows,
						previous_histogram,
						disjunctive_result_histograms,
						previous_colid
						);
	previous_histogram = NULL;
	scale_factors->Append(GPOS_NEW(mp) CDouble(std::max(CStatistics::MinRows.Get(), previous_scale_factor.Get())));

	*scale_factor = CScaleFactorUtils::CalcScaleFactorCumulativeDisj(stats_config, scale_factors, input_rows);

	CHistogram::AddHistograms(mp, input_histograms, disjunctive_result_histograms);

	non_updatable_cols->Release();

	// clean up
	scale_factors->Release();
	filter_colids->Release();

	return disjunctive_result_histograms;
}

//	create a new histograms after applying the filter that is not
//	an AND/OR predicate
CHistogram *
CFilterStatsProcessor::MakeHistSimpleFilter
	(
	CStatsPred *pred_stats,
	CBitSet *filter_colids,
	CHistogram *hist_before,
	CDouble *last_scale_factor,
	ULONG *target_last_colid
	)
{
	if (CStatsPred::EsptPoint == pred_stats->GetPredStatsType())
	{
		CStatsPredPoint *point_pred_stats = CStatsPredPoint::ConvertPredStats(pred_stats);
		return MakeHistPointFilter(point_pred_stats, filter_colids, hist_before, last_scale_factor, target_last_colid);
	}

	if (CStatsPred::EsptLike == pred_stats->GetPredStatsType())
	{
		CStatsPredLike *like_pred_stats = CStatsPredLike::ConvertPredStats(pred_stats);

		return MakeHistLikeFilter(like_pred_stats, filter_colids, hist_before, last_scale_factor, target_last_colid);
	}

	CStatsPredUnsupported *unsupported_pred_stats = CStatsPredUnsupported::ConvertPredStats(pred_stats);

	return MakeHistUnsupportedPred(unsupported_pred_stats, filter_colids, hist_before, last_scale_factor, target_last_colid);
}

// create a new histograms after applying the point filter
CHistogram *
CFilterStatsProcessor::MakeHistPointFilter
	(
	CStatsPredPoint *pred_stats,
	CBitSet *filter_colids,
	CHistogram *hist_before,
	CDouble *last_scale_factor,
	ULONG *target_last_colid
	)
{
	GPOS_ASSERT(NULL != pred_stats);
	GPOS_ASSERT(NULL != filter_colids);
	GPOS_ASSERT(NULL != hist_before);

	const ULONG colid = pred_stats->GetColId();
	GPOS_ASSERT(CHistogram::IsOpSupportedForFilter(pred_stats->GetCmpType()));

	CPoint *point = pred_stats->GetPredPoint();

	// note column id
	(void) filter_colids->ExchangeSet(colid);

	CDouble local_scale_factor(1.0);
	CHistogram *result_histogram = hist_before->MakeHistogramFilterNormalize(pred_stats->GetCmpType(), point, &local_scale_factor);

	GPOS_ASSERT(DOUBLE(1.0) <= local_scale_factor.Get());

	*last_scale_factor = *last_scale_factor * local_scale_factor;
	*target_last_colid = colid;

	return result_histogram;
}


//	create a new histograms for an unsupported predicate
CHistogram *
CFilterStatsProcessor::MakeHistUnsupportedPred
	(
	CStatsPredUnsupported *pred_stats,
	CBitSet *filter_colids,
	CHistogram *hist_before,
	CDouble *last_scale_factor,
	ULONG *target_last_colid
	)
{
	GPOS_ASSERT(NULL != pred_stats);
	GPOS_ASSERT(NULL != filter_colids);
	GPOS_ASSERT(NULL != hist_before);

	const ULONG colid = pred_stats->GetColId();

	// note column id
	(void) filter_colids->ExchangeSet(colid);

	// generate after histogram
	CHistogram *result_histogram = hist_before->CopyHistogram();
	GPOS_ASSERT(NULL != result_histogram);

	*last_scale_factor = *last_scale_factor * pred_stats->ScaleFactor();
	*target_last_colid = colid;

	return result_histogram;
}

//	create a new histograms after applying the LIKE filter
CHistogram *
CFilterStatsProcessor::MakeHistLikeFilter
	(
	CStatsPredLike *pred_stats,
	CBitSet *filter_colids,
	CHistogram *hist_before,
	CDouble *last_scale_factor,
	ULONG *target_last_colid
	)
{
	GPOS_ASSERT(NULL != pred_stats);
	GPOS_ASSERT(NULL != filter_colids);
	GPOS_ASSERT(NULL != hist_before);

	const ULONG colid = pred_stats->GetColId();

	// note column id
	(void) filter_colids->ExchangeSet(colid);
	CHistogram *result_histogram = hist_before->CopyHistogram();

	*last_scale_factor = *last_scale_factor * pred_stats->DefaultScaleFactor();
	*target_last_colid = colid;

	return result_histogram;
}

// check if the column is a new column for statistic calculation
BOOL
CFilterStatsProcessor::IsNewStatsColumn
	(
	ULONG colid,
	ULONG last_colid
	)
{
	return (gpos::ulong_max == colid || colid != last_colid);
}

// EOF
