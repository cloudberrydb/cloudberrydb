//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2014 Pivotal Inc.
//
//	@filename:
//		CScaleFactorUtils.cpp
//
//	@doc:
//		Helper routines to compute scale factors / damping factors
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/exception.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CExpressionUtils.h"
#include "gpopt/operators/CPredicateUtils.h"

#include "naucrates/statistics/CScaleFactorUtils.h"
#include "naucrates/statistics/CStatistics.h"

using namespace gpopt;
using namespace gpmd;


// default scaling factor of a non-equality (<, >, <=, >=) join predicates
const CDouble CScaleFactorUtils::DefaultInequalityJoinPredScaleFactor(3.0);

// default scaling factor of join predicates
const CDouble CScaleFactorUtils::DefaultJoinPredScaleFactor(100.0);

// default scaling factor of LIKE predicate
const CDouble CScaleFactorUtils::DDefaultScaleFactorLike(150.0);

// invalid scale factor
const CDouble CScaleFactorUtils::InvalidScaleFactor(0.0);

//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::CumulativeJoinScaleFactor
//
//	@doc:
//		Calculate the cumulative join scaling factor
//
//---------------------------------------------------------------------------
CDouble
CScaleFactorUtils::CumulativeJoinScaleFactor
	(
	const CStatisticsConfig *stats_config,
	CDoubleArray *join_conds_scale_factors
	)
{
	GPOS_ASSERT(NULL != stats_config);
	GPOS_ASSERT(NULL != join_conds_scale_factors);

	const ULONG num_join_conds = join_conds_scale_factors->Size();
	if (1 < num_join_conds)
	{
		// sort (in desc order) the scaling factor of the join conditions
		join_conds_scale_factors->Sort(CScaleFactorUtils::DescendingOrderCmpFunc);
	}

	CDouble scale_factor(1.0);
	// iterate over joins
	for (ULONG ul = 0; ul < num_join_conds; ul++)
	{
		CDouble local_scale_factor = *(*join_conds_scale_factors)[ul];

		scale_factor = scale_factor * std::max
										(
										CStatistics::MinRows.Get(),
										(local_scale_factor * DampedJoinScaleFactor(stats_config, ul + 1)).Get()
										);
	}

	return scale_factor;
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::DampedJoinScaleFactor
//
//	@doc:
//		Return scaling factor of the join predicate after apply damping
//
//---------------------------------------------------------------------------
CDouble
CScaleFactorUtils::DampedJoinScaleFactor
	(
	const CStatisticsConfig *stats_config,
	ULONG num_columns
	)
{
	if (1 >= num_columns)
	{
		return CDouble(1.0);
	}

	return stats_config->DDampingFactorJoin().Pow(CDouble(num_columns));
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::DampedFilterScaleFactor
//
//	@doc:
//		Return scaling factor of the filter after apply damping
//
//---------------------------------------------------------------------------
CDouble
CScaleFactorUtils::DampedFilterScaleFactor
	(
	const CStatisticsConfig *stats_config,
	ULONG num_columns
	)
{
	GPOS_ASSERT(NULL != stats_config);

	if (1 >= num_columns)
	{
		return CDouble(1.0);
	}

	return stats_config->DDampingFactorFilter().Pow(CDouble(num_columns));
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::DampedGroupByScaleFactor
//
//	@doc:
//		Return scaling factor of the group by predicate after apply damping
//
//---------------------------------------------------------------------------
CDouble
CScaleFactorUtils::DampedGroupByScaleFactor
	(
	const CStatisticsConfig *stats_config,
	ULONG num_columns
	)
{
	GPOS_ASSERT(NULL != stats_config);

	if (1 > num_columns)
	{
		return CDouble(1.0);
	}

	return stats_config->DDampingFactorGroupBy().Pow(CDouble(num_columns + 1));
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::SortScalingFactor
//
//	@doc:
//		Sort the array of scaling factor
//
//---------------------------------------------------------------------------
void
CScaleFactorUtils::SortScalingFactor
	(
	CDoubleArray *scale_factors,
	BOOL is_descending
	)
{
	GPOS_ASSERT(NULL != scale_factors);
	const ULONG num_cols = scale_factors->Size();
	if (1 < num_cols)
	{
		if (is_descending)
		{
			// sort (in desc order) the scaling factor based on the selectivity of each column
			scale_factors->Sort(CScaleFactorUtils::DescendingOrderCmpFunc);
		}
		else
		{
			// sort (in ascending order) the scaling factor based on the selectivity of each column
			scale_factors->Sort(CScaleFactorUtils::AscendingOrderCmpFunc);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::DescendingOrderCmpFunc
//
//	@doc:
//		Comparison function for sorting double
//
//---------------------------------------------------------------------------
INT
CScaleFactorUtils::DescendingOrderCmpFunc
	(
	const void *val1,
	const void *val2
	)
{
	GPOS_ASSERT(NULL != val1 && NULL != val2);
	const CDouble *double_val1 = *(const CDouble **) val1;
	const CDouble *double_val2 = *(const CDouble **) val2;

	return DoubleCmpFunc(double_val1, double_val2, true /*is_descending*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::AscendingOrderCmpFunc
//
//	@doc:
//		Comparison function for sorting double
//
//---------------------------------------------------------------------------
INT
CScaleFactorUtils::AscendingOrderCmpFunc
	(
	const void *val1,
	const void *val2
	)
{
	GPOS_ASSERT(NULL != val1 && NULL != val2);
	const CDouble *double_val1 = *(const CDouble **) val1;
	const CDouble *double_val2 = *(const CDouble **) val2;

	return DoubleCmpFunc(double_val1, double_val2, false /*is_descending*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::DoubleCmpFunc
//
//	@doc:
//		Helper function for double comparison
//
//---------------------------------------------------------------------------
INT
CScaleFactorUtils::DoubleCmpFunc
	(
	const CDouble *double_val1,
	const CDouble *double_val2,
	BOOL is_descending
	)
{
	GPOS_ASSERT(NULL != double_val1);
	GPOS_ASSERT(NULL != double_val2);

	if (double_val1->Get() == double_val2->Get())
	{
		return 0;
	}

	if (double_val1->Get() < double_val2->Get() && is_descending)
	{
	    return 1;
	}

	if (double_val1->Get() > double_val2->Get() && !is_descending)
	{
	    return 1;
	}

	return -1;
}

//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::CalcScaleFactorCumulativeConj
//
//	@doc:
//		Calculate the cumulative scaling factor for conjunction of filters
//		after applying damping multiplier
//
//---------------------------------------------------------------------------
CDouble
CScaleFactorUtils::CalcScaleFactorCumulativeConj
	(
	const CStatisticsConfig *stats_config,
	CDoubleArray *scale_factors
	)
{
	GPOS_ASSERT(NULL != stats_config);
	GPOS_ASSERT(NULL != scale_factors);

	const ULONG num_cols = scale_factors->Size();
	CDouble scale_factor(1.0);
	if (1 < num_cols)
	{
		// sort (in desc order) the scaling factor based on the selectivity of each column
		scale_factors->Sort(CScaleFactorUtils::DescendingOrderCmpFunc);
	}

	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		// apply damping factor
		CDouble local_scale_factor = *(*scale_factors)[ul];
		scale_factor = scale_factor * std::max
										(
										CStatistics::MinRows.Get(),
										(local_scale_factor * CScaleFactorUtils::DampedFilterScaleFactor(stats_config, ul + 1)).Get()
										);
	}

	return scale_factor;
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::CalcScaleFactorCumulativeDisj
//
//	@doc:
//		Calculate the cumulative scaling factor for disjunction of filters
//		after applying damping multiplier
//
//---------------------------------------------------------------------------
CDouble
CScaleFactorUtils::CalcScaleFactorCumulativeDisj
	(
	const CStatisticsConfig *stats_config,
	CDoubleArray *scale_factors,
	CDouble total_rows
	)
{
	GPOS_ASSERT(NULL != stats_config);
	GPOS_ASSERT(NULL != scale_factors);

	const ULONG num_cols = scale_factors->Size();
	GPOS_ASSERT(0 < num_cols);

	if (1 == num_cols)
	{
		return *(*scale_factors)[0];
	}

	// sort (in ascending order) the scaling factor based on the selectivity of each column
	scale_factors->Sort(CScaleFactorUtils::AscendingOrderCmpFunc);

	// accumulate row estimates of different predicates after applying damping
	// rows = rows0 + rows1 * 0.75 + rows2 *(0.75)^2 + ...

	CDouble rows(0.0);
	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		CDouble local_scale_factor = *(*scale_factors)[ul];
		GPOS_ASSERT(InvalidScaleFactor < local_scale_factor);

		// get a row estimate based on current scale factor
		CDouble local_rows = total_rows / local_scale_factor;

		// accumulate row estimates after damping
		rows = rows + std::max
							(
							CStatistics::MinRows.Get(),
							(local_rows * CScaleFactorUtils::DampedFilterScaleFactor(stats_config, ul + 1)).Get()
							);

		// cap accumulated row estimate with total number of rows
		rows = std::min(rows.Get(), total_rows.Get());
	}

	// return an accumulated scale factor based on accumulated row estimate
	return CDouble(total_rows / rows);
}


// EOF
