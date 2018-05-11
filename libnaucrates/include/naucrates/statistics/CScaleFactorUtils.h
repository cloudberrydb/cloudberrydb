//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2014 Pivotal Inc.
//
//	@filename:
//		CScaleFactorUtils.h
//
//	@doc:
//		Utility functions for computing scale factors used in stats computation
//---------------------------------------------------------------------------
#ifndef GPOPT_CScaleFactorUtils_H
#define GPOPT_CScaleFactorUtils_H

#include "gpos/base.h"
#include "gpopt/engine/CStatisticsConfig.h"

#include "naucrates/statistics/CHistogram.h"

namespace gpnaucrates
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CScaleFactorUtils
	//
	//	@doc:
	//		Utility functions for computing scale factors used in stats computation
	//
	//---------------------------------------------------------------------------
	class CScaleFactorUtils
	{
		public:

			// calculate the cumulative join scaling factor
			static
			CDouble CumulativeJoinScaleFactor(const CStatisticsConfig *stats_config, CDoubleArray *join_conds_scale_factors);

			// return scaling factor of the join predicate after apply damping
			static
			CDouble DampedJoinScaleFactor(const CStatisticsConfig *stats_config, ULONG num_columns);

			// return scaling factor of the filter after apply damping
			static
			CDouble DampedFilterScaleFactor(const CStatisticsConfig *stats_config, ULONG num_columns);

			// return scaling factor of the group by predicate after apply damping
			static
			CDouble DampedGroupByScaleFactor(const CStatisticsConfig *stats_config, ULONG num_columns);

			// sort the array of scaling factor
			static
			void SortScalingFactor(CDoubleArray *scale_factors, BOOL is_descending);

			// calculate the cumulative scaling factor for conjunction after applying damping multiplier
			static
			CDouble CalcScaleFactorCumulativeConj(const CStatisticsConfig *stats_config, CDoubleArray *scale_factors);

			// calculate the cumulative scaling factor for disjunction after applying damping multiplier
			static
			CDouble CalcScaleFactorCumulativeDisj(const CStatisticsConfig *stats_config, CDoubleArray *scale_factors, CDouble tota_rows);

			// comparison function in descending order
			static
			INT DescendingOrderCmpFunc(const void *val1, const void *val2);

			// comparison function in ascending order
			static
			INT AscendingOrderCmpFunc(const void *val1, const void *val2);

			// helper function for double comparison
			static
			INT DoubleCmpFunc(const CDouble *double_data1, const CDouble *double_data2, BOOL is_descending);

			// default scaling factor of LIKE predicate
			static
			const CDouble DDefaultScaleFactorLike;

			// default scaling factor of join predicate
			static
			const CDouble DefaultJoinPredScaleFactor;

			// default scaling factor of non-equality (<, <=, >=, <=) join predicate
			// Note: scale factor of InEquality (!= also denoted as <>) is computed from scale factor of equi-join
			static
			const CDouble DefaultInequalityJoinPredScaleFactor;

			// invalid scale factor
			static
			const CDouble InvalidScaleFactor;
	}; // class CScaleFactorUtils
}

#endif // !GPOPT_CScaleFactorUtils_H

// EOF
