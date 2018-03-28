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
			CDouble DCumulativeJoinScaleFactor(const CStatisticsConfig *pstatsconf, DrgPdouble *pdrgpd);

			// return scaling factor of the join predicate after apply damping
			static
			CDouble DDampingJoin(const CStatisticsConfig *pstatsconf, ULONG ulNumColumns);

			// return scaling factor of the filter after apply damping
			static
			CDouble DDampingFilter(const CStatisticsConfig *pstatsconf, ULONG ulNumColumns);

			// return scaling factor of the group by predicate after apply damping
			static
			CDouble DDampingGroupBy(const CStatisticsConfig *pstatsconf, ULONG ulNumColumns);

			// sort the array of scaling factor
			static
			void SortScalingFactor(DrgPdouble *pdrgpdScaleFactor, BOOL fDescending);

			// calculate the cumulative scaling factor for conjunction after applying damping multiplier
			static
			CDouble DScaleFactorCumulativeConj(const CStatisticsConfig *pstatsconf, DrgPdouble *pdrgpdScaleFactor);

			// calculate the cumulative scaling factor for disjunction after applying damping multiplier
			static
			CDouble DScaleFactorCumulativeDisj(const CStatisticsConfig *pstatsconf, DrgPdouble *pdrgpdScaleFactor, CDouble dRowsTotal);

			// comparison function in descending order
			static
			INT IDoubleDescCmp(const void *pv1, const void *pv2);

			// comparison function in ascending order
			static
			INT IDoubleAscCmp(const void *pv1, const void *pv2);

			// helper function for double comparison
			static
			INT IDoubleCmp(const CDouble *pd1, const CDouble *pd2, BOOL fDesc);

			// default scaling factor of LIKE predicate
			static
			const CDouble DDefaultScaleFactorLike;

			// default scaling factor of join predicate
			static
			const CDouble DDefaultScaleFactorJoin;

			// default scaling factor of non-equality (<, <=, >=, <=) join predicate
			// Note: scale factor of InEquality (!= also denoted as <>) is computed from scale factor of equi-join
			static
			const CDouble DDefaultScaleFactorNonEqualityJoin;

			// invalid scale factor
			static
			const CDouble DInvalidScaleFactor;
	}; // class CScaleFactorUtils
}

#endif // !GPOPT_CScaleFactorUtils_H

// EOF
