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
const CDouble CScaleFactorUtils::DDefaultScaleFactorNonEqualityJoin(3.0);

// default scaling factor of join predicates
const CDouble CScaleFactorUtils::DDefaultScaleFactorJoin(100.0);

// default scaling factor of LIKE predicate
const CDouble CScaleFactorUtils::DDefaultScaleFactorLike(150.0);

// invalid scale factor
const CDouble CScaleFactorUtils::DInvalidScaleFactor(0.0);

//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::DCumulativeJoinScaleFactor
//
//	@doc:
//		Calculate the cumulative join scaling factor
//
//---------------------------------------------------------------------------
CDouble
CScaleFactorUtils::DCumulativeJoinScaleFactor
	(
	const CStatisticsConfig *pstatsconf,
	DrgPdouble *pdrgpd
	)
{
	GPOS_ASSERT(NULL != pstatsconf);
	GPOS_ASSERT(NULL != pdrgpd);

	const ULONG ulJoinConds = pdrgpd->UlLength();
	if (1 < ulJoinConds)
	{
		// sort (in desc order) the scaling factor of the join conditions
		pdrgpd->Sort(CScaleFactorUtils::IDoubleDescCmp);
	}

	CDouble dScaleFactor(1.0);
	// iterate over joins
	for (ULONG ul = 0; ul < ulJoinConds; ul++)
	{
		CDouble dScaleFactorLocal = *(*pdrgpd)[ul];

		dScaleFactor = dScaleFactor * std::max
										(
										CStatistics::DMinRows.DVal(),
										(dScaleFactorLocal * DDampingJoin(pstatsconf, ul + 1)).DVal()
										);
	}

	return dScaleFactor;
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::DDampingJoin
//
//	@doc:
//		Return scaling factor of the join predicate after apply damping
//
//---------------------------------------------------------------------------
CDouble
CScaleFactorUtils::DDampingJoin
	(
	const CStatisticsConfig *pstatsconf,
	ULONG ulNumColumns
	)
{
	if (1 >= ulNumColumns)
	{
		return CDouble(1.0);
	}

	return pstatsconf->DDampingFactorJoin().FpPow(CDouble(ulNumColumns));
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::DDampingFilter
//
//	@doc:
//		Return scaling factor of the filter after apply damping
//
//---------------------------------------------------------------------------
CDouble
CScaleFactorUtils::DDampingFilter
	(
	const CStatisticsConfig *pstatsconf,
	ULONG ulNumColumns
	)
{
	GPOS_ASSERT(NULL != pstatsconf);

	if (1 >= ulNumColumns)
	{
		return CDouble(1.0);
	}

	return pstatsconf->DDampingFactorFilter().FpPow(CDouble(ulNumColumns));
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::DDampingGroupBy
//
//	@doc:
//		Return scaling factor of the group by predicate after apply damping
//
//---------------------------------------------------------------------------
CDouble
CScaleFactorUtils::DDampingGroupBy
	(
	const CStatisticsConfig *pstatsconf,
	ULONG ulNumColumns
	)
{
	GPOS_ASSERT(NULL != pstatsconf);

	if (1 > ulNumColumns)
	{
		return CDouble(1.0);
	}

	return pstatsconf->DDampingFactorGroupBy().FpPow(CDouble(ulNumColumns + 1));
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
	DrgPdouble *pdrgpdScaleFactor,
	BOOL fDescending
	)
{
	GPOS_ASSERT(NULL != pdrgpdScaleFactor);
	const ULONG ulCols = pdrgpdScaleFactor->UlLength();
	if (1 < ulCols)
	{
		if (fDescending)
		{
			// sort (in desc order) the scaling factor based on the selectivity of each column
			pdrgpdScaleFactor->Sort(CScaleFactorUtils::IDoubleDescCmp);
		}
		else
		{
			// sort (in ascending order) the scaling factor based on the selectivity of each column
			pdrgpdScaleFactor->Sort(CScaleFactorUtils::IDoubleAscCmp);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::IDoubleDescCmp
//
//	@doc:
//		Comparison function for sorting double
//
//---------------------------------------------------------------------------
INT
CScaleFactorUtils::IDoubleDescCmp
	(
	const void *pv1,
	const void *pv2
	)
{
	GPOS_ASSERT(NULL != pv1 && NULL != pv2);
	const CDouble *pd1 = *(const CDouble **) pv1;
	const CDouble *pd2 = *(const CDouble **) pv2;

	return IDoubleCmp(pd1, pd2, true /*fDesc*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::IDoubleAscCmp
//
//	@doc:
//		Comparison function for sorting double
//
//---------------------------------------------------------------------------
INT
CScaleFactorUtils::IDoubleAscCmp
	(
	const void *pv1,
	const void *pv2
	)
{
	GPOS_ASSERT(NULL != pv1 && NULL != pv2);
	const CDouble *pd1 = *(const CDouble **) pv1;
	const CDouble *pd2 = *(const CDouble **) pv2;

	return IDoubleCmp(pd1, pd2, false /*fDesc*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::IDoubleCmp
//
//	@doc:
//		Helper function for double comparison
//
//---------------------------------------------------------------------------
INT
CScaleFactorUtils::IDoubleCmp
	(
	const CDouble *pd1,
	const CDouble *pd2,
	BOOL fDesc
	)
{
	GPOS_ASSERT(NULL != pd1);
	GPOS_ASSERT(NULL != pd2);

	if (pd1->DVal() == pd2->DVal())
	{
		return 0;
	}

	if (pd1->DVal() < pd2->DVal() && fDesc)
	{
	    return 1;
	}

	if (pd1->DVal() > pd2->DVal() && !fDesc)
	{
	    return 1;
	}

	return -1;
}

//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::DScaleFactorCumulativeConj
//
//	@doc:
//		Calculate the cumulative scaling factor for conjunction of filters
//		after applying damping multiplier
//
//---------------------------------------------------------------------------
CDouble
CScaleFactorUtils::DScaleFactorCumulativeConj
	(
	const CStatisticsConfig *pstatsconf,
	DrgPdouble *pdrgpdScaleFactor
	)
{
	GPOS_ASSERT(NULL != pstatsconf);
	GPOS_ASSERT(NULL != pdrgpdScaleFactor);

	const ULONG ulCols = pdrgpdScaleFactor->UlLength();
	CDouble dScaleFactor(1.0);
	if (1 < ulCols)
	{
		// sort (in desc order) the scaling factor based on the selectivity of each column
		pdrgpdScaleFactor->Sort(CScaleFactorUtils::IDoubleDescCmp);
	}

	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		// apply damping factor
		CDouble dScaleFactorLocal = *(*pdrgpdScaleFactor)[ul];
		dScaleFactor = dScaleFactor * std::max
										(
										CStatistics::DMinRows.DVal(),
										(dScaleFactorLocal * CScaleFactorUtils::DDampingFilter(pstatsconf, ul + 1)).DVal()
										);
	}

	return dScaleFactor;
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::DScaleFactorCumulativeDisj
//
//	@doc:
//		Calculate the cumulative scaling factor for disjunction of filters
//		after applying damping multiplier
//
//---------------------------------------------------------------------------
CDouble
CScaleFactorUtils::DScaleFactorCumulativeDisj
	(
	const CStatisticsConfig *pstatsconf,
	DrgPdouble *pdrgpdScaleFactor,
	CDouble dRowsTotal
	)
{
	GPOS_ASSERT(NULL != pstatsconf);
	GPOS_ASSERT(NULL != pdrgpdScaleFactor);

	const ULONG ulCols = pdrgpdScaleFactor->UlLength();
	GPOS_ASSERT(0 < ulCols);

	if (1 == ulCols)
	{
		return *(*pdrgpdScaleFactor)[0];
	}

	// sort (in ascending order) the scaling factor based on the selectivity of each column
	pdrgpdScaleFactor->Sort(CScaleFactorUtils::IDoubleAscCmp);

	// accumulate row estimates of different predicates after applying damping
	// rows = rows0 + rows1 * 0.75 + rows2 *(0.75)^2 + ...

	CDouble dRows(0.0);
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		CDouble dScaleFactorLocal = *(*pdrgpdScaleFactor)[ul];
		GPOS_ASSERT(DInvalidScaleFactor < dScaleFactorLocal);

		// get a row estimate based on current scale factor
		CDouble dRowsLocal = dRowsTotal / dScaleFactorLocal;

		// accumulate row estimates after damping
		dRows = dRows + std::max
							(
							CStatistics::DMinRows.DVal(),
							(dRowsLocal * CScaleFactorUtils::DDampingFilter(pstatsconf, ul + 1)).DVal()
							);

		// cap accumulated row estimate with total number of rows
		dRows = std::min(dRows.DVal(), dRowsTotal.DVal());
	}

	// return an accumulated scale factor based on accumulated row estimate
	return CDouble(dRowsTotal / dRows);
}


// EOF
