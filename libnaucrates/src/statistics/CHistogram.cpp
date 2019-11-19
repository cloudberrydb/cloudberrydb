//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		CHistogram.cpp
//
//	@doc:
//		Implementation of single-dimensional histogram
//---------------------------------------------------------------------------


#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/statistics/CHistogram.h"
#include "naucrates/dxl/operators/CDXLScalarConstValue.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"
#include "gpos/common/syslibwrapper.h"

#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CStatisticsUtils.h"
#include "naucrates/statistics/CLeftAntiSemiJoinStatsProcessor.h"
#include "naucrates/statistics/CScaleFactorUtils.h"

#include "gpopt/base/CColRef.h"

using namespace gpnaucrates;
using namespace gpopt;
using namespace gpmd;

// default histogram selectivity
const CDouble CHistogram::DefaultSelectivity(0.4);

// default scale factor when there is no filtering of input
const CDouble CHistogram::NeutralScaleFactor(1.0);

// the minimum number of distinct values in a column
const CDouble CHistogram::MinDistinct(1.0);

// default Null frequency
const CDouble CHistogram::DefaultNullFreq(0.0);

// default NDV remain
const CDouble CHistogram::DefaultNDVRemain(0.0);

// default frequency of NDV remain
const CDouble CHistogram::DefaultNDVFreqRemain(0.0);

// sample size used to estimate skew
#define GPOPT_SKEW_SAMPLE_SIZE 1000

// ctor
CHistogram::CHistogram
	(
	CBucketArray *histogram_buckets,
	BOOL is_well_defined
	)
	:
	m_histogram_buckets(histogram_buckets),
	m_is_well_defined(is_well_defined),
	m_null_freq(CHistogram::DefaultNullFreq),
	m_distinct_remaining(DefaultNDVRemain),
	m_freq_remaining(DefaultNDVFreqRemain),
	m_skew_was_measured(false),
	m_skew(1.0),
	m_NDVs_were_scaled(false),
	m_is_col_stats_missing(false)
{
	GPOS_ASSERT(NULL != histogram_buckets);
}

// ctor
CHistogram::CHistogram
	(
	CBucketArray *histogram_buckets,
	BOOL is_well_defined,
	CDouble null_freq,
	CDouble distinct_remaining,
	CDouble freq_remaining,
	BOOL is_col_stats_missing
	)
	:
	m_histogram_buckets(histogram_buckets),
	m_is_well_defined(is_well_defined),
	m_null_freq(null_freq),
	m_distinct_remaining(distinct_remaining),
	m_freq_remaining(freq_remaining),
	m_skew_was_measured(false),
	m_skew(1.0),
	m_NDVs_were_scaled(false),
	m_is_col_stats_missing(is_col_stats_missing)
{
	GPOS_ASSERT(m_histogram_buckets);
	GPOS_ASSERT(CDouble(0.0) <= null_freq);
	GPOS_ASSERT(CDouble(1.0) >= null_freq);
	GPOS_ASSERT(CDouble(0.0) <= distinct_remaining);
	GPOS_ASSERT(CDouble(0.0) <= freq_remaining);
	GPOS_ASSERT(CDouble(1.0) >= freq_remaining);
	// if distinct_remaining is 0, freq_remaining must be 0 too
	GPOS_ASSERT_IMP(distinct_remaining < CStatistics::Epsilon, freq_remaining < CStatistics::Epsilon);
}

// set histograms null frequency
void
CHistogram::SetNullFrequency
	(
	CDouble null_freq
	)
{
	GPOS_ASSERT(CDouble(0.0) <= null_freq && CDouble(1.0) >= null_freq);
	m_null_freq = null_freq;
}

//	print function
IOstream&
CHistogram::OsPrint
	(
	IOstream &os
	)
	const
{
	os << std::endl << "[" << std::endl;

	ULONG num_buckets = m_histogram_buckets->Size();
	for (ULONG bucket_index = 0; bucket_index < num_buckets; bucket_index++)
	{
		os << "b" << bucket_index << " = ";
		(*m_histogram_buckets)[bucket_index]->OsPrint(os);
		os << std::endl;
	}
	os << "]" << std::endl;

	os << "Null fraction: " << m_null_freq << std::endl;

	os << "Remaining NDV: " << m_distinct_remaining << std::endl;

	os << "Remaining frequency: " << m_freq_remaining << std::endl;

	if (m_skew_was_measured)
	{
		os << "Skew: " << m_skew << std::endl;
	}
	else
	{
		os << "Skew: not measured" << std::endl;
	}

	os << "Was NDVs re-scaled Based on Row Estimate: " << m_NDVs_were_scaled << std::endl;

	return os;
}

// check if histogram is empty
BOOL
CHistogram::IsEmpty
	()
	const
{
	return (0 == m_histogram_buckets->Size() && CStatistics::Epsilon > m_null_freq && CStatistics::Epsilon > m_distinct_remaining);
}

// construct new histogram with less than or less than equal to filter
CHistogram *
CHistogram::MakeHistogramLessThanOrLessThanEqualFilter
	(
	CMemoryPool *mp,
	CStatsPred::EStatsCmpType stats_cmp_type,
	CPoint *point
	)
	const
{
	GPOS_ASSERT(CStatsPred::EstatscmptL == stats_cmp_type || CStatsPred::EstatscmptLEq == stats_cmp_type);

	CBucketArray *new_buckets = GPOS_NEW(mp) CBucketArray(mp);
	const ULONG num_buckets = m_histogram_buckets->Size();

	for (ULONG bucket_index = 0; bucket_index < num_buckets; bucket_index++)
	{
		CBucket *bucket = (*m_histogram_buckets)[bucket_index];
		if (bucket->IsBefore(point))
		{
			break;
		}
		else if (bucket->IsAfter(point))
		{
			new_buckets->Append(bucket->MakeBucketCopy(mp));
		}
		else
		{
			GPOS_ASSERT(bucket->Contains(point));
			CBucket *last_bucket = bucket->MakeBucketScaleUpper(mp, point, CStatsPred::EstatscmptLEq == stats_cmp_type /*include_upper*/);
			if (NULL != last_bucket)
			{
				new_buckets->Append(last_bucket);
			}
			break;
		}
	}

	CDouble distinct_remaining = 0.0;
	CDouble freq_remaining = 0.0;
	if (CStatistics::Epsilon < m_distinct_remaining * DefaultSelectivity)
	{
		distinct_remaining = m_distinct_remaining * DefaultSelectivity;
		freq_remaining = m_freq_remaining * DefaultSelectivity;
	}

	return GPOS_NEW(mp) CHistogram
					(
					new_buckets,
					true, // is_well_defined
					CDouble(0.0), // fNullFreq
					distinct_remaining,
					freq_remaining
					);
}

// return an array buckets after applying non equality filter on the histogram buckets
CBucketArray *
CHistogram::MakeBucketsWithInequalityFilter
	(
	CMemoryPool *mp,
	CPoint *point
	)
	const
{
	GPOS_ASSERT(NULL != point);
	CBucketArray *new_buckets = GPOS_NEW(mp) CBucketArray(mp);
	const ULONG num_buckets = m_histogram_buckets->Size();
	bool point_is_null = point->GetDatum()->IsNull();

	for (ULONG bucket_index = 0; bucket_index < num_buckets; bucket_index++)
	{
		CBucket *bucket = (*m_histogram_buckets)[bucket_index];

		if (bucket->Contains(point) && !point_is_null)
		{
			CBucket *less_than_bucket = bucket->MakeBucketScaleUpper(mp, point, false /*include_upper */);
			if (NULL != less_than_bucket)
			{
				new_buckets->Append(less_than_bucket);
			}
			CBucket *greater_than_bucket = bucket->MakeBucketGreaterThan(mp, point);
			if (NULL != greater_than_bucket)
			{
				new_buckets->Append(greater_than_bucket);
			}
		}
		else
		{
			new_buckets->Append(bucket->MakeBucketCopy(mp));
		}
	}

	return new_buckets;
}

// construct new histogram with not equal filter
CHistogram *
CHistogram::MakeHistogramInequalityFilter
	(
	CMemoryPool *mp,
	CPoint *point
	)
	const
{
	GPOS_ASSERT(NULL != point);

	CBucketArray *histogram_buckets = MakeBucketsWithInequalityFilter(mp, point);
	CDouble null_freq(0.0);

	return GPOS_NEW(mp) CHistogram(histogram_buckets, true /*is_well_defined*/, null_freq, m_distinct_remaining, m_freq_remaining);
}

// construct new histogram with IDF filter
CHistogram *
CHistogram::MakeHistogramIDFFilter
	(
	CMemoryPool *mp,
	CPoint *point
	)
	const
{
	GPOS_ASSERT(NULL != point);

	CBucketArray *histogram_buckets = MakeBucketsWithInequalityFilter(mp, point);
	CDouble null_freq(0.0);
	if (!point->GetDatum()->IsNull())
	{
		// (col IDF NOT-NULL-CONSTANT) means that null values will also be returned
		null_freq = m_null_freq;
	}

	return GPOS_NEW(mp) CHistogram(histogram_buckets, true /*is_well_defined*/, null_freq, m_distinct_remaining, m_freq_remaining);
}

// return an array buckets after applying equality filter on the histogram buckets
CBucketArray *
CHistogram::MakeBucketsWithEqualityFilter
	(
	CMemoryPool *mp,
	CPoint *point
	)
	const
{
	GPOS_ASSERT(NULL != point);

	CBucketArray *histogram_buckets = GPOS_NEW(mp) CBucketArray(mp);

	if (point->GetDatum()->IsNull())
	{
		return histogram_buckets;
	}

	const ULONG num_buckets = m_histogram_buckets->Size();
	ULONG bucket_index = 0;

	for (bucket_index = 0; bucket_index < num_buckets; bucket_index++)
	{
		CBucket *bucket = (*m_histogram_buckets)[bucket_index];

		if (bucket->Contains(point))
		{
			if (bucket->IsSingleton())
			{
				// reuse existing bucket
				histogram_buckets->Append(bucket->MakeBucketCopy(mp));
			}
			else
			{
				// scale containing bucket
				CBucket *last_bucket = bucket->MakeBucketSingleton(mp, point);
				histogram_buckets->Append(last_bucket);
			}
			break; // only one bucket can contain point
		}
	}

	return histogram_buckets;
}

// construct new histogram with equality filter
CHistogram *
CHistogram::MakeHistogramEqualFilter
	(
	CMemoryPool *mp,
	CPoint *point
	)
	const
{
	GPOS_ASSERT(NULL != point);

	if (point->GetDatum()->IsNull())
	{
		return GPOS_NEW(mp) CHistogram
							(
							GPOS_NEW(mp) CBucketArray(mp),
							true /* is_well_defined */,
							m_null_freq,
							DefaultNDVRemain,
							DefaultNDVFreqRemain
							);
	}
	
	CBucketArray *histogram_buckets = MakeBucketsWithEqualityFilter(mp, point);

	if (CStatistics::Epsilon < m_distinct_remaining && 0 == histogram_buckets->Size()) // no match is found in the buckets
	{
		return GPOS_NEW(mp) CHistogram
						(
						histogram_buckets,
						true, // is_well_defined
						0.0, // null_freq
						1.0, // distinct_remaining
						std::min(CDouble(1.0), m_freq_remaining / m_distinct_remaining) // freq_remaining
						);
	}

	return GPOS_NEW(mp) CHistogram(histogram_buckets);
}

// construct new histogram with INDF filter
CHistogram *
CHistogram::MakeHistogramINDFFilter
	(
	CMemoryPool *mp,
	CPoint *point
	)
	const
{
	GPOS_ASSERT(NULL != point);

	CBucketArray *histogram_buckets = MakeBucketsWithEqualityFilter(mp, point);
	const ULONG num_of_buckets = histogram_buckets->Size();
	CDouble null_freq(0.0);
	if (point->GetDatum()->IsNull())
	{
		GPOS_ASSERT(0 == num_of_buckets);
		// (col INDF NULL) means that only the null values will be returned
		null_freq = m_null_freq;
	}

	if (CStatistics::Epsilon < m_distinct_remaining && 0 == num_of_buckets) // no match is found in the buckets
	{
		return GPOS_NEW(mp) CHistogram
						(
						histogram_buckets,
						true, // is_well_defined
						null_freq,
						1.0, // distinct_remaining
						std::min(CDouble(1.0), m_freq_remaining / m_distinct_remaining) // freq_remaining
						);
	}

	return GPOS_NEW(mp) CHistogram
					(
					histogram_buckets,
					true /* is_well_defined */,
					null_freq,
					CHistogram::DefaultNDVRemain,
					CHistogram::DefaultNDVFreqRemain
					);
}

// construct new histogram with greater than or greater than equal filter
CHistogram *
CHistogram::MakeHistogramGreaterThanOrGreaterThanEqualFilter
	(
	CMemoryPool *mp,
	CStatsPred::EStatsCmpType stats_cmp_type,
	CPoint *point
	)
	const
{
	GPOS_ASSERT(CStatsPred::EstatscmptGEq == stats_cmp_type || CStatsPred::EstatscmptG == stats_cmp_type);

	CBucketArray *new_buckets = GPOS_NEW(mp) CBucketArray(mp);
	const ULONG num_buckets = m_histogram_buckets->Size();

	// find first bucket that contains point
	ULONG bucket_index = 0;
	for (bucket_index = 0; bucket_index < num_buckets; bucket_index++)
	{
		CBucket *bucket = (*m_histogram_buckets)[bucket_index];
		if (bucket->IsBefore(point))
		{
			break;
		}
		if (bucket->Contains(point))
		{
			if (CStatsPred::EstatscmptGEq == stats_cmp_type)
			{
				// first bucket needs to be scaled down
				CBucket *first_bucket = bucket->MakeBucketScaleLower(mp, point,  true /* include_lower */);
				new_buckets->Append(first_bucket);
			}
			else
			{
				CBucket *greater_than_bucket = bucket->MakeBucketGreaterThan(mp, point);
				if (NULL != greater_than_bucket)
				{
					new_buckets->Append(greater_than_bucket);
				}
			}
			bucket_index++;
			break;
		}
	}

	// add rest of the buckets
	for (; bucket_index < num_buckets; bucket_index++)
	{
		CBucket *bucket = (*m_histogram_buckets)[bucket_index];
		new_buckets->Append(bucket->MakeBucketCopy(mp));
	}

	CDouble distinct_remaining = 0.0;
	CDouble freq_remaining = 0.0;
	if (CStatistics::Epsilon < m_distinct_remaining * DefaultSelectivity)
	{
		distinct_remaining = m_distinct_remaining * DefaultSelectivity;
		freq_remaining = m_freq_remaining * DefaultSelectivity;
	}

	return GPOS_NEW(mp) CHistogram
					(
					new_buckets,
					true, // is_well_defined
					CDouble(0.0), // fNullFreq
					distinct_remaining,
					freq_remaining
					);
}

// sum of frequencies from buckets.
CDouble
CHistogram::GetFrequency
	()
	const
{
	CDouble frequency(0.0);
	const ULONG num_of_buckets = m_histogram_buckets->Size();
	for (ULONG bucket_index = 0; bucket_index < num_of_buckets; bucket_index++)
	{
		CBucket *bucket = (*m_histogram_buckets)[bucket_index];
		frequency = frequency + bucket->GetFrequency();
	}

	if (CStatistics::Epsilon < m_null_freq)
	{
		frequency = frequency + m_null_freq;
	}

	return frequency + m_freq_remaining;
}

// sum of number of distinct values from buckets
CDouble
CHistogram::GetNumDistinct
	()
	const
{
	CDouble distinct(0.0);
	const ULONG num_of_buckets = m_histogram_buckets->Size();
	for (ULONG bucket_index = 0; bucket_index < num_of_buckets; bucket_index++)
	{
		CBucket *bucket = (*m_histogram_buckets)[bucket_index];
		distinct = distinct + bucket->GetNumDistinct();
	}
	CDouble distinct_null(0.0);
	if (CStatistics::Epsilon < m_null_freq)
	{
		distinct_null = 1.0;
	}

	return distinct + distinct_null + m_distinct_remaining;
}

// cap the total number of distinct values (NDVs) in buckets to the number of rows
void
CHistogram::CapNDVs
	(
	CDouble rows
	)
{
	const ULONG num_of_buckets = m_histogram_buckets->Size();
	CDouble distinct = GetNumDistinct();
	if (rows >= distinct)
	{
		// no need for capping
		return;
	}

	m_NDVs_were_scaled = true;

	CDouble scale_ratio = (rows / distinct).Get();
	for (ULONG ul = 0; ul < num_of_buckets; ul++)
	{
		CBucket *bucket = (*m_histogram_buckets)[ul];
		CDouble distinct_bucket = bucket->GetNumDistinct();
		bucket->SetDistinct(std::max(CHistogram::MinDistinct.Get(), (distinct_bucket * scale_ratio).Get()));
	}

	m_distinct_remaining = m_distinct_remaining * scale_ratio;
}

// sum of frequencies is approx 1.0
BOOL
CHistogram::IsNormalized
	()
	const
{
	CDouble frequency = GetFrequency();

	return (frequency > CDouble(1.0) - CStatistics::Epsilon
			&& frequency < CDouble(1.0) + CStatistics::Epsilon);
}

//	check if histogram is well formed? Checks are:
//		1. Buckets should be in order (if applicable)
//		2. Buckets should not overlap
//		3. Frequencies should add up to less than 1.0
BOOL
CHistogram::IsValid
	()
	const
{
	// frequencies should not add up to more than 1.0
	if (GetFrequency() > CDouble(1.0) + CStatistics::Epsilon)
	{
		return false;
	}

	if (!IsHistogramForTextRelatedTypes())
	{
		for (ULONG bucket_index = 1; bucket_index < m_histogram_buckets->Size(); bucket_index++)
		{
			CBucket *bucket = (*m_histogram_buckets)[bucket_index];
			CBucket *previous_bucket = (*m_histogram_buckets)[bucket_index - 1];

			// the later bucket's lower point must be greater than or equal to
			// earlier bucket's upper point. Even if the underlying datum does not
			// support ordering, the check is safe.
			if (bucket->GetLowerBound()->IsLessThan(previous_bucket->GetUpperBound()))
			{
				return false;
			}
		}
	}
	return true;
}

// construct new histogram with filter and normalize output histogram
CHistogram *
CHistogram::MakeHistogramFilterNormalize
	(
	CMemoryPool *mp,
	CStatsPred::EStatsCmpType stats_cmp_type,
	CPoint *point,
	CDouble *scale_factor
	)
	const
{
	// if histogram is not well-defined, then result is not well defined
	if (!IsWellDefined())
	{
		CHistogram *result_histogram = GPOS_NEW(mp) CHistogram(GPOS_NEW(mp) CBucketArray(mp), false /* is_well_defined */);
		*scale_factor = CDouble(1.0) / CHistogram::DefaultSelectivity;
		return result_histogram;
	}

	CHistogram *result_histogram = MakeHistogramFilter(mp, stats_cmp_type, point);
	*scale_factor = result_histogram->NormalizeHistogram();
	GPOS_ASSERT(result_histogram->IsValid());

	return result_histogram;
}

// construct new histogram by joining with another and normalize
// output histogram. If the join is not an equality join the function
// returns an empty histogram
CHistogram *
CHistogram::MakeJoinHistogramNormalize
	(
	CMemoryPool *mp,
	CStatsPred::EStatsCmpType stats_cmp_type,
	CDouble rows,
	const CHistogram *other_histogram,
	CDouble rows_other,
	CDouble *scale_factor
	)
	const
{
	GPOS_ASSERT(NULL != other_histogram);

	if (CStatisticsUtils::IsStatsCmpTypeNdvEq(stats_cmp_type))
	{
		*scale_factor = std::max
								 (
								  std::max(CHistogram::MinDistinct.Get(), GetNumDistinct().Get()),
								  std::max(CHistogram::MinDistinct.Get(), other_histogram->GetNumDistinct().Get())
								  );
		return MakeNDVBasedJoinHistogramEqualityFilter(mp, other_histogram);
	}

	BOOL fEqOrINDF = (CStatsPred::EstatscmptEq == stats_cmp_type || CStatsPred::EstatscmptINDF == stats_cmp_type);
	if (!fEqOrINDF)
	{
		*scale_factor = CScaleFactorUtils::DefaultInequalityJoinPredScaleFactor;

		if (CStatsPred::EstatscmptNEq == stats_cmp_type || CStatsPred::EstatscmptIDF == stats_cmp_type)
		{
			*scale_factor = GetInequalityJoinScaleFactor(mp, rows, other_histogram, rows_other);
		}

		return MakeJoinHistogram(mp, stats_cmp_type, other_histogram);
	}

	// if either histogram is not well-defined, the result is not well defined
	if (!IsWellDefined() || !other_histogram->IsWellDefined())
	{
		CHistogram *result_histogram = GPOS_NEW(mp) CHistogram(GPOS_NEW(mp) CBucketArray(mp), false /* is_well_defined */);
		(*scale_factor) = CDouble(std::min(rows.Get(), rows_other.Get()));

		return result_histogram;
	}
	
	CHistogram *result_histogram = MakeJoinHistogram(mp, stats_cmp_type, other_histogram);
	*scale_factor = result_histogram->NormalizeHistogram();

	// based on Ramakrishnan and Gehrke, "Database Management Systems, Third Ed", page 484
	// the scaling factor of equality join is the MAX of the number of distinct
	// values in each of the inputs

	*scale_factor = std::max
						(
						std::max(MinDistinct.Get(), GetNumDistinct().Get()),
						std::max(MinDistinct.Get(), other_histogram->GetNumDistinct().Get())
						);

	CDouble cartesian_product_num_rows = rows * rows_other;
	if (result_histogram->IsEmpty())
	{
		// if join histogram is empty for equality join condition
		// use Cartesian product size as scale factor
		*scale_factor = cartesian_product_num_rows;
	}

	if (CStatsPred::EstatscmptINDF == stats_cmp_type)
	{
		// if the predicate is INDF then we must count for the cartesian
		// product of NULL values in both the histograms
		CDouble expected_num_rows_eq_join = cartesian_product_num_rows / *scale_factor;
		CDouble num_null_rows = rows * m_null_freq;
		CDouble num_null_rows_other = rows_other * other_histogram->GetNullFreq();
		CDouble expected_num_rows_INDF = expected_num_rows_eq_join + (num_null_rows * num_null_rows_other);
		*scale_factor = cartesian_product_num_rows / expected_num_rows_INDF;
	}

	// bound scale factor by cross product
	*scale_factor = std::min((*scale_factor).Get(), cartesian_product_num_rows.Get());

	GPOS_ASSERT(result_histogram->IsValid());
	return result_histogram;
}

// scalar factor of inequality (!=) join condition
CDouble
CHistogram::GetInequalityJoinScaleFactor
	(
	CMemoryPool *mp,
	CDouble rows,
	const CHistogram *other_histogram,
	CDouble rows_other
	)
	const
{
	GPOS_ASSERT(NULL != other_histogram);

	CDouble scale_factor(1.0);

	// we compute the scale factor of the inequality join (!= aka <>)
	// from the scale factor of equi-join
	CHistogram *equijoin_histogram = MakeJoinHistogramNormalize
								(
								mp,
								CStatsPred::EstatscmptEq,
								rows,
								other_histogram,
								rows_other,
								&scale_factor
								);
	GPOS_DELETE(equijoin_histogram);

	CDouble cartesian_product_num_rows = rows * rows_other;

	GPOS_ASSERT(CDouble(1.0) <= scale_factor);
	CDouble equality_selectivity = 1 / scale_factor;
	CDouble inequality_selectivity = (1 - equality_selectivity);

	scale_factor = cartesian_product_num_rows;
	if (CStatistics::Epsilon < inequality_selectivity)
	{
		scale_factor = 1 / inequality_selectivity;
	}

	return scale_factor;
}

// construct new histogram by left anti semi join with another and normalize
// output histogram
CHistogram *
CHistogram::MakeLASJHistogramNormalize
	(
	CMemoryPool *mp,
	CStatsPred::EStatsCmpType stats_cmp_type,
	CDouble rows,
	const CHistogram *other_histogram,
	CDouble *scale_factor,
	BOOL DoIgnoreLASJHistComputation
	)
	const
{
	// if either histogram is not well-defined, the result is not well defined
	if (!IsWellDefined() || !other_histogram->IsWellDefined())
	{
		CHistogram *result_histogram = GPOS_NEW(mp) CHistogram(GPOS_NEW(mp) CBucketArray(mp), false /* is_well_defined */);
		(*scale_factor) = CDouble(1.0);

		return result_histogram;
	}

	if (DoIgnoreLASJHistComputation)
	{
		// TODO:  04/14/2012 : LASJ derivation is pretty aggressive.
		// simply return a copy of the histogram with a scale factor corresponding to default selectivity.
		CHistogram *result_histogram = CopyHistogram(mp);
		*scale_factor = CDouble(1.0) / CHistogram::DefaultSelectivity;
		GPOS_ASSERT(result_histogram->IsValid());

		return result_histogram;
	}

	CHistogram *result_histogram = MakeLASJHistogram(mp, stats_cmp_type, other_histogram);
	*scale_factor = result_histogram->NormalizeHistogram();

	if (CStatsPred::EstatscmptEq != stats_cmp_type && CStatsPred::EstatscmptINDF != stats_cmp_type)
	{
		// TODO: , June 6 2014, we currently only support join histogram computation
		// for equality and INDF predicates, we cannot accurately estimate
		// the scale factor of the other predicates
		*scale_factor = CDouble(1.0) / CHistogram::DefaultSelectivity;
	}
	*scale_factor = std::min((*scale_factor).Get(), rows.Get());
	GPOS_ASSERT(result_histogram->IsValid());

	return result_histogram;
}

// construct new histogram after applying the filter (no normalization)
CHistogram *
CHistogram::MakeHistogramFilter
	(
	CMemoryPool *mp,
	CStatsPred::EStatsCmpType stats_cmp_type,
	CPoint *point
	)
	const
{
	CHistogram *result_histogram = NULL;
	GPOS_ASSERT(IsOpSupportedForFilter(stats_cmp_type));

	switch(stats_cmp_type)
	{
		case CStatsPred::EstatscmptEq:
		{
			result_histogram = MakeHistogramEqualFilter(mp, point);
			break;
		}
		case CStatsPred::EstatscmptINDF:
		{
			result_histogram = MakeHistogramINDFFilter(mp, point);
			break;
		}
		case CStatsPred::EstatscmptL:
		case CStatsPred::EstatscmptLEq:
		{
			result_histogram = MakeHistogramLessThanOrLessThanEqualFilter(mp, stats_cmp_type, point);
			break;
		}
		case CStatsPred::EstatscmptG:
		case CStatsPred::EstatscmptGEq:
		{
			result_histogram = MakeHistogramGreaterThanOrGreaterThanEqualFilter(mp, stats_cmp_type, point);
			break;
		}
		case CStatsPred::EstatscmptNEq:
		{
			result_histogram = MakeHistogramInequalityFilter(mp, point);
			break;
		}
		case CStatsPred::EstatscmptIDF:
		{
			result_histogram = MakeHistogramIDFFilter(mp, point);
			break;
		}
		default:
		{
			GPOS_RTL_ASSERT_MSG(false, "Not supported. Must not be called.");
			break;
		}
	}
	return result_histogram;
}

// construct new histogram by joining with another histogram, no normalization
CHistogram *
CHistogram::MakeJoinHistogram
	(
	CMemoryPool *mp,
	CStatsPred::EStatsCmpType stats_cmp_type,
	const CHistogram *histogram
	)
	const
{
	GPOS_ASSERT(JoinPredCmpTypeIsSupported(stats_cmp_type));

	if (CStatsPred::EstatscmptEq == stats_cmp_type)
	{
		return MakeJoinHistogramEqualityFilter(mp, histogram);
	}

	if (CStatsPred::EstatscmptINDF == stats_cmp_type)
	{
		return MakeJoinHistogramINDFFilter(mp, histogram);
	}

	// TODO:  Feb 24 2014, We currently only support creation of histogram for equi join
	return GPOS_NEW(mp) CHistogram(GPOS_NEW(mp) CBucketArray(mp), false /* is_well_defined */);
}

// construct new histogram by LASJ with another histogram, no normalization
CHistogram *
CHistogram::MakeLASJHistogram
	(
	CMemoryPool *mp,
	CStatsPred::EStatsCmpType stats_cmp_type,
	const CHistogram *histogram
	)
	const
{
	GPOS_ASSERT(NULL != histogram);

	if (CStatsPred::EstatscmptEq != stats_cmp_type && CStatsPred::EstatscmptINDF != stats_cmp_type)
	{
		// TODO: , June 6 2014, we currently only support join histogram computation
		// for equality and INDF predicates, we return the original histogram
		return CopyHistogram(mp);
	}

	CBucketArray *new_buckets = GPOS_NEW(mp) CBucketArray(mp);

	CBucket *lower_split_bucket = NULL;
	CBucket *upper_split_bucket = NULL;

	ULONG idx1 = 0; // index into this histogram
	ULONG idx2 = 0; // index into other histogram

	CBucket *candidate_bucket = NULL;

	const ULONG buckets1 = Buckets();
	const ULONG buckets2 = histogram->Buckets();

	while (idx1 < buckets1 && idx2 < buckets2)
	{
		// bucket from other histogram
		CBucket *bucket2 = (*histogram->m_histogram_buckets) [idx2];

		// yet to choose a candidate
		GPOS_ASSERT(NULL == candidate_bucket);

		if (NULL != upper_split_bucket)
		{
			candidate_bucket = upper_split_bucket;
		}
		else
		{
			candidate_bucket = (*m_histogram_buckets)[idx1]->MakeBucketCopy(mp); // candidate bucket in result histogram
			idx1++;
		}

		lower_split_bucket = NULL;
		upper_split_bucket = NULL;

		candidate_bucket->Difference(mp, bucket2, &lower_split_bucket, &upper_split_bucket);

		if (NULL != lower_split_bucket)
		{
			new_buckets->Append(lower_split_bucket);
		}

		// need to find a new candidate
		GPOS_DELETE(candidate_bucket);
		candidate_bucket = NULL;

		idx2++;
	}

	candidate_bucket = upper_split_bucket;

	// if we looked at all the buckets from the other histogram, then add remaining buckets from
	// this histogram
	if (idx2 == buckets2)
	{
		// candidate is part of the result
		if (NULL != candidate_bucket)
		{
			new_buckets->Append(candidate_bucket);
		}

		CStatisticsUtils::AddRemainingBuckets(mp, m_histogram_buckets, new_buckets, &idx1);
	}
	else
	{
		GPOS_DELETE(candidate_bucket);
	}

	CDouble null_freq = CLeftAntiSemiJoinStatsProcessor::NullFreqLASJ(stats_cmp_type, this, histogram);

	return GPOS_NEW(mp) CHistogram(new_buckets, true /*is_well_defined*/, null_freq, m_distinct_remaining, m_freq_remaining);
}

// scales frequencies on histogram so that they add up to 1.0.
// Returns the scaling factor that was employed. Should not be called on empty histogram.
CDouble
CHistogram::NormalizeHistogram()
{
	// trivially normalized
	if (Buckets() == 0 && CStatistics::Epsilon > m_null_freq && CStatistics::Epsilon > m_distinct_remaining)
	{
		return CDouble(GPOS_FP_ABS_MAX);
	}

	CDouble scale_factor = std::max(DOUBLE(1.0), (CDouble(1.0) / GetFrequency()).Get());

	for (ULONG ul = 0; ul < m_histogram_buckets->Size(); ul++)
	{
		CBucket *bucket = (*m_histogram_buckets)[ul];
		bucket->SetFrequency(bucket->GetFrequency() * scale_factor);
	}

	m_null_freq = m_null_freq * scale_factor;

	CDouble freq_remaining = std::min((m_freq_remaining * scale_factor).Get(), DOUBLE(1.0));
	if (CStatistics::Epsilon > m_distinct_remaining)
	{
		freq_remaining = 0.0;
	}
	m_freq_remaining = freq_remaining;

	return scale_factor;
}

// deep copy of histogram
CHistogram *
CHistogram::CopyHistogram
	(
	CMemoryPool *mp
	)
	const
{
	CBucketArray *buckets = GPOS_NEW(mp) CBucketArray(mp);
	for (ULONG ul = 0; ul < m_histogram_buckets->Size(); ul++)
	{
		CBucket *bucket = (*m_histogram_buckets)[ul];
		buckets->Append(bucket->MakeBucketCopy(mp));
	}

	CHistogram *histogram_copy = GPOS_NEW(mp) CHistogram(buckets, m_is_well_defined, m_null_freq, m_distinct_remaining, m_freq_remaining);
	if (WereNDVsScaled())
	{
		histogram_copy->SetNDVScaled();
	}

	return histogram_copy;
}

BOOL
CHistogram::IsOpSupportedForTextFilter
	(
	CStatsPred::EStatsCmpType stats_cmp_type
	)
{
	// is the scalar comparison type one of =, <>
	switch (stats_cmp_type)
	{
		case CStatsPred::EstatscmptEq:
		case CStatsPred::EstatscmptNEq:
			return true;
		default:
			return false;
	}
}



// is statistics comparison type supported for filter?
BOOL
CHistogram::IsOpSupportedForFilter
	(
	CStatsPred::EStatsCmpType stats_cmp_type
	)
{
	// is the scalar comparison type one of <, <=, =, >=, >, <>, IDF, INDF
	switch (stats_cmp_type)
	{
		case CStatsPred::EstatscmptL:
		case CStatsPred::EstatscmptLEq:
		case CStatsPred::EstatscmptEq:
		case CStatsPred::EstatscmptGEq:
		case CStatsPred::EstatscmptG:
		case CStatsPred::EstatscmptNEq:
		case CStatsPred::EstatscmptIDF:
		case CStatsPred::EstatscmptINDF:
			return true;
		default:
			return false;
	}
}

// is comparison type supported for join?
BOOL
CHistogram::JoinPredCmpTypeIsSupported
	(
	CStatsPred::EStatsCmpType stats_cmp_type
	)
{
	return (CStatsPred::EstatscmptOther != stats_cmp_type);
}

// construct a new histogram with equality join
CHistogram *
CHistogram::MakeJoinHistogramEqualityFilter
	(
	CMemoryPool *mp,
	const CHistogram *histogram
	)
	const
{
	ULONG idx1 = 0; // index on buckets from this histogram
	ULONG idx2 = 0; // index on buckets from other histogram

	const ULONG buckets1 = Buckets();
	const ULONG buckets2 = histogram->Buckets();

	CDouble hist1_buckets_freq(0.0);
	CDouble hist2_buckets_freq(0.0);

	CDouble distinct_remaining(0.0);
	CDouble freq_remaining(0.0);

	BOOL NDVBasedJoinCardEstimation1 = DoNDVBasedCardEstimation(this);
	BOOL NDVBasedJoinCardEstimation2 = DoNDVBasedCardEstimation(histogram);

	if (NDVBasedJoinCardEstimation1 || NDVBasedJoinCardEstimation2)
	{
		return MakeNDVBasedJoinHistogramEqualityFilter(mp, histogram);
	}

	CBucketArray *join_buckets = GPOS_NEW(mp) CBucketArray(mp);
	while (idx1 < buckets1 && idx2 < buckets2)
	{
		CBucket *bucket1 = (*m_histogram_buckets)[idx1];
		CBucket *bucket2 = (*histogram->m_histogram_buckets)[idx2];

		if (bucket1->Intersects(bucket2))
		{
			CDouble freq_intersect1(0.0);
			CDouble freq_intersect2(0.0);

			CBucket *new_bucket = bucket1->MakeBucketIntersect(mp, bucket2, &freq_intersect1, &freq_intersect2);
			join_buckets->Append(new_bucket);

			hist1_buckets_freq = hist1_buckets_freq + freq_intersect1;
			hist2_buckets_freq = hist2_buckets_freq + freq_intersect2;

			INT res = CBucket::CompareUpperBounds(bucket1, bucket2);
			if (0 == res)
			{
				// both ubs are equal
				idx1++;
				idx2++;
			}
			else if (1 > res)
			{
				// bucket1's ub is smaller than that of the ub of bucket2
				idx1++;
			}
			else
			{
				idx2++;
			}
		}
		else if (bucket1->IsBefore(bucket2))
		{
			// buckets do not intersect there one bucket is before the other
			idx1++;
		}
		else
		{
			GPOS_ASSERT(bucket2->IsBefore(bucket1));
			idx2++;
		}
	}

	ComputeJoinNDVRemainInfo
		(
		this,
		histogram,
		join_buckets,
		hist1_buckets_freq,
		hist2_buckets_freq,
		&distinct_remaining,
		&freq_remaining
		);

	return GPOS_NEW(mp) CHistogram(join_buckets, true /*is_well_defined*/, 0.0 /*null_freq*/, distinct_remaining, freq_remaining);
}

// construct a new histogram for NDV based cardinality estimation
CHistogram *
CHistogram::MakeNDVBasedJoinHistogramEqualityFilter
(
 CMemoryPool *mp,
 const CHistogram *histogram
 )
const
{
	CDouble distinct_remaining(0.0);
	CDouble freq_remaining(0.0);
	CBucketArray *join_buckets = GPOS_NEW(mp) CBucketArray(mp);
	
	// compute the number of non-null distinct values in the input histograms
	CDouble NDVs1 = this->GetNumDistinct();
	CDouble freq_remain1 = this->GetFrequency();
	CDouble null_freq1 = this->GetNullFreq();
	if (CStatistics::Epsilon < null_freq1)
	{
		NDVs1 = std::max(CDouble(0.0), (NDVs1 - 1.0));
		freq_remain1 = freq_remain1 - null_freq1;
	}
	
	CDouble NDVs2 = histogram->GetNumDistinct();
	CDouble freq_remain2 = histogram->GetFrequency();
	CDouble null_freq2 = histogram->GetNullFreq();
	if (CStatistics::Epsilon < histogram->GetNullFreq())
	{
		NDVs2 = std::max(CDouble(0.0), (NDVs2 - 1.0));
		freq_remain2 = freq_remain2 - null_freq2;
	}
	
	// the estimated number of distinct value is the minimum of the non-null
	// distinct values of the two inputs.
	distinct_remaining = std::min(NDVs1, NDVs2);
	
	// the frequency of a tuple in this histogram (with frequency freq_remain1) joining with
	// a tuple in another relation (with frequency freq_remain2) is a product of the two frequencies divided by
	// the maximum NDV of the two inputs
	
	// Example: consider two relations A and B with 10 tuples each. Let both relations have no nulls.
	// Let A have 2 distinct values, while B have 5 distinct values. Under uniform distribution of NDVs
	// for statistics purposes we can view A = (1,2,1,2,1,2,1,2,1,2) and B = (1,2,3,4,5,1,2,3,4,5)
	// Join Cardinality is 20, with frequency of the join tuple being 0.2 (since cartesian product is 100).
	// freq_remain1 = freq_remain2 = 1, and std::max(NDVs1, NDVs2) = 5. Therefore freq_remaining = 1/5 = 0.2
	if (CStatistics::Epsilon < distinct_remaining)
	{
		freq_remaining = freq_remain1 * freq_remain2 / std::max(NDVs1, NDVs2);
	}

	return GPOS_NEW(mp) CHistogram
	(
	 join_buckets,
	 true /*is_well_defined*/,
	 0.0 /*null_freq*/,
	 distinct_remaining,
	 freq_remaining
	 );
}

// construct a new histogram for an INDF join predicate
CHistogram *
CHistogram::MakeJoinHistogramINDFFilter
	(
	CMemoryPool *mp,
	const CHistogram *histogram
	)
	const
{
	CHistogram *join_histogram = MakeJoinHistogramEqualityFilter(mp, histogram);

	// compute the null frequency is the same means as how we perform equi-join
	// see CBuckets::MakeBucketIntersect for details
	CDouble null_freq = GetNullFreq() * histogram->GetNullFreq() *
			DOUBLE(1.0) / std::max(GetNumDistinct(), histogram->GetNumDistinct());
	join_histogram->SetNullFrequency(null_freq);

	return join_histogram;
}

// check if we can compute NDVRemain for JOIN histogram for the given input histograms
BOOL
CHistogram::CanComputeJoinNDVRemain
	(
	const CHistogram *histogram1,
	const CHistogram *histogram2
	)
{
	GPOS_ASSERT(NULL != histogram1);
	GPOS_ASSERT(NULL != histogram2);

	BOOL has_buckets1 = (0 != histogram1->Buckets());
	BOOL has_buckets2 = (0 != histogram2->Buckets());
	BOOL has_distinct_remain1 = CStatistics::Epsilon < histogram1->GetDistinctRemain();
	BOOL has_distinct_remain2 = CStatistics::Epsilon < histogram2->GetDistinctRemain();

	if (!has_distinct_remain1 && !has_distinct_remain2)
	{
		// no remaining tuples information hence no need compute NDVRemain for join histogram
		return false;
	}

	if ((has_distinct_remain1 || has_buckets1) && (has_distinct_remain2 || has_buckets2))
	{
		//
		return true;
	}

	// insufficient information to compute JoinNDVRemain, we need:
	// 1. for each input either have a histograms or NDVRemain
	// 2. at least one inputs must have NDVRemain
	return false;
}

// compute the effects of the NDV and frequency of the tuples not captured by the histogram
void
CHistogram::ComputeJoinNDVRemainInfo
	(
	const CHistogram *histogram1,
	const CHistogram *histogram2,
									 CBucketArray *join_buckets,
	CDouble hist1_buckets_freq,
	CDouble hist2_buckets_freq,
	CDouble *result_distinct_remain,
	CDouble *result_freq_remain
	)
{
	GPOS_ASSERT(NULL != histogram1);
	GPOS_ASSERT(NULL != histogram2);
	GPOS_ASSERT(NULL != join_buckets);

	CDouble join_NDVs = CStatisticsUtils::GetNumDistinct(join_buckets);
	CDouble join_freq = CStatisticsUtils::GetFrequency(join_buckets);

	*result_distinct_remain = 0.0;
	*result_freq_remain = 0.0;

	if (!CanComputeJoinNDVRemain(histogram1, histogram2))
	{
		return;
	}

	if (CStatistics::Epsilon >= (1 - join_freq))
	{
		return;
	}

	// we compute the following information for the resulting join histogram
	// 1. NDVRemain and 2. Frequency of the NDVRemain

	// compute the number of non-null distinct values in each input histogram
	CDouble num_distinct1 = histogram1->GetNumDistinct();
	CDouble num_distinct_non_null1 = num_distinct1;
	if (CStatistics::Epsilon < histogram1->GetNullFreq())
	{
		num_distinct_non_null1 = num_distinct_non_null1 - 1.0;
	}

	CDouble num_distinct2 = histogram2->GetNumDistinct();
	CDouble num_distinct_non_null2 = num_distinct2;
	if (CStatistics::Epsilon < histogram2->GetNullFreq())
	{
		num_distinct_non_null2 = num_distinct_non_null2 - 1.0;
	}

	// the estimated final number of distinct value for the join is the minimum of the non-null
	// distinct values of the two inputs. This follows the principle of used to estimate join
	// scaling factor -- defined as the maximum NDV of the two inputs
	CDouble final_join_NDVs = std::min(num_distinct_non_null1, num_distinct_non_null2);
	CDouble remaining_join_NDVs = std::max(final_join_NDVs - join_NDVs, CDouble(0.0));

	// compute the frequency of the non-joining buckets in each input histogram
	CDouble freq_buckets1 =  CStatisticsUtils::GetFrequency(histogram1->ParseDXLToBucketsArray());
	CDouble freq_buckets2 =  CStatisticsUtils::GetFrequency(histogram2->ParseDXLToBucketsArray());
	CDouble freq_non_join_buckets1 = std::max(CDouble(0), (freq_buckets1 - hist1_buckets_freq));
	CDouble freq_non_join_buckets2 = std::max(CDouble(0), (freq_buckets2 - hist2_buckets_freq));

	// compute the NDV of the non-joining buckets
	CDouble NDVs_non_join_buckets1 = CStatisticsUtils::GetNumDistinct(histogram1->ParseDXLToBucketsArray()) - join_NDVs;
	CDouble NDVs_non_join_buckets2 = CStatisticsUtils::GetNumDistinct(histogram2->ParseDXLToBucketsArray()) - join_NDVs;

	CDouble freq_remain1 = histogram1->GetFreqRemain();
	CDouble freq_remain2 = histogram2->GetFreqRemain();

	// the frequency of the NDVRemain of the join is made of:
	// 1. frequency of the NDV as a result of joining NDVRemain1 and NDVRemain2
	// 2. frequency of the NDV as a results of joining NDVRemain1 and NDVs_non_join_buckets2
	// 3. frequency of the NDV as a results of joining NDVRemain2 and NDVs_non_join_buckets1

	CDouble freq_remain_join = freq_remain1 * freq_remain2 / std::max(histogram1->GetDistinctRemain(), histogram2->GetDistinctRemain());
	freq_remain_join = freq_remain_join + freq_remain1 * freq_non_join_buckets2 / std::max(num_distinct_non_null1, NDVs_non_join_buckets2);
	freq_remain_join = freq_remain_join + freq_remain2 * freq_non_join_buckets1 / std::max(num_distinct_non_null2, NDVs_non_join_buckets1);

	*result_distinct_remain = remaining_join_NDVs;
	*result_freq_remain = freq_remain_join;
}

// construct new histogram by removing duplicates and normalize output histogram
CHistogram *
CHistogram::MakeGroupByHistogramNormalize
	(
	CMemoryPool *mp,
	CDouble, // rows,
	CDouble *result_distinct_values
	)
	const
{
	// if either histogram is not well-defined, the result is not well defined
	if (!IsWellDefined())
	{
		CHistogram *result_histogram = GPOS_NEW(mp) CHistogram(GPOS_NEW(mp) CBucketArray(mp), false /* is_well_defined */);
		*result_distinct_values = MinDistinct / CHistogram::DefaultSelectivity;
		return result_histogram;
	}

	// total number of distinct values
	CDouble distinct = GetNumDistinct();

	CBucketArray *new_buckets = GPOS_NEW(mp) CBucketArray(mp);

	const ULONG num_of_buckets = m_histogram_buckets->Size();
	for (ULONG ul = 0; ul < num_of_buckets; ul++)
	{
		CBucket *bucket = (*m_histogram_buckets)[ul];
		CPoint *lower_bound = bucket->GetLowerBound();
		CPoint *upper_bound = bucket->GetUpperBound();
		lower_bound->AddRef();
		upper_bound->AddRef();

		BOOL is_upper_closed = false;
		if (lower_bound->Equals(upper_bound))
		{
			is_upper_closed = true;
		}
		CBucket *new_bucket = GPOS_NEW(mp) CBucket
										(
												lower_bound,
												upper_bound,
										true /* fClosedLower */, 
										is_upper_closed,
										bucket->GetNumDistinct() / distinct,
										bucket->GetNumDistinct()
										);
		new_buckets->Append(new_bucket);
	}

	CDouble new_null_freq = CDouble(0.0);
	if (CStatistics::Epsilon < m_null_freq)
	{
		new_null_freq = std::min(CDouble(1.0), CDouble(1.0)/distinct);
	}


	CDouble freq_remaining = 0.0;
	if (CStatistics::Epsilon < m_distinct_remaining)
	{
		// TODO:  11/22/2013 - currently the NDV of a histogram could be 0 or a decimal number.
		// We may not need the capping if later distinct is lower bounded at 1.0 for non-empty histogram
		freq_remaining = std::min(CDouble(1.0), m_distinct_remaining/distinct);
	}

	CHistogram *result_histogram = GPOS_NEW(mp) CHistogram(new_buckets, true /*is_well_defined*/, new_null_freq, m_distinct_remaining, freq_remaining);
	*result_distinct_values = result_histogram->GetNumDistinct();

	return result_histogram;
}

// construct new histogram based on union all operation
CHistogram *
CHistogram::MakeUnionAllHistogramNormalize
	(
	CMemoryPool *mp,
	CDouble rows,
	const CHistogram *histogram,
	CDouble rows_other
	)
	const
{
	CBucketArray *new_buckets = GPOS_NEW(mp) CBucketArray(mp);
	ULONG idx1 = 0; // index on buckets from this histogram
	ULONG idx2 = 0; // index on buckets from other histogram
	CBucket *bucket1 = (*this) [idx1];
	CBucket *bucket2 = (*histogram) [idx2];
	CDouble rows_new = (rows_other + rows);

	// flags to determine if the buckets where residue of the bucket-merge operation
	BOOL bucket1_is_residual = false;
	BOOL bucket2_is_residual = false;

	while (NULL != bucket1 && NULL != bucket2)
	{
		if (bucket1->IsBefore(bucket2))
		{
			new_buckets->Append(bucket1->MakeBucketUpdateFrequency(mp, rows, rows_new));
			CleanupResidualBucket(bucket1, bucket1_is_residual);
			idx1++;
			bucket1 = (*this) [idx1];
			bucket1_is_residual = false;
		}
		else if (bucket2->IsBefore(bucket1))
		{
			new_buckets->Append(bucket2->MakeBucketUpdateFrequency(mp, rows_other, rows_new));
			CleanupResidualBucket(bucket2, bucket2_is_residual);
			idx2++;
			bucket2 = (*histogram)[idx2];
			bucket2_is_residual = false;
		}
		else
		{
			GPOS_ASSERT(bucket1->Intersects(bucket2));
			CBucket *bucket1_new = NULL;
			CBucket *bucket2_new = NULL;
			CBucket *merge_bucket = bucket1->MakeBucketMerged(mp, bucket2, rows, rows_other, &bucket1_new, &bucket2_new);
			new_buckets->Append(merge_bucket);

			GPOS_ASSERT(NULL == bucket1_new || NULL == bucket2_new);
			CleanupResidualBucket(bucket1, bucket1_is_residual);
			CleanupResidualBucket(bucket2, bucket2_is_residual);

			bucket1 = GetNextBucket(this, bucket1_new, &bucket1_is_residual, &idx1);
			bucket2 = GetNextBucket(histogram, bucket2_new, &bucket2_is_residual, &idx2);
		}
	}

	const ULONG buckets1 = Buckets();
	const ULONG buckets2 = histogram->Buckets();

	GPOS_ASSERT_IFF(NULL == bucket1, idx1 == buckets1);
	GPOS_ASSERT_IFF(NULL == bucket2, idx2 == buckets2);

	idx1 = AddResidualUnionAllBucket(mp, new_buckets, bucket1, rows_other, rows_new, bucket1_is_residual, idx1);
	idx2 = AddResidualUnionAllBucket(mp, new_buckets, bucket2, rows, rows_new, bucket2_is_residual, idx2);

	CleanupResidualBucket(bucket1, bucket1_is_residual);
	CleanupResidualBucket(bucket2, bucket2_is_residual);

	// add any leftover buckets from other histogram
	AddBuckets(mp, histogram->m_histogram_buckets, new_buckets, rows_other, rows_new, idx2, buckets2);

	// add any leftover buckets from this histogram
	AddBuckets(mp, m_histogram_buckets, new_buckets, rows, rows_new, idx1, buckets1);

	CDouble new_null_freq = (m_null_freq * rows + histogram->m_null_freq * rows_other) / rows_new;

	CDouble distinct_remaining = std::max(m_distinct_remaining, histogram->m_distinct_remaining);
	CDouble freq_remaining = (m_freq_remaining * rows + histogram->m_freq_remaining * rows_other) / rows_new;

	CHistogram *result_histogram = GPOS_NEW(mp) CHistogram(new_buckets, true /*is_well_defined*/, new_null_freq, distinct_remaining, freq_remaining);
	(void) result_histogram->NormalizeHistogram();

	return result_histogram;
}

// add residual bucket in the union all operation to the array of buckets in the histogram
ULONG
CHistogram::AddResidualUnionAllBucket
	(
	CMemoryPool *mp,
	CBucketArray *histogram_buckets,
	CBucket *bucket,
	CDouble rows_old,
	CDouble rows_new,
	BOOL bucket_is_residual,
	ULONG index
	)
	const
{
	GPOS_ASSERT(NULL != histogram_buckets);

	if (bucket_is_residual)
	{
		histogram_buckets->Append(bucket->MakeBucketUpdateFrequency(mp, rows_old, rows_new));
		return index + 1;
	}

	return index;
}

// add buckets from one array to another
void
CHistogram::AddBuckets
	(
	CMemoryPool *mp,
	CBucketArray *src_buckets,
	CBucketArray *dest_buckets,
	CDouble rows_old,
	CDouble rows_new,
	ULONG begin,
	ULONG end
	)
{
	GPOS_ASSERT(NULL != src_buckets);
	GPOS_ASSERT(NULL != dest_buckets);
	GPOS_ASSERT(begin <= end);
	GPOS_ASSERT(end <= src_buckets->Size());

	for (ULONG ul = begin; ul < end; ul++)
	{
		dest_buckets->Append(((*src_buckets)[ul])->MakeBucketUpdateFrequency(mp, rows_old, rows_new));
	}
}

// cleanup residual buckets
void
CHistogram::CleanupResidualBucket
	(
	CBucket *bucket,
	BOOL bucket_is_residual
	)
	const
{
	if (NULL != bucket && bucket_is_residual)
	{
		GPOS_DELETE(bucket);
		bucket = NULL;
	}
}

// get the next bucket for union / union all
CBucket *
CHistogram::GetNextBucket
	(
	const CHistogram *histogram,
	CBucket *new_bucket,
	BOOL *result_bucket_is_residual,
	ULONG *current_bucket_index
	)
	const
{
	GPOS_ASSERT(NULL != histogram);
	if (NULL != new_bucket)
	{
		*result_bucket_is_residual = true;
		return new_bucket;
	}

	*current_bucket_index = *current_bucket_index  + 1;
	*result_bucket_is_residual = false;

	return (*histogram) [*current_bucket_index];
}

//	construct new histogram based on union operation
CHistogram *
CHistogram::MakeUnionHistogramNormalize
	(
	CMemoryPool *mp,
	CDouble rows,
	const CHistogram *other_histogram,
	CDouble rows_other,
	CDouble *num_output_rows
	)
	const
{
	GPOS_ASSERT(NULL != other_histogram);

	ULONG idx1 = 0; // index on buckets from this histogram
	ULONG idx2 = 0; // index on buckets from other histogram
	CBucket *bucket1 = (*this) [idx1];
	CBucket *bucket2 = (*other_histogram) [idx2];

	// flags to determine if the buckets where residue of the bucket-merge operation
	BOOL bucket1_is_residual = false;
	BOOL bucket2_is_residual = false;

	// array of buckets in the resulting histogram
	CBucketArray *histogram_buckets = GPOS_NEW(mp) CBucketArray(mp);

	// number of tuples in each bucket of the resulting histogram
	CDoubleArray *num_tuples_per_bucket = GPOS_NEW(mp) CDoubleArray(mp);

	CDouble cumulative_num_rows(0.0);
	while (NULL != bucket1 && NULL != bucket2)
	{
		if (bucket1->IsBefore(bucket2))
		{
			histogram_buckets->Append(bucket1->MakeBucketCopy(mp));
			num_tuples_per_bucket->Append(GPOS_NEW(mp) CDouble(bucket1->GetFrequency() * rows));
			CleanupResidualBucket(bucket1, bucket1_is_residual);
			idx1++;
			bucket1 = (*this) [idx1];
			bucket1_is_residual = false;
		}
		else if (bucket2->IsBefore(bucket1))
		{
			histogram_buckets->Append(bucket2->MakeBucketCopy(mp));
			num_tuples_per_bucket->Append(GPOS_NEW(mp) CDouble(bucket2->GetFrequency() * rows_other));
			CleanupResidualBucket(bucket2, bucket2_is_residual);
			idx2++;
			bucket2 = (*other_histogram) [idx2];
			bucket2_is_residual = false;
		}
		else
		{
			GPOS_ASSERT(bucket1->Intersects(bucket2));
			CBucket *bucket1_new = NULL;
			CBucket *bucket2_new = NULL;
			CBucket *merge_bucket = NULL;

			merge_bucket = bucket1->MakeBucketMerged
									(
									mp,
									bucket2,
									rows,
									rows_other,
									&bucket1_new,
									&bucket2_new,
									false /* is_union_all */
									);

			// add the estimated number of rows in the merged bucket
			num_tuples_per_bucket->Append(GPOS_NEW(mp) CDouble(merge_bucket->GetFrequency() * rows));
			histogram_buckets->Append(merge_bucket);

			GPOS_ASSERT(NULL == bucket1_new || NULL == bucket2_new);
			CleanupResidualBucket(bucket1, bucket1_is_residual);
			CleanupResidualBucket(bucket2, bucket2_is_residual);

			bucket1 = GetNextBucket(this, bucket1_new, &bucket1_is_residual, &idx1);
			bucket2 = GetNextBucket(other_histogram, bucket2_new, &bucket2_is_residual, &idx2);
		}
	}

	const ULONG buckets1 = Buckets();
	const ULONG buckets2 = other_histogram->Buckets();

	GPOS_ASSERT_IFF(NULL == bucket1, idx1 == buckets1);
	GPOS_ASSERT_IFF(NULL == bucket2, idx2 == buckets2);

	idx1 = AddResidualUnionBucket(mp, histogram_buckets, bucket1, rows_other, bucket1_is_residual, idx1, num_tuples_per_bucket);
	idx2 = AddResidualUnionBucket(mp, histogram_buckets, bucket2, rows, bucket2_is_residual, idx2, num_tuples_per_bucket);

	CleanupResidualBucket(bucket1, bucket1_is_residual);
	CleanupResidualBucket(bucket2, bucket2_is_residual);

	// add any leftover buckets from other histogram
	AddBuckets(mp, other_histogram->m_histogram_buckets, histogram_buckets, rows_other, num_tuples_per_bucket, idx2, buckets2);

	// add any leftover buckets from this histogram
	AddBuckets(mp, m_histogram_buckets, histogram_buckets, rows, num_tuples_per_bucket, idx1, buckets1);

	// compute the total number of null values from both histograms
	CDouble num_null_rows = std::max( (this->GetNullFreq() * rows), (other_histogram->GetNullFreq() * rows_other));

	// compute the total number of distinct values (NDV) that are not captured by the buckets in both the histograms
	CDouble num_NDV_remain = std::max(m_distinct_remaining, other_histogram->GetDistinctRemain());

	// compute the total number of rows having distinct values not captured by the buckets in both the histograms
	CDouble NDV_remain_num_rows = std::max( (this->GetFreqRemain() * rows), (other_histogram->GetFreqRemain() * rows_other));

	CHistogram *result_histogram = MakeHistogramUpdateFreq
									(
									mp,
									histogram_buckets,
									num_tuples_per_bucket,
									num_output_rows,
									num_null_rows,
									num_NDV_remain,
									NDV_remain_num_rows
									);
	// clean up
	histogram_buckets->Release();
	num_tuples_per_bucket->Release();

	return result_histogram;
}

// create a new histogram with updated bucket frequency
CHistogram *
CHistogram::MakeHistogramUpdateFreq
	(
	CMemoryPool *mp,
	CBucketArray *histogram_buckets,
	CDoubleArray *dest_bucket_freqs,
	CDouble *result_num_rows_output,
	CDouble num_null_rows,
	CDouble num_NDV_remain,
	CDouble NDV_remain_num_rows
	)
	const
{
	GPOS_ASSERT(NULL != histogram_buckets);
	GPOS_ASSERT(NULL != dest_bucket_freqs);

	const ULONG length = dest_bucket_freqs->Size();
	GPOS_ASSERT(length == histogram_buckets->Size());

	CDouble cumulative_num_rows = num_null_rows + NDV_remain_num_rows;
	for (ULONG ul = 0; ul < length; ul++)
	{
		CDouble rows = *(*dest_bucket_freqs)[ul];
		cumulative_num_rows = cumulative_num_rows + rows;
	}

	*result_num_rows_output = std::max(CStatistics::MinRows, cumulative_num_rows);

	CBucketArray *new_buckets = GPOS_NEW(mp) CBucketArray(mp);
	for (ULONG ul = 0; ul < length; ul++)
	{
		CDouble rows = *(*dest_bucket_freqs)[ul];
		CBucket *bucket = (*histogram_buckets)[ul];

		// reuse the points
		bucket->GetLowerBound()->AddRef();
		bucket->GetUpperBound()->AddRef();

		CDouble frequency = rows / *result_num_rows_output;

		CBucket *new_bucket = GPOS_NEW(mp) CBucket
										(
										bucket->GetLowerBound(),
										bucket->GetUpperBound(),
										bucket->IsLowerClosed(),
										bucket->IsUpperClosed(),
										frequency,
										bucket->GetNumDistinct()
										);

		new_buckets->Append(new_bucket);
	}

	CDouble null_freq = num_null_rows / *result_num_rows_output ;
	CDouble NDV_remain_freq =  NDV_remain_num_rows / *result_num_rows_output ;

	return GPOS_NEW(mp) CHistogram
							(
							new_buckets,
							true /* is_well_defined */,
							null_freq,
							num_NDV_remain,
							NDV_remain_freq,
							false /* is_col_stats_missing */
							);
}

// add residual bucket in an union operation to the array of buckets in the histogram
ULONG
CHistogram::AddResidualUnionBucket
	(
	CMemoryPool *mp,
	CBucketArray *histogram_buckets,
	CBucket *bucket,
	CDouble rows,
	BOOL bucket_is_residual,
	ULONG index,
	CDoubleArray *dest_bucket_freqs
	)
	const
{
	GPOS_ASSERT(NULL != histogram_buckets);
	GPOS_ASSERT(NULL != dest_bucket_freqs);

	if (!bucket_is_residual)
	{
		return index;
	}

	CBucket *new_bucket = bucket->MakeBucketCopy(mp);
	histogram_buckets->Append(new_bucket);
	dest_bucket_freqs->Append(GPOS_NEW(mp) CDouble(new_bucket->GetFrequency() * rows));

	return index + 1;
}

// add buckets from one array to another
void
CHistogram::AddBuckets
	(
	CMemoryPool *mp,
	CBucketArray *src_buckets,
	CBucketArray *dest_buckets,
	CDouble rows,
	CDoubleArray *dest_bucket_freqs,
	ULONG begin,
	ULONG end
	)
{
	GPOS_ASSERT(NULL != src_buckets);
	GPOS_ASSERT(NULL != dest_buckets);
	GPOS_ASSERT(begin <= end);
	GPOS_ASSERT(end <= src_buckets->Size());

	for (ULONG ul = begin; ul < end; ul++)
	{
		CBucket *new_bucket = ((*src_buckets)[ul])->MakeBucketCopy(mp);
		dest_buckets->Append(new_bucket);
		dest_bucket_freqs->Append(GPOS_NEW(mp) CDouble(new_bucket->GetFrequency() * rows));
	}
}

// accessor for n-th bucket. Returns NULL if outside bounds
CBucket *
CHistogram::operator []
	(
	ULONG pos
	)
	const
{
	if (pos < Buckets())
	{
		return (*m_histogram_buckets) [pos];
	}
	return NULL;
}

// translate the histogram into a the dxl derived column statistics
CDXLStatsDerivedColumn *
CHistogram::TranslateToDXLDerivedColumnStats
	(
	CMemoryPool *mp,
	CMDAccessor *md_accessor,
	ULONG colid,
	CDouble width
	)
	const
{
	CDXLBucketArray *dxl_stats_bucket_array = GPOS_NEW(mp) CDXLBucketArray(mp);

	const ULONG num_of_buckets = m_histogram_buckets->Size();
	for (ULONG ul = 0; ul < num_of_buckets; ul++)
	{
		CBucket *bucket = (*m_histogram_buckets)[ul];

		CDouble freq = bucket->GetFrequency();
		CDouble distinct = bucket->GetNumDistinct();

		CDXLDatum *dxl_datum_lower = bucket->GetLowerBound()->GetDatumVal(mp, md_accessor);
		CDXLDatum *dxl_datum_upper = bucket->GetUpperBound()->GetDatumVal(mp, md_accessor);

		CDXLBucket *dxl_bucket = GPOS_NEW(mp) CDXLBucket
											(
											dxl_datum_lower,
											dxl_datum_upper,
											bucket->IsLowerClosed(),
											bucket->IsUpperClosed(),
											freq,
											distinct
											);

		dxl_stats_bucket_array->Append(dxl_bucket);
	}

	return GPOS_NEW(mp) CDXLStatsDerivedColumn(colid, width, m_null_freq, m_distinct_remaining, m_freq_remaining, dxl_stats_bucket_array);
}

// randomly pick a bucket index based on bucket frequency values
ULONG
CHistogram::GetRandomBucketIndex
	(
	ULONG *seed
	)
	const
{
	const ULONG size = m_histogram_buckets->Size();
	GPOS_ASSERT(0 < size);

	DOUBLE rand_val = ((DOUBLE) clib::Rand(seed)) / RAND_MAX;
	CDouble accumulated_freq = 0;
	for (ULONG ul = 0; ul < size - 1; ul++)
	{
		CBucket *bucket = (*m_histogram_buckets)[ul];
		accumulated_freq = accumulated_freq + bucket->GetFrequency();

		// we compare generated random value with accumulated frequency,
		// this will result in picking a bucket based on its frequency,
		// example: bucket freq {0.1, 0.3, 0.6}
		//			random value in [0,0.1] --> pick bucket 1
		//			random value in [0.1,0.4] --> pick bucket 2
		//			random value in [0.4,1.0] --> pick bucket 3

		if (rand_val <= accumulated_freq)
		{
			return ul;
		}
	}

	return size - 1;
}

// estimate data skew by sampling histogram buckets,
// the estimate value is >= 1.0, where 1.0 indicates no skew
//
// skew is estimated by computing the second and third moments of
// sample distribution: for a sample of size n, where x_bar is sample mean,
// skew is estimated as (m_3/m_2^(1.5)), where m_2 = 1/n Sum ((x -x_bar)^2), and
// m_3 = 1/n Sum ((x -x_bar)^3)
//
// since we use skew as multiplicative factor in cost model, this function
// returns (1.0 + |skew estimate|)
void
CHistogram::ComputeSkew()
{
	m_skew_was_measured = true;

	if (!IsNormalized() || 0 == m_histogram_buckets->Size() || !(*m_histogram_buckets)[0]->CanSample())
	{
		return;
	}

	// generate randomization seed from system time
	TIMEVAL tv;
	syslib::GetTimeOfDay(&tv, NULL/*timezone*/);
	ULONG seed = CombineHashes((ULONG) tv.tv_sec, (ULONG)tv.tv_usec);

	// generate a sample from histogram data, and compute sample mean
	DOUBLE sample_mean = 0;
	DOUBLE samples[GPOPT_SKEW_SAMPLE_SIZE];
	for (ULONG ul = 0; ul < GPOPT_SKEW_SAMPLE_SIZE; ul++)
	{
		ULONG bucket_index = GetRandomBucketIndex(&seed);
		CBucket *bucket = (*m_histogram_buckets)[bucket_index];
		samples[ul] = bucket->GetSample(&seed).Get();
		sample_mean = sample_mean + samples[ul];
	}
	sample_mean = (DOUBLE) sample_mean / GPOPT_SKEW_SAMPLE_SIZE;

	// compute second and third moments of sample distribution
	DOUBLE num2 = 0;
	DOUBLE num3 = 0;
	for (ULONG ul = 0; ul < GPOPT_SKEW_SAMPLE_SIZE; ul++)
	{
		num2 = num2 + pow((samples[ul] - sample_mean) , 2.0);
		num3 = num3 + pow((samples[ul] - sample_mean) , 3.0);
	}
	DOUBLE moment2 = (DOUBLE)(num2 / GPOPT_SKEW_SAMPLE_SIZE);
	DOUBLE moment3 = (DOUBLE)(num3 / GPOPT_SKEW_SAMPLE_SIZE);

	// set skew measure
	m_skew =  CDouble(1.0 + fabs(moment3 / pow(moment2, 1.5)));
}

// create the default histogram for a given column reference
CHistogram *
CHistogram::MakeDefaultHistogram
	(
	CMemoryPool *mp,
	CColRef *col_ref,
	BOOL is_empty
	)
{
	GPOS_ASSERT(NULL != col_ref);

	if (IMDType::EtiBool == col_ref->RetrieveType()->GetDatumType() && !is_empty)
	{
		return CHistogram::MakeDefaultBoolHistogram(mp);
	}

	return GPOS_NEW(mp) CHistogram(GPOS_NEW(mp) CBucketArray(mp), false /* is_well_defined */);
}


// create the default non-empty histogram for a boolean column
CHistogram *
CHistogram::MakeDefaultBoolHistogram
	(
	CMemoryPool *mp
	)
{
	CBucketArray *buckets = GPOS_NEW(mp) CBucketArray(mp);

	// a boolean column can at most have 3 values (true, false, and NULL).
	CDouble distinct_remaining = CDouble(3.0);
	CDouble freq_remaining = CDouble(1.0);
	CDouble null_freq = CDouble(0.0);

	return GPOS_NEW(mp) CHistogram
						(
						buckets,
						true /* is_well_defined */,
						null_freq,
						distinct_remaining,
						freq_remaining,
						true /*is_col_stats_missing */
						);
}

// check if the join cardinality estimation can be done based on NDV alone
BOOL
CHistogram::DoNDVBasedCardEstimation
	(
	const CHistogram *histogram
	)
{
	GPOS_ASSERT(NULL != histogram);

	if (0 == histogram->Buckets())
	{
		// no buckets, so join cardinality estimation is based solely on NDV remain
		return true;
	}

	const IBucket *bucket = (*histogram->ParseDXLToBucketsArray())[0];

	IDatum *datum = bucket->GetLowerBound()->GetDatum();

	IMDType::ETypeInfo type_info = datum->GetDatumType();
	if (IMDType::EtiInt2 == type_info ||
		IMDType::EtiInt4 == type_info ||
		IMDType::EtiInt8 == type_info ||
		IMDType::EtiBool == type_info ||
		IMDType::EtiOid == type_info )
	{
		return false;
	}

	BOOL result = true;
	if (datum->StatsMappable() && datum->IsDatumMappableToDouble())
	{
		result = false;
	}

	return result;
}

// append given histograms to current object
void
CHistogram::AddHistograms
	(
	CMemoryPool *mp,
	UlongToHistogramMap *src_histograms,
	UlongToHistogramMap *dest_histograms
	)
{
	UlongToHistogramMapIter col_hist_mapping(src_histograms);
	while (col_hist_mapping.Advance())
	{
		ULONG colid = *(col_hist_mapping.Key());
		const CHistogram *histogram = col_hist_mapping.Value();
		CStatisticsUtils::AddHistogram(mp, colid, histogram, dest_histograms);
	}
}

// add dummy histogram buckets and column information for the array of columns
void
CHistogram::AddDummyHistogramAndWidthInfo
	(
	CMemoryPool *mp,
	CColumnFactory *col_factory,
										  UlongToHistogramMap *output_histograms,
										  UlongToDoubleMap *output_col_widths,
	const ULongPtrArray *columns,
	BOOL is_empty
	)
{
	GPOS_ASSERT(NULL != col_factory);
	GPOS_ASSERT(NULL != output_histograms);
	GPOS_ASSERT(NULL != output_col_widths);
	GPOS_ASSERT(NULL != columns);

	const ULONG count = columns->Size();
	// for computed aggregates, we're not going to be very smart right now
	for (ULONG ul = 0; ul < count; ul++)
	{
		ULONG colid = *(*columns)[ul];

		CColRef *col_ref = col_factory->LookupColRef(colid);
		GPOS_ASSERT(NULL != col_ref);

		CHistogram *histogram = CHistogram::MakeDefaultHistogram(mp, col_ref, is_empty);
		output_histograms->Insert(GPOS_NEW(mp) ULONG(colid), histogram);

		CDouble width = CStatisticsUtils::DefaultColumnWidth(col_ref->RetrieveType());
		output_col_widths->Insert(GPOS_NEW(mp) ULONG(colid), GPOS_NEW(mp) CDouble(width));
	}
}

//	add empty histogram for the columns in the input histogram
void
CHistogram::AddEmptyHistogram
	(
	CMemoryPool *mp,
	UlongToHistogramMap *output_histograms,
	UlongToHistogramMap *input_histograms
	)
{
	GPOS_ASSERT(NULL != output_histograms);
	GPOS_ASSERT(NULL != input_histograms);

	UlongToHistogramMapIter col_hist_mapping(input_histograms);
	while (col_hist_mapping.Advance())
	{
		ULONG colid = *(col_hist_mapping.Key());

		// empty histogram
		CHistogram *histogram = GPOS_NEW(mp) CHistogram(GPOS_NEW(mp) CBucketArray(mp), false /* is_well_defined */);
		output_histograms->Insert(GPOS_NEW(mp) ULONG(colid), histogram);
	}
}

BOOL
CHistogram::IsHistogramForTextRelatedTypes()
const
{
	if (m_histogram_buckets->Size() > 0)
	{
		IMDId *mdid = (*m_histogram_buckets)[0]->GetLowerBound()->GetDatum()->MDId();
		return CStatsPredUtils::IsTextRelatedType(mdid);
	}
	return false;
}
// EOF

