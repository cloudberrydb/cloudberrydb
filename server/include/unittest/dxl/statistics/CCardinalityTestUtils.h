//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal, Inc.
//
//	@filename:
//		CCardinalityTestUtils.h
//
//	@doc:
//		Utility functions used in the testing cardinality estimation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CCardinalityTestUtils_H
#define GPNAUCRATES_CCardinalityTestUtils_H

#include "naucrates/statistics/CPoint.h"
#include "naucrates/statistics/CBucket.h"
#include "naucrates/statistics/CHistogram.h"
#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CStatsPredDisj.h"


namespace gpnaucrates
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CCardinalityTestUtils
	//
	//	@doc:
	//		Static utility functions used in the testing cardinality estimation
	//
	//---------------------------------------------------------------------------
	class CCardinalityTestUtils
	{
		public:

			// create a bucket with integer bounds, and lower bound is closed
			static
			CBucket *PbucketIntegerClosedLowerBound(IMemoryPool *pmp, INT iLower, INT iUpper, CDouble, CDouble);

			// create a singleton bucket containing a boolean value
			static
			CBucket *PbucketSingletonBoolVal(IMemoryPool *pmp, BOOL fValue, CDouble dFrequency);

			// create an integer bucket with the provider upper/lower bound, frequency and NDV information
			static
			CBucket *PbucketInteger
				(
				IMemoryPool *pmp,
				INT iLower,
				INT iUpper,
				BOOL fLowerClosed,
				BOOL fUpperClosed,
				CDouble dFrequency,
				CDouble dDistinct
				);

			// helper function to generate integer histogram based on the NDV and bucket information provided
			static
			CHistogram* PhistInt4Remain
				(
				IMemoryPool *pmp,
				ULONG ulBuckets,
				CDouble dNDVPerBucket,
				BOOL fNullFreq,
				CDouble dNDVRemain
				);

			// helper function to generate an example integer histogram
			static
			CHistogram* PhistExampleInt4(IMemoryPool *pmp);

			// helper function to generate an example boolean histogram
			static
			CHistogram* PhistExampleBool(IMemoryPool *pmp);

			// helper function to generate a point from an encoded value of specific datatype
			static
			CPoint *PpointGeneric(IMemoryPool *pmp, OID oid, CWStringDynamic *pstrValueEncoded, LINT lValue);

			// helper function to generate a point of numeric datatype
			static
			CPoint *PpointNumeric(IMemoryPool *pmp, CWStringDynamic *pstrEncodedValue, CDouble dValue);

			// helper method to print statistics object
			static
			void PrintStats(IMemoryPool *pmp, const CStatistics *pstats);

			// helper method to print histogram object
			static
			void PrintHist(IMemoryPool *pmp, const char *pcPrefix, const CHistogram *phist);

			// helper method to print bucket object
			static
			void PrintBucket(IMemoryPool *pmp, const char *pcPrefix, const CBucket *pbucket);

	}; // class CCardinalityTestUtils
}

#endif // !GPNAUCRATES_CCardinalityTestUtils_H


// EOF
