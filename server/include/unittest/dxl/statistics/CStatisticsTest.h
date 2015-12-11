//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CStatisticsTest.h
//
//	@doc:
//		Test for CPoint
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatisticsTest_H
#define GPNAUCRATES_CStatisticsTest_H

#include "naucrates/statistics/CPoint.h"
#include "naucrates/statistics/CBucket.h"
#include "naucrates/statistics/CHistogram.h"
#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CStatsPredDisj.h"

// fwd declarations
namespace gpopt
{
	class CTableDescriptor;
}

namespace gpmd
{
	class IMDTypeInt4;
}

namespace gpnaucrates
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CStatisticsTest
	//
	//	@doc:
	//		Static unit tests for statistics objects
	//
	//---------------------------------------------------------------------------
	class CStatisticsTest
	{

		// shorthand for functions for generating the join predicates
		typedef DrgPstatsjoin *(FnPdrgpstatjoin)(IMemoryPool *pmp);

		// shorthand for functions for generating the disjunctive filter predicates
		typedef CStatsPred *(FnPstatspredDisj)(IMemoryPool *pmp);

		private:

			// triplet consisting of comparison type, double value and its byte array representation
			struct SStatsCmpValElem
			{
				CStatsPred::EStatsCmpType m_escmpt; // comparison operator
				const WCHAR *m_wsz; // byte array representation
				CDouble m_dVal; // double value
			}; // SStatsCmpValElem

			// triplet consisting of MCV input file, histogram input file and merged output file
			struct SMergeTestElem
			{
				const CHAR *szInputMCVFile;
				const CHAR *szInputHistFile;
				const CHAR *szMergedFile;
			};

			// test case for intersection of buckets
			struct SBucketsIntersectTestElem
			{
				 // lower bound of bucket 1
				INT m_iLb1;

				 // upper bound of bucket 1
				INT m_iUb1;

				// is lower bound of bucket 1 closed
				BOOL m_fLb1Closed;

				// is upper bound of bucket 1 closed
				BOOL m_fUb1Closed;

				// lower bound of bucket 2
				INT m_iLb2;

				// upper bound of bucket 2
				INT m_iUb2;

				// is lower bound of bucket 2 closed
				BOOL m_fLb2Closed;

				// is upper bound of bucket 2 closed
				BOOL m_fUb2Closed;

				// result of the bucket intersect test
				BOOL fIntersect;

				// lower bound of output bucket
				INT m_iLbOutput;

				// upper bound of output bucket
				INT m_iUbOutput;

				// is lower bound of output bucket closed
				BOOL m_fLbOutputClosed;

				// is upper bound of output bucket closed
				BOOL m_fUbOutputClosed;

			}; // SBucketsIntersectTestElem

			// test case for join evaluation
			struct SStatsJoinSTestCase
			{
				// input stats dxl file
				const CHAR *m_szInputFile;

				// output stats dxl file
				const CHAR *m_szOutputFile;

				// is the join a left outer join
				BOOL m_fLeftOuterJoin;

				// join predicate generation function pointer
				FnPdrgpstatjoin *m_pf;
			}; // SStatsJoinSTestCase

			// int4 histogram test cases
			struct SHistogramTestCase
			{
				// number of buckets in the histogram
				ULONG m_ulBuckets;

				// number of distinct values per bucket
				CDouble m_dNDVPerBucket;

				// percentage of tuples that are null
				BOOL m_fNullFreq;

				// number of remain distinct values
				CDouble m_dNDVRemain;

			}; // SHistogramTestCase

			// test case for join evaluation with NDVRemain
			struct SStatsJoinNDVRemainTestCase
			{
				// column identifier for the first histogram
				ULONG m_ulCol1;

				// column identifier for the second histogram
				ULONG m_ulCol2;

				// number of buckets in the output
				ULONG m_ulBucketsJoin;

				// cumulative number of distinct values in the buckets of the join histogram
				CDouble m_dNDVBucketsJoin;

				// NDV remain of the join histogram
				CDouble m_dNDVRemainJoin;

				// frequency of the NDV remain in the join histogram
				CDouble m_dFreqRemainJoin;
			}; // SStatsJoinNDVRemainTestCase

			// test case for disjunctive filter evaluation
			struct SStatsFilterSTestCase
			{
				// input stats dxl file
				const CHAR *m_szInputFile;

				// output stats dxl file
				const CHAR *m_szOutputFile;

				// filter predicate generation function pointer
				FnPstatspredDisj *m_pf;
			}; // SStatsFilterSTestCase

			// test case for union all evaluation
			struct SStatsUnionAllSTestCase
			{
				// input stats dxl file
				const CHAR *m_szInputFile;

				// output stats dxl file
				const CHAR *m_szOutputFile;
			};

			// helper methods
			static
			CPoint *PpointGeneric(IMemoryPool *pmp, OID oid, CWStringDynamic *pstrValueEncoded, LINT lValue);

			static
			CPoint *PpointNumeric(IMemoryPool *pmp, CWStringDynamic *pstrEncodedValue, CDouble dValue);

			static
			CBucket *Pbucket(IMemoryPool *pmp, INT iLower, INT iUpper, CDouble, CDouble);

			static
			CBucket *Pbucket(IMemoryPool *pmp, BOOL fValue, CDouble);

			static
			CBucket *Pbucket(IMemoryPool *pmp, INT iLower, INT iUpper, BOOL fLowerClosed, BOOL fUpperClosed, CDouble dFrequency, CDouble dDistinct);

			// create filter on int4 types
			static
			void StatsFilterInt4(IMemoryPool *pmp, ULONG ulColId, INT iLower, INT iUpper, DrgPstatspred *pgrgpstatspred);

			// create filter on boolean types
			static
			void StatsFilterBool(IMemoryPool *pmp, ULONG ulColId, BOOL fValue, DrgPstatspred *pgrgpstatspred);

			// create filter on numeric types
			static
			void
			StatsFilterNumeric
				(
				IMemoryPool *pmp,
				ULONG ulColId,
				CWStringDynamic *pstrLowerEncoded,
				CWStringDynamic *pstrUpperEncoded,
				CDouble dValLower,
				CDouble dValUpper,
				DrgPstatspred *pdrgpstatspred
				);

			// create filter on generic types
			static
			void StatsFilterGeneric
				(
				IMemoryPool *pmp,
				ULONG ulColId,
				OID oid,
				CWStringDynamic *pstrLowerEncoded,
				CWStringDynamic *pstrUpperEncoded,
				LINT lValLower,
				LINT lValUpper,
				DrgPstatspred *pgrgpstatspred
				);

			// print methods
			static
			void Print(IMemoryPool *pmp, const char *pcPrefix, const CHistogram *phist);

			static
			void Print(IMemoryPool *pmp, const char *pcPrefix, const CBucket *pbucket);

			// produce example histogram
			static
			CHistogram* PhistExampleInt4(IMemoryPool *pmp);

			// generate int histogram having tuples not covered by buckets,
			// including null fraction and nDistinctRemain
			static
			CHistogram* PhistExampleInt4Remain(IMemoryPool *pmp);

			// generate int histogram based on the NDV and bucket information provided
			static
			CHistogram* PhistInt4Remain
						(
						IMemoryPool *pmp,
						ULONG ulBuckets,
						CDouble dNDVPerBucket,
						BOOL fNullFreq,
						CDouble dNDVRemain
						);

			static
			CHistogram* PhistExampleInt4Dim(IMemoryPool *pmp);

			static
			CHistogram* PhistExampleBool(IMemoryPool *pmp);

			// create a numeric predicate on a particular column
			static
			DrgPstatspred *PdrgppredfilterNumeric(IMemoryPool *pmp, ULONG ulColId, SStatsCmpValElem statsCmpValElem);

			// helper method to generate join predicates for stats evaluation
			static
			DrgPstatsjoin *Pdrgpstatsjoin1(IMemoryPool *pmp);

			// helper function that generates an array of ULONG pointers
			static
			DrgPul *Pdrgpul
					(
					IMemoryPool *pmp,
					ULONG ul1,
					ULONG ul2 = ULONG_MAX
					)
			{
				DrgPul *pdrgpul = GPOS_NEW(pmp) DrgPul(pmp);
				pdrgpul->Append(GPOS_NEW(pmp) ULONG (ul1));

				if (ULONG_MAX != ul2)
				{
					pdrgpul->Append(GPOS_NEW(pmp) ULONG (ul2));
				}

				return pdrgpul;
			}

			// generate join predicate over columns that contain null values
			static
			DrgPstatsjoin *PdrgpstatsjoinNullableCols(IMemoryPool *pmp);

			// helper method to generate join predicates for stats evaluation
			static
			DrgPstatsjoin *Pdrgpstatsjoin2(IMemoryPool *pmp);
 
 			// helper method to iterate over an array generated filter predicates for stats evaluation
			static
			GPOS_RESULT EresUnittest_CStatistics(SStatsFilterSTestCase rgstatsdisjtc[], ULONG ulTestCases);

			// create a table descriptor with two columns having the given names
			static
			CTableDescriptor *PtabdescTwoColumnSource
				(
				IMemoryPool *pmp,
				const CName &nameTable,
				const IMDTypeInt4 *pmdtype,
				const CWStringConst &strColA,
				const CWStringConst &strColB
				);

			// do the bucket boundaries match
			static
			BOOL FMatchBucketBoundary(CBucket *pbucket1, CBucket *pbucket2);

		public:

			// helper method
			static
			void Print(IMemoryPool *, const CStatistics *);

			// generate an array of filter given a column identifier, comparison type, and array of integer point
			static
			DrgPstatspred *PdrgpstatspredInteger
				(
				IMemoryPool *pmp,
				ULONG ulColId,
				CStatsPred::EStatsCmpType escmpt,
				INT *piVals,
				ULONG ulVals
				);

			// example filter
			static
			DrgPstatspred *Pdrgpstatspred1(IMemoryPool *pmp);

			static
			DrgPstatspred *Pdrgpstatspred2(IMemoryPool *pmp);

			// create a filter on a column with null values
			static
			CStatsPred *PstatspredNullableCols(IMemoryPool *pmp);

			// create a point filter where the constant is null
			static
			CStatsPred *PstatspredWithNullConstant(IMemoryPool *pmp);

			// create a 'is not null' point filter
			static
			CStatsPred *PstatspredNotNull(IMemoryPool *pmp);

			// nested AND and OR predicates
			static
			CStatsPred *PstatspredNestedPredDiffCol1(IMemoryPool *pmp);

			// nested AND and OR predicates
			static
			CStatsPred *PstatspredNestedPredDiffCol2(IMemoryPool *pmp);

			// nested AND and OR predicates
			static
			CStatsPred *PstatspredNestedPredCommonCol1(IMemoryPool *pmp);

			// nested AND and OR predicates
			static
			CStatsPred *PstatspredNestedPredCommonCol2(IMemoryPool *pmp);

			// nested AND and OR predicates
			static
			CStatsPred *PstatspredNestedSharedCol(IMemoryPool *pmp);

			// nested AND and OR predicates
			static
			CStatsPred *PstatspredDisjOverConjSameCol1(IMemoryPool *pmp);

			// nested AND and OR predicates
			static
			CStatsPred *PstatspredDisjOverConjSameCol2(IMemoryPool *pmp);

			// nested AND and OR predicates
			static
			CStatsPred *PstatspredDisjOverConjSameCol3(IMemoryPool *pmp);

			// nested AND and OR predicates
			static
			CStatsPred *PstatspredDisjOverConjSameCol4(IMemoryPool *pmp);

			// conjunctive predicates
			static
			CStatsPred *PstatspredConj(IMemoryPool *pmp);

			// nested AND and OR predicates
			static
			CStatsPred *PstatspredDisjOverConjDifferentCol1(IMemoryPool *pmp);

			static
			CStatsPred *PstatspredDisjOverConjMultipleIdenticalCols(IMemoryPool *pmp);

			// disjunction filters
			static
			CStatsPred *PstatspredDisj1(IMemoryPool *pmp);
			
			// disjunction filters
			static
			CStatsPred *PstatspredDisj2(IMemoryPool *pmp);

			// disjunction filters
			static
			CStatsPred *PstatspredDisj3(IMemoryPool *pmp);

			// disjunction filters
			static
			CStatsPred *PstatspredDisj4(IMemoryPool *pmp);

			// disjunction filters
			static
			CStatsPred *PstatspredDisj5(IMemoryPool *pmp);

			// disjunction filters
			static
			CStatsPred *PstatspredDisj6(IMemoryPool *pmp);

			// disjunction filters
			static
			CStatsPred *PstatspredDisj7(IMemoryPool *pmp);

			// disjunction filters
			static
			CStatsPred *PstatspredDisj8(IMemoryPool *pmp);

			// unittests
			static
			GPOS_RESULT EresUnittest();

			// point related tests
			static
			GPOS_RESULT EresUnittest_CPointInt4();

			static
			GPOS_RESULT EresUnittest_CPointBool();

			// join of histograms with NDVRemain information
			static
			GPOS_RESULT EresUnittest_JoinNDVRemain();

			// bucket basic tests
			static
			GPOS_RESULT EresUnittest_CBucketInt4();

			static
			GPOS_RESULT EresUnittest_CBucketBool();

			// join buckets tests
			static
			GPOS_RESULT EresUnittest_Join();

			// union all tests
			static
			GPOS_RESULT EresUnittest_UnionAll();

			// bucket intersect
			static
			GPOS_RESULT EresUnittest_CBucketIntersect();

			// bucket scaling tests
			static
			GPOS_RESULT EresUnittest_CBucketScale();

			// bucket difference tests
			static
			GPOS_RESULT EresUnittest_CBucketDifference();

			// histogram basic tests
			static
			GPOS_RESULT EresUnittest_CHistogramValid();

			static
			GPOS_RESULT EresUnittest_CHistogramInt4();

			static
			GPOS_RESULT EresUnittest_CHistogramBool();

			// statistics basic tests
			static
			GPOS_RESULT EresUnittest_CStatisticsBasic();

			// basic statistics parsing
			static
			GPOS_RESULT EresUnittest_CStatisticsBasicsFromDXL();

			// testing select predicates
			static
			GPOS_RESULT EresUnittest_CStatisticsFilter();

			// testing nested AND / OR predicates
			static
			GPOS_RESULT EresUnittest_CStatisticsNestedPred();

			// test disjunctive filter
			static
			GPOS_RESULT EresUnittest_CStatisticsFilterDisj();

			// test conjunctive filter
			static
			GPOS_RESULT EresUnittest_CStatisticsFilterConj();

			// DXL based test on numeric data types
			static
			GPOS_RESULT EresUnittest_CStatisticsBasicsFromDXLNumeric();

			// compare the derived statistics with the statistics in the outputfile
			static
			GPOS_RESULT EresUnittest_CStatisticsCompare
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				DrgPstats *pdrgpstatBefore,
				CStatsPred *pstatspred,
				const CHAR *szDXLOutput,
				BOOL fApplyTwice = false
				);

			// exercise stats derivation during optimization
			static
			GPOS_RESULT EresUnittest_CStatisticsSelectDerivation();

			// skew basic tests
			static
			GPOS_RESULT EresUnittest_Skew();

			// sort MCVs tests
			static
			GPOS_RESULT EresUnittest_SortInt4MCVs();

			// merge MCVs and histogram
			static
			GPOS_RESULT EresUnittest_MergeHistMCV();

			// test for accumulating cardinality in disjunctive and conjunctive predicates
	 		static
			GPOS_RESULT EresUnittest_CStatisticsAccumulateCard();

			// GbAgg test when grouping on repeated columns
			static
			GPOS_RESULT EresUnittest_GbAggWithRepeatedGbCols();


	}; // class CStatisticsTest
}

#endif // !GPNAUCRATES_CStatisticsTest_H


// EOF
