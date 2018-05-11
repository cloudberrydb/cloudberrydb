//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		CFilterCardinalityTest.h
//
//	@doc:
//		Test for filter cardinality estimation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CFilterCardinalityTest_H
#define GPNAUCRATES_CFilterCardinalityTest_H

#include "gpos/base.h"

#include "naucrates/statistics/CPoint.h"
#include "naucrates/statistics/CBucket.h"
#include "naucrates/statistics/CHistogram.h"
#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CStatsPredDisj.h"

#include "naucrates/dxl/CDXLUtils.h"

namespace gpnaucrates
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CFilterCardinalityTest
	//
	//	@doc:
	//		Static unit tests for join cardinality estimation
	//
	//---------------------------------------------------------------------------
	class CFilterCardinalityTest
	{
		// shorthand for functions for generating the disjunctive filter predicates
		typedef CStatsPred *(FnPstatspredDisj)(IMemoryPool *mp);

		private:

			// triplet consisting of comparison type, double value and its byte array representation
			struct SStatsCmpValElem
			{
				CStatsPred::EStatsCmpType m_stats_cmp_type; // comparison operator
				const WCHAR *m_wsz; // byte array representation
				CDouble m_value; // double value
			}; // SStatsCmpValElem

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

			// helper method to iterate over an array generated filter predicates for stats evaluation
			static
			GPOS_RESULT EresUnittest_CStatistics(SStatsFilterSTestCase rgstatsdisjtc[], ULONG ulTestCases);

			// disjunction filters
			static
			CStatsPred *PstatspredDisj1(IMemoryPool *mp);

			// disjunction filters
			static
			CStatsPred *PstatspredDisj2(IMemoryPool *mp);

			// disjunction filters
			static
			CStatsPred *PstatspredDisj3(IMemoryPool *mp);

			// disjunction filters
			static
			CStatsPred *PstatspredDisj4(IMemoryPool *mp);

			// disjunction filters
			static
			CStatsPred *PstatspredDisj5(IMemoryPool *mp);

			// disjunction filters
			static
			CStatsPred *PstatspredDisj6(IMemoryPool *mp);

			// disjunction filters
			static
			CStatsPred *PstatspredDisj7(IMemoryPool *mp);

			// disjunction filters
			static
			CStatsPred *PstatspredDisj8(IMemoryPool *mp);

			// nested AND and OR predicates
			static
			CStatsPred *PstatspredNestedPredDiffCol1(IMemoryPool *mp);

			// nested AND and OR predicates
			static
			CStatsPred *PstatspredNestedPredDiffCol2(IMemoryPool *mp);

			// nested AND and OR predicates
			static
			CStatsPred *PstatspredNestedPredCommonCol1(IMemoryPool *mp);

			// nested AND and OR predicates
			static
			CStatsPred *PstatspredNestedPredCommonCol2(IMemoryPool *mp);

			// nested AND and OR predicates
			static
			CStatsPred *PstatspredNestedSharedCol(IMemoryPool *mp);

			// nested AND and OR predicates
			static
			CStatsPred *PstatspredDisjOverConjSameCol1(IMemoryPool *mp);

			// nested AND and OR predicates
			static
			CStatsPred *PstatspredDisjOverConjSameCol2(IMemoryPool *mp);

			// nested AND and OR predicates
			static
			CStatsPred *PstatspredDisjOverConjSameCol3(IMemoryPool *mp);

			// nested AND and OR predicates
			static
			CStatsPred *PstatspredDisjOverConjSameCol4(IMemoryPool *mp);

			// nested AND and OR predicates
			static
			CStatsPred *PstatspredDisjOverConjDifferentCol1(IMemoryPool *mp);

			static
			CStatsPred *PstatspredDisjOverConjMultipleIdenticalCols(IMemoryPool *mp);

			// conjunctive predicates
			static
			CStatsPred *PstatspredConj(IMemoryPool *mp);

			// generate an array of filter given a column identifier, comparison type, and array of integer point
			static
			CStatsPredPtrArry *PdrgpstatspredInteger
			(
					IMemoryPool *mp,
					ULONG colid,
					CStatsPred::EStatsCmpType stats_cmp_type,
					INT *piVals,
					ULONG ulVals
			);

			// create a numeric predicate on a particular column
			static
			CStatsPredPtrArry *PdrgppredfilterNumeric(IMemoryPool *mp, ULONG colid, SStatsCmpValElem statsCmpValElem);

			// create a filter on a column with null values
			static
			CStatsPred *PstatspredNullableCols(IMemoryPool *mp);

			// create a point filter where the constant is null
			static
			CStatsPred *PstatspredWithNullConstant(IMemoryPool *mp);

			// create a 'is not null' point filter
			static
			CStatsPred *PstatspredNotNull(IMemoryPool *mp);

			// compare the derived statistics with the statistics in the outputfile
			static
			GPOS_RESULT EresUnittest_CStatisticsCompare
			(
					IMemoryPool *mp,
					CMDAccessor *md_accessor,
					CStatisticsArray *pdrgpstatBefore,
					CStatsPred *pred_stats,
					const CHAR *szDXLOutput,
					BOOL fApplyTwice = false
			);

		public:

			// unittests
			static
			GPOS_RESULT EresUnittest();

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

			// basic statistics parsing
			static
			GPOS_RESULT EresUnittest_CStatisticsBasicsFromDXL();

			// test for accumulating cardinality in disjunctive and conjunctive predicates
			static
			GPOS_RESULT EresUnittest_CStatisticsAccumulateCard();

	}; // class CFilterCardinalityTest
}

#endif // !GPNAUCRATES_CFilterCardinalityTest_H


// EOF
