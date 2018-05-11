//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CStatisticsTest.h
//
//	@doc:
//		Test for CPoint
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
		private:

			// test case for union all evaluation
			struct SStatsUnionAllSTestCase
			{
				// input stats dxl file
				const CHAR *m_szInputFile;

				// output stats dxl file
				const CHAR *m_szOutputFile;
			};

			// create filter on int4 types
			static
			void StatsFilterInt4(IMemoryPool *mp, ULONG colid, INT iLower, INT iUpper, CStatsPredPtrArry *pgrgpstatspred);

			// create filter on boolean types
			static
			void StatsFilterBool(IMemoryPool *mp, ULONG colid, BOOL fValue, CStatsPredPtrArry *pgrgpstatspred);

			// create filter on numeric types
			static
			void
			StatsFilterNumeric
				(
				IMemoryPool *mp,
				ULONG colid,
				CWStringDynamic *pstrLowerEncoded,
				CWStringDynamic *pstrUpperEncoded,
				CDouble dValLower,
				CDouble dValUpper,
				CStatsPredPtrArry *pdrgpstatspred
				);

			// create filter on generic types
			static
			void StatsFilterGeneric
				(
				IMemoryPool *mp,
				ULONG colid,
				OID oid,
				CWStringDynamic *pstrLowerEncoded,
				CWStringDynamic *pstrUpperEncoded,
				LINT lValLower,
				LINT lValUpper,
				CStatsPredPtrArry *pgrgpstatspred
				);

			static
			CHistogram* PhistExampleInt4Dim(IMemoryPool *mp);

			// helper function that generates an array of ULONG pointers
			static
			ULongPtrArray *
					Pdrgpul(IMemoryPool *mp,
					ULONG ul1,
					ULONG ul2 = gpos::ulong_max
					)
			{
				ULongPtrArray *pdrgpul = GPOS_NEW(mp) ULongPtrArray(mp);
				pdrgpul->Append(GPOS_NEW(mp) ULONG (ul1));

				if (gpos::ulong_max != ul2)
				{
					pdrgpul->Append(GPOS_NEW(mp) ULONG (ul2));
				}

				return pdrgpul;
			}

			// create a table descriptor with two columns having the given names
			static
			CTableDescriptor *PtabdescTwoColumnSource
				(
				IMemoryPool *mp,
				const CName &nameTable,
				const IMDTypeInt4 *pmdtype,
				const CWStringConst &strColA,
				const CWStringConst &strColB
				);

		public:

			// example filter
			static
			CStatsPredPtrArry *Pdrgpstatspred1(IMemoryPool *mp);

			static
			CStatsPredPtrArry *Pdrgpstatspred2(IMemoryPool *mp);

			// unittests
			static
			GPOS_RESULT EresUnittest();

			// union all tests
			static
			GPOS_RESULT EresUnittest_UnionAll();

			// statistics basic tests
			static
			GPOS_RESULT EresUnittest_CStatisticsBasic();

			// exercise stats derivation during optimization
			static
			GPOS_RESULT EresUnittest_CStatisticsSelectDerivation();

			// GbAgg test when grouping on repeated columns
			static
			GPOS_RESULT EresUnittest_GbAggWithRepeatedGbCols();


	}; // class CStatisticsTest
}

#endif // !GPNAUCRATES_CStatisticsTest_H


// EOF
