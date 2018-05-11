//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CSchedulerTest.h
//
//	@doc:
//		Test for CScheduler
//---------------------------------------------------------------------------
#ifndef GPOPT_CSchedulerTest_H
#define GPOPT_CSchedulerTest_H

#include "gpos/base.h"

#include "gpopt/search/CJobTest.h"
#include "gpopt/search/CSearchStage.h"

namespace gpopt
{
	// prototypes
	class CJobFactory;
	class CScheduler;
	class CEngine;
	class CMDAccessor;

	//---------------------------------------------------------------------------
	//	@class:
	//		CSchedulerTest
	//
	//	@doc:
	//		unittest for scheduler
	//
	//---------------------------------------------------------------------------
	class CSchedulerTest
	{
		private:

			// create scheduler and add root job
			static
			void ScheduleRoot
					(
					CJobTest::ETestType ett,
					ULONG ulRounds,
					ULONG ulFanout,
					ULONG ulIters,
					ULONG ulWorkers
#ifdef GPOS_DEBUG
					,
					BOOL fTrackingJobs = false
#endif // GPOS_DEBUG
					);

			// run job execution tasks
			static
			void RunTasks
					(
					IMemoryPool *mp,
					CJobFactory *pjf,
					CScheduler *psched,
					CEngine *peng,
					ULONG ulWorkers
					);

		public:

			// build memo using multiple threads
			static
			void BuildMemoMultiThreaded(IMemoryPool *mp, CExpression *pexprInput, CSearchStageArray *search_stage_array);

			// unittests
			static GPOS_RESULT EresUnittest();
			static GPOS_RESULT EresUnittest_SpawnBasic();
			static GPOS_RESULT EresUnittest_SpawnLight();
			static GPOS_RESULT EresUnittest_SpawnHeavy();
			static GPOS_RESULT EresUnittest_QueueBasic();
			static GPOS_RESULT EresUnittest_QueueLight();
			static GPOS_RESULT EresUnittest_QueueHeavy();
			static GPOS_RESULT EresUnittest_BuildMemo();
			static GPOS_RESULT EresUnittest_BuildMemoLargeJoins();


	}; // CSchedulerTest

}

#endif // !GPOPT_CSchedulerTest_H


// EOF
