//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008-2011 Greenplum, Inc.
//
//	@filename:
//		CScheduler.cpp
//
//	@doc:
//		Implementation of optimizer job scheduler
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpos/sync/CAutoMutex.h"

#include "gpopt/search/CJob.h"
#include "gpopt/search/CJobFactory.h"
#include "gpopt/search/CScheduler.h"
#include "gpopt/search/CSchedulerContext.h"

#include "naucrates/traceflags/traceflags.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CScheduler::CScheduler
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScheduler::CScheduler
	(
	IMemoryPool *mp,
	ULONG ulJobs,
	ULONG_PTR ulpTasks
#ifdef GPOS_DEBUG
	,
	BOOL fTrackingJobs
#endif // GPOS_DEBUG
	)
	:
	m_spjl(mp, ulJobs),
	m_ulpTasksMax(ulpTasks),
	m_ulpTasksActive(0),
	m_ulpTotal(0),
	m_ulpRunning(0),
	m_ulpQueued(0),
	m_ulpStatsQueued(0),
	m_ulpStatsDequeued(0),
	m_ulpStatsSuspended(0),
	m_ulpStatsCompleted(0),
	m_ulpStatsCompletedQueued(0),
	m_ulpStatsResumed(0)
#ifdef GPOS_DEBUG
	,
	m_fTrackingJobs(fTrackingJobs)
#endif // GPOS_DEBUG
{
	// initialize pool of job links
	m_spjl.Init(GPOS_OFFSET(SJobLink, m_id));

	// initialize list of waiting new jobs
	m_listjlWaiting.Init(GPOS_OFFSET(SJobLink, m_link));
	
	// initialize event for job queue
	m_event.Init(&m_mutex);

#ifdef GPOS_DEBUG
	// initialize list of running jobs
	m_listjRunning.Init(GPOS_OFFSET(CJob, m_linkRunning));

	// initialize list of suspended jobs
	m_listjSuspended.Init(GPOS_OFFSET(CJob, m_linkSuspended));
#endif // GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CScheduler::~CScheduler
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScheduler::~CScheduler()
{
	GPOS_ASSERT_IMP
		(
		!ITask::Self()->HasPendingExceptions(),
		0 == m_ulpTotal
		);

	GPOS_ASSERT(0 == m_event.GetNumWaiters());
}


//---------------------------------------------------------------------------
//	@function:
//		CScheduler::Run
//
//	@doc:
//		Main job processing task
//
//---------------------------------------------------------------------------
void*
CScheduler::Run
	(
	void *pv
	)
{
	CSchedulerContext *psc = reinterpret_cast<CSchedulerContext*>(pv);
	psc->Psched()->ProcessJobs(psc);

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CScheduler::ProcessJobs
//
//	@doc:
// 		Internal job processing task
//
//---------------------------------------------------------------------------
void
CScheduler::ProcessJobs
	(
	CSchedulerContext *psc
	)
{
	while (true)
	{
		IncTasksActive();

		// execute waiting jobs
		ExecuteJobs(psc);

		DecrTasksActive();

		CAutoMutex am(m_mutex);
		am.Lock();

		// stop when all jobs have completed
		if (IsEmpty())
		{
			m_event.Broadcast();
			break;
		}

		// wait until there is enough work to pick up
		m_event.Wait();

		GPOS_CHECK_ABORT;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CScheduler::ExecuteJobs
//
//	@doc:
// 		Job processing loop;
//		keeps executing jobs as long as there is work queued;
//
//---------------------------------------------------------------------------
void
CScheduler::ExecuteJobs
	(
	CSchedulerContext *psc
	)
{
	CJob *pj = NULL;
	ULONG count = 0;

	// keep retrieving jobs
	while (NULL != (pj = PjRetrieve()))
	{
		// prepare for job execution
		PreExecute(pj);

		// execute job
		BOOL fCompleted = FExecute(pj, psc);

#ifdef GPOS_DEBUG
		// restrict parallelism to keep track of jobs
		CAutoMutex am(m_mutex);
		if (FTrackingJobs())
		{
			am.Lock();
			m_listjRunning.Remove(pj);
		}
#endif // GPOS_DEBUG

		// process job result
		switch (EjrPostExecute(pj, fCompleted))
		{
			case EjrCompleted:
				// job is completed
				Complete(pj);

#ifdef GPOS_DEBUG
				if (GPOS_FTRACE(EopttracePrintJobScheduler))
				{
					CAutoTrace at(psc->GetGlobalMemoryPool());

					at.Os()
						<< "Print scheduler:" << std::endl
						<< *this;
				}
#endif // GPOS_DEBUG

				psc->Pjf()->Release(pj);
				break;

			case EjrRunnable:
				// child jobs have completed, job can immediately resume
				Resume(pj);
				continue;

			case EjrSuspended:
				// job is suspended until child jobs complete
				Suspend(pj);
				break;

			default:
				GPOS_ASSERT(!"Invalid job execution result");
		}

		if (++count == OPT_SCHED_CFA)
		{
			GPOS_CHECK_ABORT;
			count = 0;
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CScheduler::Add
//
//	@doc:
//		Add new job for execution
//
//---------------------------------------------------------------------------
void
CScheduler::Add
	(
	CJob *pj,
	CJob *pjParent
	)
{
	GPOS_ASSERT(NULL != pj);
	GPOS_ASSERT(0 == pj->UlpRefs());

#ifdef GPOS_DEBUG
	// restrict parallelism to keep track of jobs
	CAutoMutex am(m_mutex);
	if (FTrackingJobs())
	{
		am.Lock();
	}
#endif // GPOS_DEBUG

	// increment ref counter for parent job
	if (NULL != pjParent)
	{
		pjParent->IncRefs();
	}

	// set current job as parent of its child
	pj->SetParent(pjParent);

	// increment total number of jobs
	(void) ExchangeAddUlongPtrWithInt(&m_ulpTotal, 1);

	Schedule(pj);
}


//---------------------------------------------------------------------------
//	@function:
//		CScheduler::Resume
//
//	@doc:
//		Resume suspended job
//
//---------------------------------------------------------------------------
void
CScheduler::Resume
	(
	CJob *pj
	)
{
	GPOS_ASSERT(NULL != pj);
	GPOS_ASSERT(0 == pj->UlpRefs());

	Schedule(pj);
}


//---------------------------------------------------------------------------
//	@function:
//		CScheduler::Schedule
//
//	@doc:
//		Schedule job for execution
//
//---------------------------------------------------------------------------
void
CScheduler::Schedule
	(
	CJob *pj
	)
{
	GPOS_ASSERT(NULL != pj);
	GPOS_ASSERT_IMP(FTrackingJobs(), m_mutex.IsOwned());

	// get job link
	SJobLink *pjl = m_spjl.PtRetrieve();

	// throw OOM if no link can be retrieved
	if (NULL == pjl)
	{
		GPOS_OOM_CHECK(NULL);
	}
	pjl->Init(pj);

#ifdef GPOS_DEBUG
	if (FTrackingJobs())
	{
		GPOS_ASSERT
			(
			CJob::EjsInit == pj->Ejs() ||
			CJob::EjsRunning == pj->Ejs() ||
			CJob::EjsSuspended == pj->Ejs()
			);
		pj->SetState(CJob::EjsWaiting);
	}
#endif // GPOS_DEBUG

	// add to waiting list
	m_listjlWaiting.Push(pjl);

	// increment number of queued jobs
	(void) ExchangeAddUlongPtrWithInt(&m_ulpQueued, 1);

	// update statistics
	(void) ExchangeAddUlongPtrWithInt(&m_ulpStatsQueued, 1);

	// check if there is enough work for another worker
	if (FIncreaseWorkers())
	{
		CAutoMutex am(m_mutex);
#ifdef GPOS_DEBUG
		if (!FTrackingJobs())
		{
			am.Lock();
		}
#else
		am.Lock();
#endif // GPOS_DEBUG

		// wake up worker to pick up a job
		m_event.Signal();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CScheduler::PreExecute
//
//	@doc:
// 		Prepare for job execution
//
//---------------------------------------------------------------------------
void
CScheduler::PreExecute
	(
	CJob *pj
	)
{
	GPOS_ASSERT(NULL != pj);
	GPOS_ASSERT(0 == pj->UlpRefs() &&
				"Runnable job cannot have pending children");

	// increment number of running jobs
	(void) ExchangeAddUlongPtrWithInt(&m_ulpRunning, 1);

	// increment job ref counter
	pj->IncRefs();
}


//---------------------------------------------------------------------------
//	@function:
//		CScheduler::FExecute
//
//	@doc:
//		Execution function using job queue
//
//---------------------------------------------------------------------------
BOOL
CScheduler::FExecute
	(
	CJob *pj,
	CSchedulerContext *psc
	)
{
	BOOL fCompleted = true;
	CJobQueue *pjq = pj->Pjq();

	// check if job is associated to a job queue
	if (NULL == pjq)
	{
		fCompleted = pj->FExecute(psc);
	}
	else
	{
		switch (pjq->EjqrAdd(pj))
		{
			case CJobQueue::EjqrMain:
				// main job, runs job operation
				fCompleted = pj->FExecute(psc);
				if (fCompleted)
				{
					// notify queued jobs
					pjq->NotifyCompleted(psc);
				}
				else
				{
					// task is suspended
					(void) pj->UlpDecrRefs();
				}
				break;

			case CJobQueue::EjqrQueued:
				// queued job
				fCompleted = false;
				break;

			case CJobQueue::EjqrCompleted:
				break;
		}
	}

	return fCompleted;
}


//---------------------------------------------------------------------------
//	@function:
//		CScheduler::EjrPostExecute
//
//	@doc:
// 		Process job execution outcome
//
//---------------------------------------------------------------------------
CScheduler::EJobResult
CScheduler::EjrPostExecute
	(
	CJob *pj,
	BOOL fCompleted
	)
{
	GPOS_ASSERT(NULL != pj);
	GPOS_ASSERT(0 < pj->UlpRefs() && "IsRunning job is marked as completed");

	// decrement job ref counter
	ULONG_PTR ulRefs = pj->UlpDecrRefs();

	// decrement number of running jobs
	(void) ExchangeAddUlongPtrWithInt(&m_ulpRunning, -1);

	// check if job completed
	if (fCompleted)
	{
		GPOS_ASSERT(1 == ulRefs);

		return EjrCompleted;
	}

	// check if all children have completed
	if (1 == ulRefs)
	{
		return EjrRunnable;
	}

	return EjrSuspended;
}


//---------------------------------------------------------------------------
//	@function:
//		CScheduler::PjRetrieve
//
//	@doc:
//		Retrieve next runnable job from queue
//
//---------------------------------------------------------------------------
CJob *
CScheduler::PjRetrieve()
{
#ifdef GPOS_DEBUG
	// restrict parallelism to keep track of jobs
	CAutoMutex am(m_mutex);
	if (FTrackingJobs())
	{
		am.Lock();
	}
#endif // GPOS_DEBUG

	// retrieve runnable job from lists of waiting jobs
	SJobLink *pjl = m_listjlWaiting.Pop();
	CJob *pj = NULL;

	if (NULL != pjl)
	{
		pj = pjl->m_pj;

		GPOS_ASSERT(NULL != pj);
		GPOS_ASSERT(0 == pj->UlpRefs());

		// decrement number of queued jobs
		(void) ExchangeAddUlongPtrWithInt(&m_ulpQueued, -1);

		// update statistics
		(void) ExchangeAddUlongPtrWithInt(&m_ulpStatsDequeued, 1);

		// recycle job link
		m_spjl.Recycle(pjl);

#ifdef GPOS_DEBUG
		// add job to running list
		if (FTrackingJobs())
		{
			GPOS_ASSERT(CJob::EjsWaiting == pj->Ejs());

			pj->SetState(CJob::EjsRunning);
			m_listjRunning.Append(pj);
		}
#endif // GPOS_DEBUG
	}
	
	return pj;
}


//---------------------------------------------------------------------------
//	@function:
//		CScheduler::Suspend
//
//	@doc:
//		Transition job to suspended
//
//---------------------------------------------------------------------------
void
CScheduler::Suspend
	(
	CJob *
#ifdef GPOS_DEBUG
	pj
#endif // GPOS_DEBUG
	)
{
	GPOS_ASSERT(NULL != pj);
	GPOS_ASSERT_IMP(FTrackingJobs(), m_mutex.IsOwned());

#ifdef GPOS_DEBUG
	if (FTrackingJobs())
	{
		GPOS_ASSERT(CJob::EjsRunning == pj->Ejs());

		pj->SetState(CJob::EjsSuspended);
		m_listjSuspended.Append(pj);
	}
#endif // GPOS_DEBUG)

	(void) ExchangeAddUlongPtrWithInt(&m_ulpStatsSuspended, 1);
}


//---------------------------------------------------------------------------
//	@function:
//		CScheduler::Complete
//
//	@doc:
//		Transition job to completed
//
//---------------------------------------------------------------------------
void
CScheduler::Complete
	(
	CJob *pj
	)
{
	GPOS_ASSERT(0 == pj->UlpRefs());
	GPOS_ASSERT_IMP(FTrackingJobs(), m_mutex.IsOwned());

#ifdef GPOS_DEBUG
	if (FTrackingJobs())
	{
		GPOS_ASSERT(CJob::EjsRunning == pj->Ejs());

		pj->SetState(CJob::EjsCompleted);
	}
#endif // GPOS_DEBUG

	ResumeParent(pj);

	// update statistics
	(void) ExchangeAddUlongPtrWithInt(&m_ulpTotal, -1);
	(void) ExchangeAddUlongPtrWithInt(&m_ulpStatsCompleted, 1);
}


//---------------------------------------------------------------------------
//	@function:
//		CScheduler::CompleteQueued
//
//	@doc:
//		Transition queued job to completed
//
//---------------------------------------------------------------------------
void
CScheduler::CompleteQueued
	(
	CJob *pj
	)
{
	GPOS_ASSERT(0 == pj->UlpRefs());

#ifdef GPOS_DEBUG
	// restrict parallelism to keep track of jobs
	CAutoMutex am(m_mutex);
	if (FTrackingJobs())
	{
		am.Lock();

		GPOS_ASSERT(CJob::EjsSuspended == pj->Ejs());

		m_listjSuspended.Remove(pj);
		pj->SetState(CJob::EjsCompleted);
	}
#endif // GPOS_DEBUG

	ResumeParent(pj);

	// update statistics
	(void) ExchangeAddUlongPtrWithInt(&m_ulpTotal, -1);
	(void) ExchangeAddUlongPtrWithInt(&m_ulpStatsCompleted, 1);
	(void) ExchangeAddUlongPtrWithInt(&m_ulpStatsCompletedQueued, 1);
}


//---------------------------------------------------------------------------
//	@function:
//		CScheduler::ResumeParent
//
//	@doc:
//		Resume parent job
//
//---------------------------------------------------------------------------
void
CScheduler::ResumeParent
	(
	CJob *pj
	)
{
	GPOS_ASSERT(0 == pj->UlpRefs());
	GPOS_ASSERT_IMP(FTrackingJobs(), m_mutex.IsOwned());

	CJob *pjParent = pj->PjParent();

	if (NULL != pjParent)
	{
		// notify parent job
		if (pj->FResumeParent())
		{
#ifdef GPOS_DEBUG
			if (FTrackingJobs())
			{
				m_listjSuspended.Remove(pjParent);
			}
#endif // GPOS_DEBUG)

			// reschedule parent
			Resume(pjParent);

			// update statistics
			(void) ExchangeAddUlongPtrWithInt(&m_ulpStatsResumed, 1);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CScheduler::PrintStats
//
//	@doc:
//		Print statistics
//
//---------------------------------------------------------------------------
void
CScheduler::PrintStats() const
{
	GPOS_TRACE_FORMAT
		(
		"Job statistics: Queued=%d Dequeued=%d Suspended=%d "
		                "Resumed=%d CompletedQueued=%d Completed=%d",
		m_ulpStatsQueued,
		m_ulpStatsDequeued,
		m_ulpStatsSuspended,
		m_ulpStatsResumed,
		m_ulpStatsCompletedQueued,
		m_ulpStatsCompleted
		);
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::OsPrintActiveJobs
//
//	@doc:
//		Print scheduler queue
//
//---------------------------------------------------------------------------
IOstream &
CScheduler::OsPrintActiveJobs
	(
	IOstream &os
	)
{
	CAutoMutex am(m_mutex);
	am.Lock();

	os << "Scheduler - active jobs: " << std::endl << std::endl;

	os << "List of running jobs: " << std::endl;
	CJob *pj = m_listjRunning.First();
	while(NULL != pj)
	{
		pj->OsPrint(os);
		pj = m_listjRunning.Next(pj);
	}

	os << std::endl << "List of waiting jobs: " << std::endl;
	
	SJobLink *pjl = m_listjlWaiting.PtFirst();
	while(NULL != pjl)
	{
		pjl->m_pj->OsPrint(os);
		pjl = m_listjlWaiting.Next(pjl);
	}

	os << std::endl << "List of suspended jobs: " << std::endl;
	pj = m_listjSuspended.First();
	while(NULL != pj)
	{
		pj->OsPrint(os);
		pj = m_listjSuspended.Next(pj);
	}

	return os;
}


#endif // GPOS_DEBUG

// EOF

