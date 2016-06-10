//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CTaskSchedulerFifo.cpp
//
//	@doc:
//		Implementation of task scheduler with FIFO.
//---------------------------------------------------------------------------

#include "gpos/task/CTaskSchedulerFifo.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CTaskSchedulerFifo::Enqueue
//
//	@doc:
//		Add task to waiting queue
//
//---------------------------------------------------------------------------
void
CTaskSchedulerFifo::Enqueue
	(
	CTask *ptsk
	)
{
	m_qtsk.Append(ptsk);
	ptsk->SetStatus(CTask::EtsQueued);
}


//---------------------------------------------------------------------------
//	@function:
//		CTaskSchedulerFifo::PtskDequeue
//
//	@doc:
//		Get next task to execute
//
//---------------------------------------------------------------------------
CTask *
CTaskSchedulerFifo::PtskDequeue()
{
	GPOS_ASSERT(!m_qtsk.FEmpty());

	CTask *ptsk = m_qtsk.RemoveHead();
	ptsk->SetStatus(CTask::EtsDequeued);
	return ptsk;
}


//---------------------------------------------------------------------------
//	@function:
//		CTaskSchedulerFifo::Cancel
//
//	@doc:
//		Check if task is waiting to be scheduled and remove it
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTaskSchedulerFifo::EresCancel
	(
	 CTask *ptsk
	)
{
	// iterate until found
	CTask *ptskIt = m_qtsk.PtFirst();
	while (NULL != ptskIt)
	{
		if (ptskIt == ptsk)
		{
			m_qtsk.Remove(ptskIt);
			ptskIt->Cancel();

			return GPOS_OK;
		}
		ptskIt = m_qtsk.PtNext(ptskIt);
	}

	return GPOS_NOT_FOUND;
}





// EOF

