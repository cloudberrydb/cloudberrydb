//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CJobStateMachine.inl
//
//	@doc:
//		Inline implementation of job state machine
//---------------------------------------------------------------------------
#ifndef GPOPT_CJobStateMachine_INL
#define GPOPT_CJobStateMachine_INL

#include "gpopt/engine/CEngine.h"
#include "gpopt/search/CSchedulerContext.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CJobStateMachine::Init
//
//	@doc:
//		Initialize state machine
//
//---------------------------------------------------------------------------
template<class TEnumState, TEnumState estSentinel, class TEnumEvent, TEnumEvent eevSentinel>
void
CJobStateMachine<TEnumState, estSentinel, TEnumEvent, eevSentinel>::Init
	(
	const TEnumEvent rgfTransitions[estSentinel][estSentinel]
#ifdef GPOS_DEBUG
	,
	const WCHAR wszStates[estSentinel][GPOPT_FSM_NAME_LENGTH],
	const WCHAR wszEvents[estSentinel][GPOPT_FSM_NAME_LENGTH]
#endif // GPOS_DEBUG	
	)
{
	Reset();

	m_sm.Init
		(
		rgfTransitions
#ifdef GPOS_DEBUG
		,
		wszStates,
		wszEvents
#endif // GPOS_DEBUG
		);
}


//---------------------------------------------------------------------------
//	@function:
//		CJobStateMachine::FRun
//
//	@doc:
//		Run state machine
//
//---------------------------------------------------------------------------
template<class TEnumState, TEnumState estSentinel, class TEnumEvent, TEnumEvent eevSentinel>
BOOL
CJobStateMachine<TEnumState, estSentinel, TEnumEvent, eevSentinel>::FRun
	(
	CSchedulerContext *psc,
	CJob *pjOwner
	)
{
	GPOS_ASSERT(NULL != psc);
	GPOS_ASSERT(NULL != pjOwner);
		
	TEnumState estCurrent = estSentinel;
	TEnumState estNext = estSentinel;
	do
	{
		// check if current search stage is timed-out
		if (psc->Peng()->PssCurrent()->FTimedOut())
		{
			// cleanup job state and terminate state machine
			pjOwner->Cleanup();
			return true;
		}
		
		// find current state
		estCurrent = m_sm.Estate();
		
		// get the function associated with current state
		PFuncAction pfunc = m_rgPfuncAction[estCurrent];
		GPOS_ASSERT(NULL != pfunc);
		
		// execute the function to get an event
		TEnumEvent eev = pfunc(psc, pjOwner);
		
		// use the event to transition state machine
		estNext = estCurrent;
#ifdef GPOS_DEBUG
		BOOL fSucceeded =
#endif // GPOS_DEBUG				
		m_sm.FTransition(eev, estNext);

		GPOS_ASSERT(fSucceeded);
		
	}
	while (estNext != estCurrent && estNext != m_sm.TesFinal());
	
	return (estNext == m_sm.TesFinal());
}


//---------------------------------------------------------------------------
//	@function:
//		CJobStateMachine::Reset
//
//	@doc:
//		Reset state machine
//
//---------------------------------------------------------------------------
template<class TEnumState, TEnumState estSentinel, class TEnumEvent, TEnumEvent eevSentinel>
void
CJobStateMachine<TEnumState, estSentinel, TEnumEvent, eevSentinel>::Reset()
{
	m_sm.Reset();

	// initialize actions array
	for (ULONG i = 0; i < estSentinel; i++)
	{
		m_rgPfuncAction[i] = NULL;
	}
}

#endif // !GPOPT_CJobStateMachine_INL

// EOF
