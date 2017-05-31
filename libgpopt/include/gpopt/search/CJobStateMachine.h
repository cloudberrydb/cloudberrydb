//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CJobStateMachine.h
//
//	@doc:
//		State machine to execute actions and maintain state of
//		optimization jobs;
//---------------------------------------------------------------------------
#ifndef GPOPT_CJobStateMachine_H
#define GPOPT_CJobStateMachine_H


#include "gpos/base.h"
#include "gpos/types.h"
#include "gpos/common/CEnumSet.h"

#include "gpopt/base/CStateMachine.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/search/CSchedulerContext.h"

namespace gpopt
{

	using namespace gpos;

	// prototypes
	class CJob;
	class CSchedulerContext;

	//---------------------------------------------------------------------------
	//	@class:
	//		CJobStateMachine
	//
	//	@doc:
	//		Optimization job state machine;
	//
	//---------------------------------------------------------------------------
	template<class TEnumState, TEnumState estSentinel, class TEnumEvent, TEnumEvent eevSentinel>
	class CJobStateMachine
	{

		private:

			// pointer to job action function
			typedef TEnumEvent (*PFuncAction)(CSchedulerContext *psc, CJob *pjOwner);

			// shorthand for state machine
			typedef CStateMachine<TEnumState, estSentinel, TEnumEvent, eevSentinel> SM;

			// array of actions corresponding to states
			PFuncAction m_rgPfuncAction[estSentinel];

			// job state machine
			SM m_sm;

			// hidden copy ctor
			CJobStateMachine (const CJobStateMachine&);

		public:

			// ctor
			CJobStateMachine<TEnumState, estSentinel, TEnumEvent, eevSentinel>()
			{};

			// dtor
			~CJobStateMachine()
			{};

			// initialize state machine
			void Init
				(
				const TEnumEvent rgfTransitions[estSentinel][estSentinel]
#ifdef GPOS_DEBUG
				,
				const WCHAR wszStates[estSentinel][GPOPT_FSM_NAME_LENGTH],
				const WCHAR wszEvent[estSentinel][GPOPT_FSM_NAME_LENGTH]
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
                 wszEvent
#endif // GPOS_DEBUG
                 );
            }

			// match action with state
			void SetAction
				(
				TEnumState	est,
				PFuncAction pfAction
				)
			{
				GPOS_ASSERT(NULL != pfAction);
				GPOS_ASSERT(NULL == m_rgPfuncAction[est] && "Action has been already set");

				m_rgPfuncAction[est] = pfAction;
			}

			// run the state machine
			BOOL FRun(CSchedulerContext *psc, CJob *pjOwner)
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

			// reset state machine
			void Reset()
            {
                m_sm.Reset();

                // initialize actions array
                for (ULONG i = 0; i < estSentinel; i++)
                {
                    m_rgPfuncAction[i] = NULL;
                }
            }

#ifdef GPOS_DEBUG
			// dump history
			IOstream &OsHistory
				(
				IOstream &os
				)
				const
			{
				m_sm.OsHistory(os);
				return os;
			}

			// dump state machine diagram in graphviz format
			IOstream &OsDiagramToGraphviz
				(
				IMemoryPool *pmp,
				IOstream &os,
				const WCHAR *wszTitle
				)
				const
			{
				(void) m_sm.OsDiagramToGraphviz(pmp, os, wszTitle);

				return os;
			}

			// compute unreachable states
			void Unreachable
				(
				IMemoryPool *pmp,
				TEnumState **ppestate,
				ULONG *pulSize
				)
				const
			{
				m_sm.Unreachable(pmp, ppestate, pulSize);
			}

#endif // GPOS_DEBUG

	}; // class CJobStateMachine
}

#endif // !GPOPT_CJobStateMachine_H


// EOF
