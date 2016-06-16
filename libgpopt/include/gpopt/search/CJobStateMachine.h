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
				);

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
			BOOL FRun(CSchedulerContext *psc, CJob *pjOwner);

			// reset state machine
			void Reset();

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


#include "gpopt/search/CJobStateMachine.inl"

#endif // !GPOPT_CJobStateMachine_H


// EOF
