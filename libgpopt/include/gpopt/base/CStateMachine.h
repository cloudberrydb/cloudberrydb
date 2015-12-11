//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CStateMachine.h
//
//	@doc:
//		Basic FSM implementation;
//		This superclass provides the API and handles only the bookkeeping;
//
//		Each subclass has to provide a transition function which encodes
//		the actual state graph and determines validity of transitions;
//
//		Provides debug functionality to draw state diagram and validation
//		function to detect unreachable states.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CStateMachine_H
#define GPOPT_CStateMachine_H

#include "gpos/base.h"
#include "gpos/types.h"
#include "gpos/common/CEnumSet.h"

#ifdef GPOS_DEBUG
#include "gpos/common/CEnumSetIter.h"

// history of past N states/events
#define GPOPT_FSM_HISTORY 25u

#endif // GPOS_DEBUG

// maximum length of names used for states and events
#define GPOPT_FSM_NAME_LENGTH 128

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CStateMachine
	//
	//	@doc:
	//		Abstract FSM which accepts events to manage state transitions;
	//		Corresponds to a Moore Machine;
	//
	//---------------------------------------------------------------------------
	template<class TEnumState, TEnumState tenumstateSentinel, class TEnumEvent, TEnumEvent tenumeventSentinel>
	class CStateMachine
	{
		private:

#ifdef GPOS_DEBUG
			// shorthand for sets and iterators
			typedef CEnumSet<TEnumState, tenumstateSentinel> EsetStates;
			typedef CEnumSet<TEnumEvent, tenumeventSentinel> EsetEvents;
			
			typedef CEnumSetIter<TEnumState, tenumstateSentinel> EsetStatesIter;
			typedef CEnumSetIter<TEnumEvent, tenumeventSentinel> EsetEventsIter;
#endif // GPOS_DEBUG

			// current state
			TEnumState m_tenumstate;

			// flag indicating if the state machine is initialized
			BOOL m_fInit;

			// array of transitions
			TEnumEvent m_rgrgtenumeventTransitions[tenumstateSentinel][tenumstateSentinel];

#ifdef GPOS_DEBUG

			// state names
			const WCHAR *m_rgwszStates[tenumstateSentinel];

			// event names
			const WCHAR *m_rgwszEvents[tenumeventSentinel];

			// current index into history
			ULONG m_ulHistory;
			
			// state history
			TEnumState m_tenumstateHistory[GPOPT_FSM_HISTORY];

			// event history
			TEnumEvent m_tenumeventHistory[GPOPT_FSM_HISTORY];

			// track event
			void RecordHistory(TEnumEvent tenumevent, TEnumState tenumstate);
			
			// resolve names for states
			const WCHAR *WszState(TEnumState tenumstate) const;
			
			// resolve names for events
			const WCHAR *WszEvent(TEnumEvent tenumevent) const;

			// retrieve all states -- enum might have 'holes'
			void States(EsetStates *peset) const;
			
			// determine all possible transitions between 2 given states
			void Transitions(TEnumState tenumstate, TEnumState tenumevent, EsetEvents *peset) const;

			// shorthand for walker function type
			typedef void (*PfWalker)
					(
					const CStateMachine *psm,
					TEnumState tenumstateOld,
					TEnumState tenumstateNew,
					TEnumEvent tenumevent,
					void *pvContext
					);

			// generic walker function, called for every edge in the graph
			void Walk(IMemoryPool *pmp, PfWalker, void *pvContext) const;

			// print function -- used with walker
			static
			void Diagram
				(
				const CStateMachine *psm,
				TEnumState tenumstateOld,
				TEnumState tenumstateNew,
				TEnumEvent tenumevent,
				void *pvContext
				);

			
			// check for unreachable nodes -- used with walker
			static
			void Unreachable
				(
				const CStateMachine *psm,
				TEnumState tenumstateOld,
				TEnumState tenumstateNew,
				TEnumEvent tenumevent,
				void *pvContext
				);
#endif // GPOS_DEBUG

			// actual implementation of transition
			BOOL FAttemptTransition
				(
				TEnumState tenumstateOld,
				TEnumEvent tenumevent,
				TEnumState &tenumstateNew
				) const;

			// hidden copy ctor
			CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>
				(const CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>&);
		
		public:
				
			// ctor
			CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>()
				:
				m_tenumstate(TesInitial()),
				m_fInit(false)
#ifdef GPOS_DEBUG
				,
				m_ulHistory(0)
#endif // GPOS_DEBUG
			{
				GPOS_ASSERT
					(
					0 < tenumstateSentinel &&
					0 < tenumeventSentinel &&
					(ULONG) tenumeventSentinel + 1 >= (ULONG) tenumstateSentinel
					);
			}

			// initialize state machine
			void Init
				(
				const TEnumEvent rgrgtenumstateTransitions[tenumstateSentinel][tenumstateSentinel]
#ifdef GPOS_DEBUG
				,
				const WCHAR rgwszStates[tenumstateSentinel][GPOPT_FSM_NAME_LENGTH],
				const WCHAR rgwszEvents[tenumeventSentinel][GPOPT_FSM_NAME_LENGTH]
#endif // GPOS_DEBUG
				);

			// dtor
			~CStateMachine()
			{};
						
			// attempt transition
			BOOL FTransition(TEnumEvent tenumevent, TEnumState &tenumstate);
			
			// shorthand if current state and return value are not needed
			void Transition(TEnumEvent tenumevent);

			// inspect current state; to be used only in assertions
			TEnumState Estate() const
			{
				return m_tenumstate;
			}
			
			// get initial state
			TEnumState TesInitial() const
			{
				return (TEnumState) 0;
			}

			// get final state
			TEnumState TesFinal() const
			{
				return (TEnumState) (tenumstateSentinel - 1);
			}

			// reset state
			void Reset()
			{
				m_tenumstate = TesInitial();
				m_fInit = false;
			}

#ifdef GPOS_DEBUG
			// dump history
			IOstream &OsHistory(IOstream &os) const;
			
			// check for unreachable states
			BOOL FReachable(IMemoryPool *pmp) const;
			
			// compute array of unreachable states
			void Unreachable(IMemoryPool *pmp, TEnumState **ppestate, ULONG *pulSize) const;

			// dump Moore diagram in graphviz format
			IOstream &OsDiagramToGraphviz
				(
				IMemoryPool *pmp,
				IOstream &os,
				const WCHAR *wszTitle
				)
				const;
#endif // GPOS_DEBUG

	}; // class CStateMachine
}


#include "gpopt/base/CStateMachine.inl"

#endif // !GPOPT_CStateMachine_H


// EOF
