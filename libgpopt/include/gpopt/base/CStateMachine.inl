//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CStateMachine.inl
//
//	@doc:
//		Inline functions for CStateMachine
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CStateMachine_INL
#define GPOPT_CStateMachine_INL

namespace gpopt
{
	using namespace gpos;

#ifdef GPOS_DEBUG

// graphviz shorthands for dumping the state diagram
#define GRAPHVIZ_SHAPE(shape,x)		\
	"node [shape = " << shape << " ]; \"" << x << "\" ; node [shape = circle];"

#define GRAPHVIZ_DOUBLE_CIRCLE(x)	GRAPHVIZ_SHAPE("doublecircle",x)
#define GRAPHVIZ_BOX(x)				GRAPHVIZ_SHAPE("box",x)


	//---------------------------------------------------------------------------
	//	@function:
	//		CStateMachine::RecordHistory
	//
	//	@doc:
	//		Track event and resulting state
	//
	//---------------------------------------------------------------------------
	template<class TEnumState, TEnumState tenumstateSentinel, class TEnumEvent, TEnumEvent tenumeventSentinel>
	void
	CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>::RecordHistory
		(
		TEnumEvent tenumevent,
		TEnumState tenumstate
		)
	{
		ULONG ulHistory = m_ulHistory % GPOPT_FSM_HISTORY;
		
		m_tenumeventHistory[ulHistory] = tenumevent;
		m_tenumstateHistory[ulHistory] = tenumstate;
		
		++m_ulHistory;
	}
	
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CStateMachine::Walk
	//
	//	@doc:
	//		Generic walker, calls argument function for all edges and passes
	//		void * context object
	//
	//---------------------------------------------------------------------------
	template<class TEnumState, TEnumState tenumstateSentinel, class TEnumEvent, TEnumEvent tenumeventSentinel>
	void
	CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>::Walk
		(
		IMemoryPool *pmp,
		PfWalker Pfpv,
		void *pvContext
		)
		const
	{
		// retrieve all states
		EsetStates *pesetStates = GPOS_NEW(pmp) EsetStates(pmp);
		States(pesetStates);
		
		// loop through all sink states
		EsetStatesIter esetIterSink(*pesetStates);
		while(esetIterSink.FAdvance())
		{
			TEnumState tenumstateSink = esetIterSink.TBit();
		
			// loop through all source states
			EsetStatesIter esetIterSource(*pesetStates);
			while(esetIterSource.FAdvance())
			{
				TEnumState tenumstateSource = esetIterSource.TBit();
				
				// for all pairs of states (source, sink) 
				// compute possible transitions
				EsetEvents *pesetEvents = GPOS_NEW(pmp) EsetEvents(pmp);
				Transitions(tenumstateSource, tenumstateSink, pesetEvents);

				// loop through all connecting edges
				EsetEventsIter esetIterTrans(*pesetEvents);
				while(esetIterTrans.FAdvance())
				{
					// apply walker function
					Pfpv(this, tenumstateSource, tenumstateSink, esetIterTrans.TBit(), pvContext);
				}
				
				pesetEvents->Release();
			}
		}
		
		pesetStates->Release();
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CStateMachine::Diagram
	//
	//	@doc:
	//		Write an individual edge to the output stream
	//
	//---------------------------------------------------------------------------
	template<class TEnumState, TEnumState tenumstateSentinel, class TEnumEvent, TEnumEvent tenumeventSentinel>
	void
	CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>::Diagram
		(
		const CStateMachine *psm,
		TEnumState tenumstateSource,
		TEnumState tenumstateSink,
		TEnumEvent tenumevent,
		void *pvContext
		)
	{	
		IOstream &os = *(IOstream*)pvContext;
		
		os
			<< "\""
			<< psm->WszState(tenumstateSource)
			<< "\" -> \""
			<< psm->WszState(tenumstateSink)
			<< "\" [ label = \""
			<< psm->WszEvent(tenumevent)
			<< "\" ];"
			<< std::endl;
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CStateMachine::Unreachable
	//
	//	@doc:
	//		Check for unreachable states;
	//		Context object is a set of all unreachable states. If this function is
	//		called for an edge that is not a loop, remove the sink of the edge
	//		from the set of unreachable states.
	//
	//---------------------------------------------------------------------------
	template<class TEnumState, TEnumState tenumstateSentinel, class TEnumEvent, TEnumEvent tenumeventSentinel>
	void
	CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>::Unreachable
		(
		const CStateMachine *,
		TEnumState tenumstateSource,
		TEnumState tenumstateSink,
		TEnumEvent , // tenumevent
		void *pvContext
		)
	{	
		EsetStates &eset = *(EsetStates*)pvContext;

		if (tenumstateSource != tenumstateSink)
		{
			// reachable -- remove from set of unreachables
			(void) eset.FExchangeClear(tenumstateSink);
		}
	}
#endif // GPOS_DEBUG

	//---------------------------------------------------------------------------
	//	@function:
	//		CStateMachine::Init
	//
	//	@doc:
	//		Initialize state machine
	//
	//---------------------------------------------------------------------------
	template<class TEnumState, TEnumState tenumstateSentinel, class TEnumEvent, TEnumEvent tenumeventSentinel>
	void
	CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>::Init
		(
		const TEnumEvent rgrgtenumeventTransitions[tenumstateSentinel][tenumstateSentinel]
#ifdef GPOS_DEBUG
		,
		const WCHAR rgwszStates[tenumstateSentinel][GPOPT_FSM_NAME_LENGTH],
		const WCHAR rgwszEvents[tenumeventSentinel][GPOPT_FSM_NAME_LENGTH]
#endif // GPOS_DEBUG
		)
	{
		GPOS_ASSERT(!m_fInit);

		for (ULONG ulOuter = 0; ulOuter < tenumstateSentinel; ulOuter++)
		{
			for (ULONG ulInner = 0; ulInner < tenumstateSentinel; ulInner++)
			{
				m_rgrgtenumeventTransitions[ulOuter][ulInner] = rgrgtenumeventTransitions[ulOuter][ulInner];
			}
		}
		
		
#ifdef GPOS_DEBUG
		for (ULONG ul = 0; ul < tenumstateSentinel; ul++)
		{
			m_rgwszStates[ul] = rgwszStates[ul];
		}
		
		for (ULONG ul = 0; ul < tenumeventSentinel; ul++)
		{		
			m_rgwszEvents[ul] = rgwszEvents[ul];
		}
#endif // GPOS_DEBUG


		m_tenumstate = TesInitial();
		m_fInit = true;
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CStateMachine::FTransition
	//
	//	@doc:
	//		Attempt a transition by calling the virtual implementation;
	//		Returning bool indicates success; returned state is current state -- 
	//		independent of success of this attempt;
	//
	//---------------------------------------------------------------------------
	template<class TEnumState, TEnumState tenumstateSentinel, class TEnumEvent, TEnumEvent tenumeventSentinel>
	BOOL
	CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>::FTransition
		(
		TEnumEvent tenumevent,
		TEnumState &tenumstate
		)
	{
		TEnumState tenumstateNew;
		BOOL fSucceeded = FAttemptTransition(m_tenumstate, tenumevent, tenumstateNew);

		if (fSucceeded)
		{
			m_tenumstate = tenumstateNew;
#ifdef GPOS_DEBUG
			RecordHistory(tenumevent, m_tenumstate);
#endif // GPOS_DEBUG
		}

		tenumstate = m_tenumstate;
		return fSucceeded;
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CStateMachine::Transition
	//
	//	@doc:
	//		Transition shorthand when success is guaranteed (enforced by assert) and
	//		resulting state is ignored by caller;
	//
	//---------------------------------------------------------------------------
	template<class TEnumState, TEnumState tenumstateSentinel, class TEnumEvent, TEnumEvent tenumeventSentinel>
	void
	CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>::Transition
		(
		TEnumEvent tenumevent
		)
	{
		TEnumState tenumstateDummy;
#ifdef GPOS_DEBUG
		BOOL fCheck =
#else
		(void) 
#endif // GPOS_DEBUG
			FTransition(tenumevent, tenumstateDummy);
			
		GPOS_ASSERT(fCheck && "Event rejected");
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CStateMachine::FAttemptTransition
	//
	//	@doc:
	//		Actual transition function;
	//
	//---------------------------------------------------------------------------
	template<class TEnumState, TEnumState tenumstateSentinel, class TEnumEvent, TEnumEvent tenumeventSentinel>
	BOOL
	CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>::FAttemptTransition
		(
		TEnumState tenumstateOld,
		TEnumEvent tenumevent,
		TEnumState &tenumstateNew
		)
		const
	{
		GPOS_ASSERT(tenumevent < tenumeventSentinel);
		GPOS_ASSERT(m_fInit);

		for (ULONG ulOuter = 0; ulOuter < tenumstateSentinel; ulOuter++)
		{
			if (m_rgrgtenumeventTransitions[tenumstateOld][ulOuter] == tenumevent)
			{
#ifdef GPOS_DEBUG
				// make sure there isn't another transition possible for the same event
				for (ULONG ulInner = ulOuter + 1; ulInner < tenumstateSentinel; ulInner++)
				{
					GPOS_ASSERT(m_rgrgtenumeventTransitions[tenumstateOld][ulInner] != tenumevent);
				}
#endif // GPOS_DEBUG

				tenumstateNew = (TEnumState) ulOuter;
				return true;
			}
		}

		return false;
	}


#ifdef GPOS_DEBUG

	//---------------------------------------------------------------------------
	//	@function:
	//		CStateMachine::WszEvent
	//
	//	@doc:
	//		Retrieve event name
	//
	//---------------------------------------------------------------------------
	template<class TEnumState, TEnumState tenumstateSentinel, class TEnumEvent, TEnumEvent tenumeventSentinel>
	const WCHAR *
	CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>::WszEvent
		(
		TEnumEvent tenumevent
		)
		const
	{
		GPOS_ASSERT(m_fInit);
		GPOS_ASSERT(0 <= tenumevent && tenumevent < tenumeventSentinel);

		return m_rgwszEvents[tenumevent];
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CStateMachine::WszState
	//
	//	@doc:
	//		Retrieve state name
	//
	//---------------------------------------------------------------------------
	template<class TEnumState, TEnumState tenumstateSentinel, class TEnumEvent, TEnumEvent tenumeventSentinel>
	const WCHAR *
	CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>::WszState
		(
		TEnumState tenumstate
		)
		const
	{
		GPOS_ASSERT(m_fInit);
		GPOS_ASSERT(0 <= tenumstate && tenumstate < tenumstateSentinel);

		return m_rgwszStates[tenumstate];
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CStateMachine::States
	//
	//	@doc:
	//		Retrieve all states -- enum might have 'holes'
	//
	//---------------------------------------------------------------------------
	template<class TEnumState, TEnumState tenumstateSentinel, class TEnumEvent, TEnumEvent tenumeventSentinel>
	void
	CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>::States
		(
		EsetStates *peset
		)
		const
	{
		for(ULONG ul = 0; ul < tenumstateSentinel; ul++)
		{
			(void) peset->FExchangeSet((TEnumState) ul);
		}
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CStateMachine::Transitions
	//
	//	@doc:
	//		All transitions for a given pair of states;
	//
	//---------------------------------------------------------------------------
	template<class TEnumState, TEnumState tenumstateSentinel, class TEnumEvent, TEnumEvent tenumeventSentinel>
	void
	CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>::Transitions
		(
		TEnumState tenumstateOld,
		TEnumState tenumstateNew,
		EsetEvents *peset
		)
		const
	{
		TEnumEvent tenumevent = m_rgrgtenumeventTransitions[tenumstateOld][tenumstateNew];
		if (tenumeventSentinel != tenumevent)
		{
			(void) peset->FExchangeSet(tenumevent);
		}
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CStateMachine::OsHistory
	//
	//	@doc:
	//		Dump history trace
	//
	//---------------------------------------------------------------------------
	template<class TEnumState, TEnumState tenumstateSentinel, class TEnumEvent, TEnumEvent tenumeventSentinel>
	IOstream &
	CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>::OsHistory
		(
		IOstream &os
		)
		const
	{
		ULONG ulElems = std::min(m_ulHistory, GPOPT_FSM_HISTORY);
		
		ULONG ulStart = m_ulHistory + 1;
		if (m_ulHistory < GPOPT_FSM_HISTORY)
		{
			// if we haven't rolled over, just start at 0
			ulStart = 0;
		}
		
		os << "State Machine History (" << (void*)this << ")" << std::endl;
		
		for (ULONG ul = 0; ul < ulElems; ul++)
		{
			ULONG ulPos = (ulStart + ul) % GPOPT_FSM_HISTORY;
			os 
				<< ul << ": " 
				<< WszEvent(m_tenumeventHistory[ulPos])
				<< " (event) -> " 
				<< WszState(m_tenumstateHistory[ulPos])
				<< " (state)" 
				<< std::endl;
		}
		
		return os;
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CStateMachine::FReachable
	//
	//	@doc:
	//		Check for unreachable states;
	//		Note, this does not check for connectedness of components;
	//
	//---------------------------------------------------------------------------
	template<class TEnumState, TEnumState tenumstateSentinel, class TEnumEvent, TEnumEvent tenumeventSentinel>
	BOOL
	CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>::FReachable
		(
		IMemoryPool *pmp
		)
		const
	{		
		TEnumState *pestate = NULL;
		ULONG ulSize = 0;
		Unreachable(pmp, &pestate, &ulSize);
		GPOS_DELETE_ARRAY(pestate);
		
		return  (ulSize == 0);				
	}
	
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CStateMachine::Unreachable
	//
	//	@doc:
	//		Compute array of unreachable states
	//
	//---------------------------------------------------------------------------
	template<class TEnumState, TEnumState tenumstateSentinel, class TEnumEvent, TEnumEvent tenumeventSentinel>
	void
	CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>::Unreachable
		(
		IMemoryPool *pmp,
		TEnumState **ppestate,
		ULONG *pulSize
		)
		const
	{	
		GPOS_ASSERT(NULL != ppestate);
		GPOS_ASSERT(NULL != pulSize);
		
		// initialize output array
		*ppestate = GPOS_NEW_ARRAY(pmp, TEnumState, tenumstateSentinel);
		for (ULONG ul = 0; ul < tenumstateSentinel; ul++)
		{
			(*ppestate)[ul] = tenumstateSentinel;
		}
		
		// mark all states unreachable at first
		EsetStates *peset = GPOS_NEW(pmp) EsetStates(pmp);
		States(peset);
		
		Walk(pmp, Unreachable, peset);
		
		// store remaining states in output array
		EsetStatesIter esetIter(*peset);
		ULONG ul = 0;
		while (esetIter.FAdvance())
		{
			(*ppestate)[ul++] = esetIter.TBit();
		}
		*pulSize = ul;
		peset->Release();
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CStateMachine::OsDiagramToGraphviz
	//
	//	@doc:
	//		Dump state diagram in graphviz format;
	//
	//		Post-process with: dot -Tps2 <diagramfile> > <postgresfile>
	//
	//---------------------------------------------------------------------------
	template<class TEnumState, TEnumState tenumstateSentinel, class TEnumEvent, TEnumEvent tenumeventSentinel>
	IOstream &
	CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>::OsDiagramToGraphviz
		(
		IMemoryPool *pmp,
		IOstream &os,
		const WCHAR *wszTitle
		)
		const
	{		
		os 
			<< "digraph " << wszTitle << " { "
			<< std::endl
			<< GRAPHVIZ_DOUBLE_CIRCLE(WszState(TesInitial()))
			<< std::endl;

		// get unreachable states
		EsetStates *peset = GPOS_NEW(pmp) EsetStates(pmp);
		States(peset);
				
		Walk(pmp, Unreachable, peset);

		// print all unreachable nodes using BOXes
		EsetStatesIter esetIter(*peset);
		while(esetIter.FAdvance())
		{
			os
				<< GRAPHVIZ_BOX(WszState(esetIter.TBit()))
				<< std::endl;
		}
		peset->Release();

		// print the remainder of the diagram by writing all edges only;
		// nodes are implicit;
		Walk(pmp, Diagram, &os);

		os << "} " << std::endl;
		return os;
	}

#endif // GPOS_DEBUG

}

#endif // !GPOPT_CStateMachine_INL

// EOF
