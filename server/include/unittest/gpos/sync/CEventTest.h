//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CEvent.h
//
//	@doc:
//      Unit test for CEvent
//---------------------------------------------------------------------------
#ifndef GPOS_CEventTest_H
#define GPOS_CEventTest_H

#include "gpos/types.h"
#include "gpos/sync/CMutex.h"
#include "gpos/sync/CEvent.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CEventTest
	//
	//	@doc:
	//		Static unit tests
	//
	//---------------------------------------------------------------------------
	class CEventTest
	{
		public:

			static ULONG ulValue;

			// unittests
			static GPOS_RESULT EresUnittest();

			static GPOS_RESULT EresUnittest_ProducerConsumer();
			static GPOS_RESULT EresUnittest_TimedWait();

			static void *PvUnittest_Producer(void *);
			static void *PvUnittest_Consumer(void *);
			static void *PvUnittest_TimedWait(void *);

	}; // CEventTest
}

#endif // !GPOS_CEventTest_H

// EOF

