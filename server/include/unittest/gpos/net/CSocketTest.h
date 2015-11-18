//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSocketTest.h
//
//	@doc:
//		Socket tests
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPOS_CSocketTest_H
#define GPOS_CSocketTest_H

#include "gpos/base.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CSocketTest
	//
	//	@doc:
	//		Static unit tests for communication over sockets
	//
	//---------------------------------------------------------------------------
	class CSocketTest
	{
		private:

			// struct containing test parameters
			struct SSocketTestParams
			{
				// memory pool
				IMemoryPool *m_pmp;

				// binding path
				const CHAR *m_szPath;

				// connection id
				ULONG m_ulId;

				// client count
				ULONG m_ulClients;

				// number of send iterations
				ULONG m_ulIterations;
			};

			// send a message and receive expected response
			static
			void *PvUnittest_SendBasic(void *pv);

			// receive expected message and send response
			static
			void *PvUnittest_RecvBasic(void *pv);

			// send a message with client ID and receive expected response
			static
			void *PvUnittest_SendConcurrent(void *pv);

			// receive messages from multiple clients and send response
			static
			void *PvUnittest_RecvConcurrent(void *pv);

			// send multiple messages and receive expected response
			static
			void *PvUnittest_SendStress(void *pv);

			// receive messages from multiple clients and assign them to tasks
			static
			void *PvUnittest_RecvStress(void *pv);

			// client-handling task
			static
			void *PvUnittest_RecvTask(void *pv);

			// check for exception simulation
			static
			void CheckException(CException ex);

		public:

			// unittest driver
			static
			GPOS_RESULT EresUnittest();

			// test ability to send a message and receive a response
			static
			GPOS_RESULT EresUnittest_Basic();

			// test ability to serve many clients concurrently
			static
			GPOS_RESULT EresUnittest_Concurrency();

			// test ability to communicate under heavy load
			static
			GPOS_RESULT EresUnittest_Stress();

			// test handling of creating an invalid listener
			static
			GPOS_RESULT EresUnittest_InvalidListener();

	}; // class CSocketTest

}

#endif // !GPOS_CSocketTest_H

// EOF

