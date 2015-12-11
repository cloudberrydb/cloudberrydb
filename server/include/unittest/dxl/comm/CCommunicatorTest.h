//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CCommunicatorTest.h
//
//	@doc:
//		Test for communication handler
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CCommunicatorTest_H
#define GPOPT_CCommunicatorTest_H

#include "gpos/base.h"

namespace gpnaucrates
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CCommunicatorTest
	//
	//	@doc:
	//		unittest for scheduler
	//
	//---------------------------------------------------------------------------
	class CCommunicatorTest
	{
		private:

			// struct containing test parameters
			struct STestParams
			{
				// memory pool
				IMemoryPool *m_pmp;

				// binding path
				const CHAR *m_szPath;
			};

			// spawn tasks to run client-server
			static
			void Unittest_RunClientServer
				(
				void *(*pfServer)(void *pv),
				void *(*pfClient)(void *pv)
				);

			// server function on basic test
			static
			void *PvUnittest_ServerBasic(void *pv);

			// client function on basic test
			static
			void *PvUnittest_ClientBasic(void *pv);

			// server function on error test
			static
			void *PvUnittest_ServerError(void *pv);

			// client function on error test
			static
			void *PvUnittest_ClientError(void *pv);

		public:

			// unittest driver
			static
			GPOS_RESULT EresUnittest();

			// basic communication test
			static
			GPOS_RESULT EresUnittest_Basic();

			// exception propagation test
			static
			GPOS_RESULT EresUnittest_Error();

	}; // CCommunicatorTest

}

#endif // !GPOPT_CCommunicatorTest_H


// EOF
