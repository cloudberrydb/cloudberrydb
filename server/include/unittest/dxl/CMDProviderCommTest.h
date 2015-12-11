//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 Greenplum, Inc.
//
//	@filename:
//		CMDProviderCommTest.h
//
//	@doc:
//		Tests the metadata provider.
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------


#ifndef GPOPT_CMDProviderCommTest_H
#define GPOPT_CMDProviderCommTest_H

#include "gpos/base.h"


namespace gpdxl
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CMDProviderCommTest
	//
	//	@doc:
	//		Static unit tests
	//
	//---------------------------------------------------------------------------

	class CMDProviderCommTest
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

			// client function for retrieving type info
			static
			void *PvUnittest_ClientTypeInfo(void *pv);

		public:

			// file for the file-based MD provider
			static const CHAR *szFileName;

			// unittests
			static GPOS_RESULT EresUnittest();
			static GPOS_RESULT EresUnittest_MDProviderComm();


	}; // class CMDProviderCommTest
}

#endif // !GPOPT_CMDProviderCommTest_H

// EOF
