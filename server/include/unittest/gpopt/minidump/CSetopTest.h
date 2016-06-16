//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal, Inc.
//
//	@filename:
//		CSetopTest.h
//
//	@doc:
//		Test for optimizing queries with set operators
//---------------------------------------------------------------------------
#ifndef GPOPT_CSetopTest_H
#define GPOPT_CSetopTest_H

#include "gpos/base.h"

namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CSetopTest
	//
	//	@doc:
	//		Unittests
	//
	//---------------------------------------------------------------------------
	class CSetopTest
	{
		private:

			// counter used to mark last successful test
			static
			ULONG m_ulSetopTestCounter;

		public:

			// unittests
			static
			GPOS_RESULT EresUnittest();

			static
			GPOS_RESULT EresUnittest_RunTests();

	}; // class CSetopTest
}

#endif // !GPOPT_CSetopTest_H

// EOF

