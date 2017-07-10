//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal, Inc.
//
//	@filename:
//		CExistsSubqueryTest.h
//
//	@doc:
//		Test for exists and not exists subquery optimization
//---------------------------------------------------------------------------
#ifndef GPOPT_CExistsSubqueryTest_H
#define GPOPT_CExistsSubqueryTest_H

#include "gpos/base.h"

namespace gpopt
{
	class CExistsSubqueryTest
	{
		private:

			// counter used to mark last successful test
			static
			gpos::ULONG m_ulExistsSubQueryTestCounter;

		public:

			// unittests
			static
			gpos::GPOS_RESULT EresUnittest();

			static
			gpos::GPOS_RESULT EresUnittest_RunTests();

	}; // class CExistsSubqueryTest
}

#endif // !GPOPT_CExistsSubqueryTest_H

// EOF

