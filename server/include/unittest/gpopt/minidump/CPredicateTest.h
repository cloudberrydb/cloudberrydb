//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//---------------------------------------------------------------------------
#ifndef GPOPT_CPredicateTest_H
#define GPOPT_CPredicateTest_H

#include "gpos/base.h"

namespace gpopt
{
	class CPredicateTest
	{
		private:

			// counter used to mark last successful test
			static
			gpos::ULONG m_ulTestCounter;

		public:

			// unittests
			static
			gpos::GPOS_RESULT EresUnittest();

			static
			gpos::GPOS_RESULT EresUnittest_RunTests();

	}; // class CPredicateTest
}

#endif // !GPOPT_CPredicateTest_H

// EOF

