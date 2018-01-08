//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//---------------------------------------------------------------------------
#ifndef GPOPT_CSemiJoinTest_H
#define GPOPT_CSemiJoinTest_H

#include "gpos/base.h"

namespace gpopt
{
	class CSemiJoinTest
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

	}; // class CSemiJoinTest
}

#endif // !GPOPT_CSemiJoinTest_H

// EOF

