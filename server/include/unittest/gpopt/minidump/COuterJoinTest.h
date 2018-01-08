//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//---------------------------------------------------------------------------
#ifndef GPOPT_COuterJoinTest_H
#define GPOPT_COuterJoinTest_H

#include "gpos/base.h"

namespace gpopt
{
	class COuterJoinTest
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

	}; // class COuterJoinTest
}

#endif // !GPOPT_COuterJoinTest_H

// EOF

