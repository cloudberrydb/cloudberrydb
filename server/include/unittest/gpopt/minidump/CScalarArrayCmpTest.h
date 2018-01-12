//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarArrayCmpTest_H
#define GPOPT_CScalarArrayCmpTest_H

#include "gpos/base.h"

namespace gpopt
{
	class CScalarArrayCmpTest
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

	}; // class CScalarArrayCmpTest
}

#endif // !GPOPT_CScalarArrayCmpTest_H

// EOF

