//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal, Inc.
//
//	@filename:
//		CIndexTest.h
//
//	@doc:
//		Test for optimizing queries on tables with indexes
//---------------------------------------------------------------------------
#ifndef GPOPT_CIndexTest_H
#define GPOPT_CIndexTest_H

#include "gpos/base.h"

namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CIndexTest
	//
	//	@doc:
	//		Unittests
	//
	//---------------------------------------------------------------------------
	class CIndexTest
	{
		private:

			// counter used to mark last successful test
			static
			ULONG m_ulIndexTestCounter;

		public:

			// unittests
			static
			GPOS_RESULT EresUnittest();

			static
			GPOS_RESULT EresUnittest_RunTests();

	}; // class CIndexTest
}

#endif // !GPOPT_CIndexTest_H

// EOF

