//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal, Inc.
//
//	@filename:
//		CPartTblTest.h
//
//	@doc:
//		Test for optimizing queries on partitioned tables
//---------------------------------------------------------------------------
#ifndef GPOPT_CPartTblTest_H
#define GPOPT_CPartTblTest_H

#include "gpos/base.h"

namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CPartTblTest
	//
	//	@doc:
	//		Unittests
	//
	//---------------------------------------------------------------------------
	class CPartTblTest
	{
		private:

			// counter used to mark last successful test
			static
			ULONG m_ulPartTblTestCounter;

		public:

			// unittests
			static
			GPOS_RESULT EresUnittest();

			static
			GPOS_RESULT EresUnittest_RunTests();

	}; // class CPartTblTest
}

#endif // !GPOPT_CPartTblTest_H

// EOF

