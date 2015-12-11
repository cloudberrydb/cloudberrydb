//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CTpcdsTest.h
//
//	@doc:
//		Test for optimizing TPC-DS queries
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CTpcdsTest_H
#define GPOPT_CTpcdsTest_H

#include "gpos/base.h"

namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CTpcdsTest
	//
	//	@doc:
	//		Unittests
	//
	//---------------------------------------------------------------------------
	class CTpcdsTest
	{
		private:

			// counter used to mark last successful test
			static
			ULONG m_ulTestCounter;

			// counter used to mark last successful test for partitioned tables
			static
			ULONG m_ulTestCounterParts;
		public:

			// unittests; 'ulQueryNum' indicates TPC-DS query number in [1, 99]
			static
			GPOS_RESULT EresSubtest(ULONG ulQueryNum);

	}; // class CTpcdsTest
}

#endif // !GPOPT_CTpcdsTest_H

// EOF

