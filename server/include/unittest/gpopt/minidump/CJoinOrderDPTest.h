//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software
//
//	@filename:
//		CJoinOrderDPTest.h
//
//	@doc:
//		Test for n-ary join order
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CJoinOrderDPTest_H
#define GPOPT_CJoinOrderDPTest_H

#include "gpos/base.h"

namespace gpopt
{
	class CJoinOrderDPTest
	{
		public:

			// unittests
			static
			gpos::GPOS_RESULT EresUnittest();
	}; // class CJoinOrderDPTest
}

#endif // !GPOPT_CJoinOrderDPTest_H

// EOF
