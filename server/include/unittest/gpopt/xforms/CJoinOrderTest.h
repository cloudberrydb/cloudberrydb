//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CJoinOrderTest.h
//
//	@doc:
//		Tests for join ordering
//---------------------------------------------------------------------------
#ifndef GPOPT_CJoinOrderTest_H
#define GPOPT_CJoinOrderTest_H

#include "gpos/base.h"

#include "gpopt/operators/CExpression.h"

namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CJoinOrderTest
	//
	//	@doc:
	//		Tests for join ordering
	//
	//---------------------------------------------------------------------------
	class CJoinOrderTest
	{

		public:
		
			// unittests
			static GPOS_RESULT EresUnittest();
			static GPOS_RESULT EresUnittest_Expand();
			static GPOS_RESULT EresUnittest_ExpandMinCard();

	}; // class CJoinOrderTest
}


#endif // !GPOPT_CJoinOrderTest_H

// EOF
