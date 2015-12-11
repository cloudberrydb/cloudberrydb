//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CFunctionalDependencyTest.h
//
//	@doc:
//		Test for functional dependencies
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CFunctionalDependencyTest_H
#define GPOPT_CFunctionalDependencyTest_H

namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CFunctionalDependencyTest
	//
	//	@doc:
	//		Static unit tests for functional dependencies
	//
	//---------------------------------------------------------------------------
	class CFunctionalDependencyTest
	{

		public:

			// unittests
			static GPOS_RESULT EresUnittest();
			static GPOS_RESULT EresUnittest_Basics();

	}; // class CFunctionalDependencyTest
}

#endif // !GPOPT_CFunctionalDependencyTest_H


// EOF
