//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2012 EMC Corp.
//
//	@filename:
//		CKeyCollectionTest.h
//
//	@doc:
//		Test for key collection functionality
//---------------------------------------------------------------------------
#ifndef GPOS_CKeyCollectionTest_H
#define GPOS_CKeyCollectionTest_H

namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CKeyCollectionTest
	//
	//	@doc:
	//		Static unit tests for key collections
	//
	//---------------------------------------------------------------------------
	class CKeyCollectionTest
	{

		public:

			// unittests
			static GPOS_RESULT EresUnittest();
			static GPOS_RESULT EresUnittest_Basics();
			static GPOS_RESULT EresUnittest_Subsumes();
	}; // class CKeyCollectionTest
}

#endif // !GPOS_CKeyCollectionTest_H


// EOF
