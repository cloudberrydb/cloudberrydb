//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2012 EMC Corp.
//
//	@filename:
//		CMaxCardTest.cpp
//
//	@doc:
//		Tests for max card computation
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/test/CUnittest.h"

#include "gpopt/base/CMaxCard.h"
#include "unittest/gpopt/base/CMaxCardTest.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CMaxCardTest::EresUnittest
//
//	@doc:
//		Unittest for key collections
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMaxCardTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CMaxCardTest::EresUnittest_Basics)
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}

//---------------------------------------------------------------------------
//	@function:
//		CMaxCardTest::EresUnittest_Basics
//
//	@doc:
//		Basic test for key collections
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMaxCardTest::EresUnittest_Basics()
{

#ifdef GPOS_DEBUG

	CMaxCard mcOne(1);
	CMaxCard mcTwo(1);
	GPOS_ASSERT(mcOne == mcTwo);

	CMaxCard mcThree;
	GPOS_ASSERT(!(mcOne == mcThree));
	
	CMaxCard mcFour(0);
	mcFour *= mcThree;
	GPOS_ASSERT(0 == mcFour);
	
	mcFour += mcOne;
	GPOS_ASSERT(1 == mcFour);
	
	mcFour *= mcThree;
	GPOS_ASSERT(GPOPT_MAX_CARD == mcFour);

	mcFour += mcThree;
	GPOS_ASSERT(GPOPT_MAX_CARD == mcFour);
	
#endif

	return GPOS_OK;
}


// EOF
