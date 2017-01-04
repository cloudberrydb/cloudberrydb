//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CTimeSliceTest.h
//
//	@doc:
//      Extended test for time slice enforcement.
//---------------------------------------------------------------------------
#ifndef GPOS_CTimeSliceTest_H
#define GPOS_CTimeSliceTest_H

#include "gpos/base.h"

#ifdef GPOS_DEBUG

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CTimeSliceTest
	//
	//	@doc:
	//		Extended unittest for time slice enforcement
	//
	//---------------------------------------------------------------------------
	class CTimeSliceTest
	{
		public:

			// unittests
			static GPOS_RESULT EresUnittest();
			static GPOS_RESULT EresUnittest_Basic();
			static GPOS_RESULT EresUnittest_CheckTimeSlice();

	}; // CTimeSliceTest
}

#endif // GPOS_DEBUG

#endif // !GPOS_CTimeSliceTest_H

// EOF

