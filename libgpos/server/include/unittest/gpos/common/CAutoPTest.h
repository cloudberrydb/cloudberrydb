//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CAutoPTest.h
//
//	@doc:
//      Unit test for CAutoP
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoPTest_H
#define GPOS_CAutoPTest_H

#include "gpos/base.h"

namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CAutoPTest
	//
	//	@doc:
	//		Static unit tests for auto pointer
	//
	//---------------------------------------------------------------------------
	class CAutoPTest
	{

		public:

			class CElem
			{
				public:

					ULONG m_ul;

			}; // class CElem


			// unittests
			static GPOS_RESULT EresUnittest();
			static GPOS_RESULT EresUnittest_Basics();
#ifdef GPOS_DEBUG
#if (GPOS_i386 || GPOS_i686 || GPOS_x86_64)
			static GPOS_RESULT EresUnittest_Allocation();
#endif // (GPOS_i386 || GPOS_i686 || GPOS_x86_64)
#endif // GPOS_DEBUG

	}; // class CAutoPTest

}

#endif // !GPOS_CAutoPTest_H

// EOF

