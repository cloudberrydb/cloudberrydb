//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDatumTest.h
//
//	@doc:
//		Test for datum classes
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CDatumTest_H
#define GPNAUCRATES_CDatumTest_H

#include "gpos/base.h"
#include "naucrates/base/IDatum.h"

namespace gpnaucrates
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDatumTest
	//
	//	@doc:
	//		Static unit tests for datum
	//
	//---------------------------------------------------------------------------
	class CDatumTest
	{

		private:
			// create an oid datum
			static
			IDatum *PdatumOid(IMemoryPool *pmp, BOOL fNull);

			// create an int2 datum
			static
			IDatum *PdatumInt2(IMemoryPool *pmp, BOOL fNull);

			// create an int4 datum
			static
			IDatum *PdatumInt4(IMemoryPool *pmp, BOOL fNull);
		
			// create an int8 datum
			static
			IDatum *PdatumInt8(IMemoryPool *pmp, BOOL fNull);

			// create a bool datum
			static
			IDatum *PdatumBool(IMemoryPool *pmp, BOOL fNull);

			// create a generic datum
			static
			IDatum *PdatumGeneric(IMemoryPool *pmp, BOOL fNull);

		public:

			// unittests
			static 
			GPOS_RESULT EresUnittest();
			
			static 
			GPOS_RESULT EresUnittest_Basics();

	}; // class CDatumTest
}

#endif // !GPNAUCRATES_CDatumTest_H


// EOF
