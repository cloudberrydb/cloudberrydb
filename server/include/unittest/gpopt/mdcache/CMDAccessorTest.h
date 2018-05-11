//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CMDAccessorTest.h
//
//	@doc:
//		Tests accessing objects from the metadata cache.
//---------------------------------------------------------------------------


#ifndef GPOPT_CMDAccessorTest_H
#define GPOPT_CMDAccessorTest_H

#include "gpos/base.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/operators/CExpression.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CMDAccessorTest
	//
	//	@doc:
	//		Static unit tests
	//
	//---------------------------------------------------------------------------
	
	class CMDAccessorTest
	{
		private:

			// a task for looking up a single object from the MD cache
			static void *PvLookupSingleObj(void * pv);
			
			// a task for looking up multiple objects from the MD cache
			static void *PvLookupMultipleObj(void * pv);
			
			// task that creates a MD accessor and starts multiple threads which
			// lookup MD objects through that accessor
			static void *PvInitMDAAndLookup(void *pv);
			
			// cache task function pointer
			typedef void * (*TaskFuncPtr)(void *);
			
			// structure for passing parameters to task functions
			struct SMDCacheTaskParams
			{
				// memory pool
				IMemoryPool *m_pmp;
				
				// MD accessor
				CMDAccessor *m_pmda;
								
				SMDCacheTaskParams(IMemoryPool *pmp, CMDAccessor *pmda)
					:
					m_pmp(pmp),
					m_pmda(pmda)
				{}
			};

		public:

			// unittests
			static GPOS_RESULT EresUnittest();
			static GPOS_RESULT EresUnittest_Basic();
			static GPOS_RESULT EresUnittest_Datum();
			static GPOS_RESULT EresUnittest_DatumGeneric();
			static GPOS_RESULT EresUnittest_Navigate();
			static GPOS_RESULT EresUnittest_Negative();
			static GPOS_RESULT EresUnittest_Indexes();
			static GPOS_RESULT EresUnittest_CheckConstraint();
			static GPOS_RESULT EresUnittest_IndexPartConstraint();
			static GPOS_RESULT EresUnittest_Cast();
			static GPOS_RESULT EresUnittest_ScCmp();

			static GPOS_RESULT EresUnittest_ConcurrentAccessSingleMDA();
			static GPOS_RESULT EresUnittest_ConcurrentAccessMultipleMDA();
			static GPOS_RESULT EresUnittest_PrematureMDIdRelease();

	}; // class CMDAccessorTest
}

#endif // !GPOPT_CMDAccessorTest_H

// EOF
