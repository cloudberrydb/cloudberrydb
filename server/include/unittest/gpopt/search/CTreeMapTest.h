//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CTreeMapTest.h
//
//	@doc:
//		Test for tree map implementation
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CTreeMapTest_H
#define GPOPT_CTreeMapTest_H

#include "gpos/base.h"

#include "gpopt/search/CTreeMap.h"

namespace gpopt
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CTreeMapTest
	//
	//	@doc:
	//		unittest for tree map
	//
	//---------------------------------------------------------------------------
	class CTreeMapTest
	{

		// fwd declaration
		class CNode;
		typedef CDynamicPtrArray<CNode, CleanupRelease<CNode> > DrgPnd;

		// struct for resulting trees
		class CNode : public CRefCount
		{
			private: 
			
				// element number
				ULONG m_ulData;
				
				// children
				DrgPnd *m_pdrgpnd;

				// private copy ctor
				CNode(const CNode &);

			public:
				
				// ctor
				CNode(IMemoryPool *pmp, ULONG *pulData, DrgPnd *pdrgpnd);
				
				// dtor
				~CNode();
				
				// debug print
				IOstream &OsPrint(IOstream &os, ULONG ulIndent = 0) const;
		};

				
		private:

			// counter used to mark last successful test
			static
			ULONG m_ulTestCounter;

			// factory function for result object
			static
			CNode *Pnd(IMemoryPool *pmp, ULONG *pul, DrgPnd *pdrgpnd, BOOL *fTestTrue);
		
			// shorthand for tests
			typedef CTreeMap<ULONG, CNode, BOOL, UlHash<ULONG>, FEqual<ULONG> > TestMap;

			// helper to generate loaded the tree map
			static 
			TestMap *PtmapLoad(IMemoryPool *pmp);

		public:

			// unittests
			static GPOS_RESULT EresUnittest();
			static GPOS_RESULT EresUnittest_Basic();
			static GPOS_RESULT EresUnittest_Count();
			static GPOS_RESULT EresUnittest_Unrank();
			static GPOS_RESULT EresUnittest_Memo();
			
#ifndef GPOS_DEBUG
			// this test is run in optimized build because of long optimization time
			static GPOS_RESULT EresUnittest_FailedPlanEnumerationTests();
#endif // GPOS_DEBUG



#ifdef GPOS_DEBUG
			static GPOS_RESULT EresUnittest_Cycle();
#endif // GPOS_DEBUG

	}; // CTreeMapTest

}

#endif // !GPOPT_CTreeMapTest_H


// EOF
