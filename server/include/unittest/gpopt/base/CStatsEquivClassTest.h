//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CStatsEquivClassTest.h
//
//	@doc:
//		Testing the generation of equivalence class for the purposed of statistics
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CStatsEquivClassTest_H
#define GPOPT_CStatsEquivClassTest_H

#include "gpos/base.h"

#include "gpopt/base/CRange.h"
#include "gpopt/base/CConstraintInterval.h"
#include "gpopt/base/CConstraintConjunction.h"
#include "gpopt/base/CConstraintDisjunction.h"
#include "gpopt/base/CConstraintNegation.h"

#include "unittest/gpopt/CTestUtils.h"

namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CStatsEquivClassTest
	//
	//	@doc:
	//		Testing the generation of equivalence class for the purposed of statistics
	//
	//---------------------------------------------------------------------------
	class CStatsEquivClassTest
	{
		//---------------------------------------------------------------------------
		//	@class:
		//		SEquivClass
		//
		//	@doc:
		//		Internal structure to store scalar expression and its equivalence class
		//
		//---------------------------------------------------------------------------
		struct SEquivClass
		{
			private:

				// the scalar expression
				CExpression *m_pexpr;

				// expected equivalence classes
				DrgPcrs *m_pdrgpcrs;

			public:

				// ctor
				SEquivClass
					(
					CExpression *pexpr,
					DrgPcrs *pdrgpcrs
					)
					:
					m_pexpr(pexpr),
					m_pdrgpcrs(pdrgpcrs)
					{
						GPOS_ASSERT(NULL != pexpr);
					}

					// dtor
					virtual
					~SEquivClass()
					{
						m_pexpr->Release();
						CRefCount::SafeRelease(m_pdrgpcrs);
					};

					// return the scalar expression
					CExpression *Pexpr() const
					{
						return m_pexpr;
					}

					// return the number of equivalence class
					DrgPcrs* PdrgpcrsEquivClass() const
					{
						return m_pdrgpcrs;
					}
		}; // SEquivClass

		// shorthand for functions for generating a cases for testing generating of equivalence classes
		typedef SEquivClass *(FnPequivclass)(IMemoryPool *pmp, CColumnFactory *pcf, CMDAccessor *pmda);

		// test case for disjunctive filter evaluation
		struct SEquivClassSTestCase
		{
			// filter predicate generation function pointer
			FnPequivclass *m_pf;
		}; // SEquivClassSTestCase

		private:

			// equivalence class tests
			// expression and equivalence classes of a  scalar comparison between column reference and a constant
			static
			SEquivClass *PequivclasstcScIdCmpConst(IMemoryPool *pmp, CColumnFactory *pcf, CMDAccessor *pmda);

			// expression and equivalence classes of a scalar comparison between column references
			static
			SEquivClass *PequivclasstcScIdCmpScId(IMemoryPool *pmp, CColumnFactory *pcf, CMDAccessor *pmda);

			// expression and equivalence classes of a predicate on same column
			static
			SEquivClass *PequivclasstcSameCol(IMemoryPool *pmp, CColumnFactory *pcf, CMDAccessor *pmda);

			// expression and equivalence classes of a predicate with conjunction
			static
			SEquivClass *PequivclasstcConj1(IMemoryPool *pmp, CColumnFactory *pcf, CMDAccessor *pmda);

			// expression and equivalence classes of a predicate with conjunction
			static
			SEquivClass *PequivclasstcConj2(IMemoryPool *pmp, CColumnFactory *pcf, CMDAccessor *pmda);

			// expression and equivalence classes of a predicate with conjunction
			static
			SEquivClass *PequivclasstcConj3(IMemoryPool *pmp, CColumnFactory *pcf, CMDAccessor *pmda);

			// expression and equivalence classes of a predicate with conjunction
			static
			SEquivClass *PequivclasstcConj4(IMemoryPool *pmp, CColumnFactory *pcf, CMDAccessor *pmda);

			// expression and equivalence classes of a predicate with conjunction
			static
			SEquivClass *PequivclasstcConj5(IMemoryPool *pmp, CColumnFactory *pcf, CMDAccessor *pmda);

			// expression and equivalence classes of a predicate with disjunction
			static
			SEquivClass *PequivclasstcDisj1(IMemoryPool *pmp, CColumnFactory *pcf, CMDAccessor *pmda);

			// expression and equivalence classes of a predicate with disjunction
			static
			SEquivClass *PequivclasstcDisj2(IMemoryPool *pmp, CColumnFactory *pcf, CMDAccessor *pmda);

			// expression and equivalence classes of a predicate with disjunction and conjunction
			static
			SEquivClass *PequivclasstcDisjConj1(IMemoryPool *pmp, CColumnFactory *pcf, CMDAccessor *pmda);

			// expression and equivalence classes of a predicate with disjunction and conjunction
			static
			SEquivClass *PequivclasstcDisjConj2(IMemoryPool *pmp, CColumnFactory *pcf, CMDAccessor *pmda);

			// expression and equivalence classes of a predicate with disjunction and conjunction
			static
			SEquivClass *PequivclasstcDisjConj3(IMemoryPool *pmp, CColumnFactory *pcf, CMDAccessor *pmda);

			// expression and equivalence classes of a scalar comparison with cast
			static
			SEquivClass *PequivclasstcCast(IMemoryPool *pmp, CColumnFactory *pcf, CMDAccessor *pmda);

			// check if the two equivalence classes match
			static
			BOOL FMatchingEquivClasses(const DrgPcrs *pdrgpcrsActual, const DrgPcrs *pdrgpcrsExpected);

			// return the number of non-singleton equivalence classes
			static
			ULONG UlEquivClasses(const DrgPcrs *pdrgpcrs);

		public:

			// unittests
			static GPOS_RESULT EresUnittest();
			static GPOS_RESULT EresUnittest_EquivClassFromScalarExpr();

	}; // class CStatsEquivClassTest
}

#endif // !GPOPT_CStatsEquivClassTest_H


// EOF
