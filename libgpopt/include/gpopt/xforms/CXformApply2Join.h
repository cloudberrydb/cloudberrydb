//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformApply2Join.h
//
//	@doc:
//		Base class for transforming Apply to Join
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformApply2Join_H
#define GPOPT_CXformApply2Join_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformExploration.h"


namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformApply2Join
	//
	//	@doc:
	//		Transform Apply into Join by decorrelating the inner side
	//
	//---------------------------------------------------------------------------
	template<class TApply, class TJoin>
	class CXformApply2Join : public CXformExploration
	{

		private:

			// check if we can create a correlated apply expression from the given expression
			static
			BOOL FCanCreateCorrelatedApply(IMemoryPool *pmp, CExpression *pexprApply);

			// create correlated apply expression
			static
			void CreateCorrelatedApply(IMemoryPool *pmp, CExpression *pexprApply, CXformResult *pxfres);

			// private copy ctor
			CXformApply2Join(const CXformApply2Join &);
			
		protected:

			// helper function to attempt decorrelating Apply's inner child
			static
			BOOL FDecorrelate(IMemoryPool *pmp, CExpression *pexprApply, CExpression **ppexprInner, DrgPexpr **ppdrgpexpr);

			// helper function to decorrelate apply expression and insert alternative into results container
			static
			void Decorrelate(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexprApply);

			// helper function to create a join expression from an apply expression and insert alternative into results container
			static
			void CreateJoinAlternative(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexprApply);

		public:

			// ctor for deep pattern
			explicit
			CXformApply2Join<TApply, TJoin>(IMemoryPool *pmp, BOOL fDeepTree);

			// ctor for shallow pattern
			explicit
			CXformApply2Join<TApply, TJoin>(IMemoryPool *pmp);

			// ctor for passed pattern
			CXformApply2Join<TApply, TJoin>
				(
				IMemoryPool *, // pmp
				CExpression *pexprPattern
				)
				:
				CXformExploration(pexprPattern)
			{}

			// dtor
			virtual
			~CXformApply2Join<TApply, TJoin>()
			{}

			// is transformation an Apply decorrelation (Apply To Join) xform?
			virtual
			BOOL FApplyDecorrelating() const
			{
				return true;
			}

	}; // class CXformApply2Join

}

// include inline
#include "CXformApply2Join.inl"

#endif // !GPOPT_CXformApply2Join_H

// EOF
