//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CXformLeftSemiApplyInWithExternalCorrs2InnerJoin.h
//
//	@doc:
//		Turn Left Semi Apply (with IN semantics) with external correlations
//		into inner join;
//		external correlations are correlations in the inner child of LSA
//		that use columns not defined by the outer child of LSA
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftSemiApplyInWithExternalCorrs2InnerJoin_H
#define GPOPT_CXformLeftSemiApplyInWithExternalCorrs2InnerJoin_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformLeftSemiApplyWithExternalCorrs2InnerJoin.h"
#include "gpopt/operators/ops.h"


namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformLeftSemiApplyInWithExternalCorrs2InnerJoin
	//
	//	@doc:
	//		Transform Apply into Join by decorrelating the inner side
	//
	//---------------------------------------------------------------------------
	class CXformLeftSemiApplyInWithExternalCorrs2InnerJoin : public CXformLeftSemiApplyWithExternalCorrs2InnerJoin
	{

		private:

			// private copy ctor
			CXformLeftSemiApplyInWithExternalCorrs2InnerJoin(const CXformLeftSemiApplyInWithExternalCorrs2InnerJoin &);

		public:

			// ctor
			explicit
			CXformLeftSemiApplyInWithExternalCorrs2InnerJoin
				(
				IMemoryPool *pmp
				)
				:
				CXformLeftSemiApplyWithExternalCorrs2InnerJoin
					(
						pmp,
						GPOS_NEW(pmp) CExpression
							(
							pmp,
							GPOS_NEW(pmp) CLogicalLeftSemiApplyIn(pmp),
							GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // left child
							GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp)), // right child
							GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp)) // predicate
							)
					)
			{}

			// dtor
			virtual
			~CXformLeftSemiApplyInWithExternalCorrs2InnerJoin()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfLeftSemiApplyInWithExternalCorrs2InnerJoin;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CXformLeftSemiApplyInWithExternalCorrs2InnerJoin";
			}


	}; // class CXformLeftSemiApplyInWithExternalCorrs2InnerJoin

}

#endif // !GPOPT_CXformLeftSemiApplyInWithExternalCorrs2InnerJoin_H

// EOF

