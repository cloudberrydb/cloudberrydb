//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformLeftAntiSemiJoinNotIn2CrossProduct.h
//
//	@doc:
//		Transform left anti semi join with NotIn semantics to cross product
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftAntiSemiJoinNotIn2CrossProduct_H
#define GPOPT_CXformLeftAntiSemiJoinNotIn2CrossProduct_H

#include "gpos/base.h"
#include "gpopt/operators/CPatternTree.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CLogicalLeftAntiSemiJoinNotIn.h"
#include "gpopt/xforms/CXformLeftAntiSemiJoin2CrossProduct.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformLeftAntiSemiJoinNotIn2CrossProduct
	//
	//	@doc:
	//		Transform left anti semi join with NotIn semantics to cross product
	//
	//---------------------------------------------------------------------------
	class CXformLeftAntiSemiJoinNotIn2CrossProduct : public CXformLeftAntiSemiJoin2CrossProduct
	{

		private:

			// private copy ctor
			CXformLeftAntiSemiJoinNotIn2CrossProduct(const CXformLeftAntiSemiJoinNotIn2CrossProduct &);

		public:

			// ctor
			explicit
			CXformLeftAntiSemiJoinNotIn2CrossProduct
				(
				IMemoryPool *pmp
				)
				:
				// pattern
				CXformLeftAntiSemiJoin2CrossProduct
					(
					GPOS_NEW(pmp) CExpression
								(
								pmp,
								GPOS_NEW(pmp) CLogicalLeftAntiSemiJoinNotIn(pmp),
								GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp)), // left child is a tree since we may need to push predicates down
								GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // right child
								GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))  // predicate is a tree since we may need to do clean-up of scalar expression
								)
					)
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfLeftAntiSemiJoinNotIn2CrossProduct;
			}

			// return a string for xform name
			virtual
			const CHAR *SzId() const
			{
				return "CXformLeftAntiSemiJoinNotIn2CrossProduct";
			}

	}; // class CXformLeftAntiSemiJoinNotIn2CrossProduct

}


#endif // !GPOPT_CXformLeftAntiSemiJoinNotIn2CrossProduct_H

// EOF
