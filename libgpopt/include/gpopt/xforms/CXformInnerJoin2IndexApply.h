//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoin2IndexApply.h
//
//	@doc:
//		Transform Inner Join to Index Apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformInnerJoin2IndexApply_H
#define GPOPT_CXformInnerJoin2IndexApply_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformJoin2IndexApply.h"

namespace gpopt
{
	using namespace gpos;

	// fwd declaration
	class CLogicalJoin;
	class CLogicalApply;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformInnerJoin2IndexApply
	//
	//	@doc:
	//		Transform Inner Join to Index Apply
	//
	//---------------------------------------------------------------------------
	class CXformInnerJoin2IndexApply : public CXformJoin2IndexApply
	{

		private:

			// private copy ctor
			CXformInnerJoin2IndexApply(const CXformInnerJoin2IndexApply &);

		protected:

			virtual
			CLogicalJoin *PopLogicalJoin(IMemoryPool *pmp) const
			{
				return GPOS_NEW(pmp) CLogicalInnerJoin(pmp);
			}


			virtual
			CLogicalApply *PopLogicalApply
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcr
				) const
			{
				return GPOS_NEW(pmp) CLogicalInnerIndexApply(pmp, pdrgpcr);
			}

		public:

			// ctor
			explicit
			CXformInnerJoin2IndexApply
				(
				CExpression *pexprPattern
				)
				:
				CXformJoin2IndexApply(pexprPattern)
			{}

			// dtor
			virtual
			~CXformInnerJoin2IndexApply()
			{}

	}; // class CXformInnerJoin2IndexApply

}

#endif // !GPOPT_CXformInnerJoin2IndexApply_H

// EOF
