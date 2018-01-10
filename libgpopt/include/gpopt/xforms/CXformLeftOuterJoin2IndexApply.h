//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal, Inc.
//
//	Transform left outer Join to Index Apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftOuterJoin2IndexApply_H
#define GPOPT_CXformLeftOuterJoin2IndexApply_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformJoin2IndexApply.h"

namespace gpopt
{
	using namespace gpos;

	// fwd declaration
	class CLogicalJoin;
	class CLogicalApply;

	class CXformLeftOuterJoin2IndexApply : public CXformJoin2IndexApply
	{

		private:

			// private copy ctor
			CXformLeftOuterJoin2IndexApply(const CXformLeftOuterJoin2IndexApply &);

		protected:

			virtual
			CLogicalJoin *PopLogicalJoin(IMemoryPool *pmp) const
			{
				return GPOS_NEW(pmp) CLogicalLeftOuterJoin(pmp);
			}


			virtual
			CLogicalApply *PopLogicalApply
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcr
				) const
			{
				return GPOS_NEW(pmp) CLogicalLeftOuterIndexApply(pmp, pdrgpcr);
			}

		public:

			// ctor
			explicit
			CXformLeftOuterJoin2IndexApply
				(
				CExpression *pexprPattern
				)
				:
				CXformJoin2IndexApply(pexprPattern)
			{}

			// dtor
			virtual
			~CXformLeftOuterJoin2IndexApply()
			{}

	}; // class CXformLeftOuterJoin2IndexApply

}

#endif // !GPOPT_CXformLeftOuterJoin2IndexApply_H

// EOF
