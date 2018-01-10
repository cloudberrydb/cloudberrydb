//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal, Inc.
//
//	Transform left outer Join to IndexGet Apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftOuterJoin2IndexGetApply_H
#define GPOPT_CXformLeftOuterJoin2IndexGetApply_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformLeftOuterJoin2IndexApply.h"

namespace gpopt
{
	using namespace gpos;

	class CXformLeftOuterJoin2IndexGetApply : public CXformLeftOuterJoin2IndexApply
	{

		private:

			// private copy ctor
			CXformLeftOuterJoin2IndexGetApply(const CXformLeftOuterJoin2IndexGetApply &);

		public:

			// ctor
			explicit
			CXformLeftOuterJoin2IndexGetApply(IMemoryPool *pmp);

			// dtor
			virtual
			~CXformLeftOuterJoin2IndexGetApply()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfLeftOuterJoin2IndexGetApply;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CXformLeftOuterJoin2IndexGetApply";
			}

			// actual transform
			virtual
			void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const;

	}; // class CXformLeftOuterJoin2IndexGetApply

}

#endif // !GPOPT_CXformLeftOuterJoin2IndexGetApply_H

// EOF
