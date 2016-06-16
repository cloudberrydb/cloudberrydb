//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoin2IndexGetApply.h
//
//	@doc:
//		Transform Inner Join to IndexGet Apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformInnerJoin2IndexGetApply_H
#define GPOPT_CXformInnerJoin2IndexGetApply_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformInnerJoin2IndexApply.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformInnerJoin2IndexGetApply
	//
	//	@doc:
	//		Transform Inner Join to IndexGet Apply
	//
	//---------------------------------------------------------------------------
	class CXformInnerJoin2IndexGetApply : public CXformInnerJoin2IndexApply
	{

		private:

			// private copy ctor
			CXformInnerJoin2IndexGetApply(const CXformInnerJoin2IndexGetApply &);

		public:

			// ctor
			explicit
			CXformInnerJoin2IndexGetApply(IMemoryPool *pmp);

			// dtor
			virtual
			~CXformInnerJoin2IndexGetApply()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfInnerJoin2IndexGetApply;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CXformInnerJoin2IndexGetApply";
			}

			// actual transform
			virtual
			void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const;

	}; // class CXformInnerJoin2IndexGetApply

}

#endif // !GPOPT_CXformInnerJoin2IndexGetApply_H

// EOF
