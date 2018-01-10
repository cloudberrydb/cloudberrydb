//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoin2BitmapIndexGetApply.h
//
//	@doc:
//		Transform Inner Join to Bitmap IndexGet Apply
//
//	@owner:
//		n
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformInnerJoin2BitmapIndexGetApply_H
#define GPOPT_CXformInnerJoin2BitmapIndexGetApply_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformInnerJoin2IndexApply.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformInnerJoin2BitmapIndexGetApply
	//
	//	@doc:
	//		Transform Inner Join to Bitmap IndexGet Apply
	//
	//---------------------------------------------------------------------------
	class CXformInnerJoin2BitmapIndexGetApply : public CXformInnerJoin2IndexApply
	{
		private:
			// private copy ctor
			CXformInnerJoin2BitmapIndexGetApply(const CXformInnerJoin2BitmapIndexGetApply &);

		public:
			// ctor
			explicit
			CXformInnerJoin2BitmapIndexGetApply(IMemoryPool *pmp);

			// dtor
			virtual
			~CXformInnerJoin2BitmapIndexGetApply()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfInnerJoin2BitmapIndexGetApply;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CXformInnerJoin2BitmapIndexGetApply";
			}

			// actual transform
			virtual
			void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const;
	}; // class CXformInnerJoin2BitmapIndexGetApply
}

#endif // !GPOPT_CXformInnerJoin2BitmapIndexGetApply_H

// EOF
