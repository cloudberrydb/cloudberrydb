//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoin2DynamicBitmapIndexGetApply.h
//
//	@doc:
//		Transform Inner Join to Dynamic Bitmap IndexGet Apply
//
//	@owner:
//		n
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformInnerJoin2DynamicBitmapIndexGetApply_H
#define GPOPT_CXformInnerJoin2DynamicBitmapIndexGetApply_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformInnerJoin2IndexApply.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformInnerJoin2Dynamic BitmapIndexGetApply
	//
	//	@doc:
	//		Transform Inner Join to Dynamic Bitmap IndexGet Apply
	//
	//---------------------------------------------------------------------------
	class CXformInnerJoin2DynamicBitmapIndexGetApply : public CXformInnerJoin2IndexApply
	{
		private:
			// private copy ctor
			CXformInnerJoin2DynamicBitmapIndexGetApply(const CXformInnerJoin2DynamicBitmapIndexGetApply &);

		public:
			// ctor
			explicit
			CXformInnerJoin2DynamicBitmapIndexGetApply(IMemoryPool *pmp);

			// dtor
			virtual
			~CXformInnerJoin2DynamicBitmapIndexGetApply()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfInnerJoin2DynamicBitmapIndexGetApply;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CXformInnerJoin2DynamicBitmapIndexGetApply";
			}

			// actual transform
			virtual
			void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const;
	}; // class CXformInnerJoin2DynamicBitmapIndexGetApply
}


#endif // !GPOPT_CXformInnerJoin2DynamicBitmapIndexGetApply_H

// EOF
