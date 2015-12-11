//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply.h
//
//	@doc:
//		Transform Inner Join with a Select over a partitioned table on the inner
//		branch to a dynamic Bitmap IndexGet Apply
//
//	@owner:
//		n
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply_H
#define GPOPT_CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformInnerJoin2IndexApply.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply
	//
	//	@doc:
	//		Transform Inner Join with a Select over a partitioned on the inner branch
	//		to dynamic Bitmap IndexGet Apply
	//
	//---------------------------------------------------------------------------
	class CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply : public CXformInnerJoin2IndexApply
	{
		private:
			// private copy ctor
			CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply
				(
				const CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply &
				);

		public:
			// ctor
			explicit
			CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply(IMemoryPool *pmp);

			// dtor
			virtual
			~CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply";
			}

			// actual transform
			virtual
			void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const;

	}; // class CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply
}

#endif // !GPOPT_CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply_H

// EOF
