//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoinWithInnerSelect2BitmapIndexGetApply.h
//
//	@doc:
//		Transform Inner Join with a Select on the inner branch to
//		Bitmap IndexGet Apply
//
//	@owner:
//		n
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformInnerJoinWithInnerSelect2BitmapIndexGetApply_H
#define GPOPT_CXformInnerJoinWithInnerSelect2BitmapIndexGetApply_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformInnerJoin2IndexApply.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformInnerJoinWithInnerSelect2BitmapIndexGetApply
	//
	//	@doc:
	//		Transform Inner Join with a Select on the inner branch to
	//		Bitmap IndexGet Apply
	//
	//---------------------------------------------------------------------------
	class CXformInnerJoinWithInnerSelect2BitmapIndexGetApply : public CXformInnerJoin2IndexApply
	{
		private:
			// private copy ctor
			CXformInnerJoinWithInnerSelect2BitmapIndexGetApply
				(
				const CXformInnerJoinWithInnerSelect2BitmapIndexGetApply &
				);

		public:
			// ctor
			explicit
			CXformInnerJoinWithInnerSelect2BitmapIndexGetApply(IMemoryPool *pmp);

			// dtor
			virtual
			~CXformInnerJoinWithInnerSelect2BitmapIndexGetApply()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfInnerJoinWithInnerSelect2BitmapIndexGetApply;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CXformInnerJoinWithInnerSelect2BitmapIndexGetApply";
			}

			// actual transform
			virtual
			void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const;

	}; // class CXformInnerJoinWithInnerSelect2BitmapIndexGetApply
}

#endif // !GPOPT_CXformInnerJoinWithInnerSelect2BitmapIndexGetApply_H

// EOF
