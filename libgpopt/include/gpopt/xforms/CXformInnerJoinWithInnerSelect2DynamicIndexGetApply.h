//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoinWithInnerSelect2DynamicIndexGetApply.h
//
//	@doc:
//		Transform Inner Join with Select over Dynamic Get on inner branch into IndexGet Apply
//
//	@owner:
//		n
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformInnerJoinWithInnerSelect2DynamicIndexGetApply_H
#define GPOPT_CXformInnerJoinWithInnerSelect2DynamicIndexGetApply_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformInnerJoin2IndexApply.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformInnerJoinWithInnerSelect2DynamicIndexGetApply
	//
	//	@doc:
	//		Transform Inner Join with Select over Dynamic Get on the inner branch into IndexGet Apply
	//
	//---------------------------------------------------------------------------
	class CXformInnerJoinWithInnerSelect2DynamicIndexGetApply : public CXformInnerJoin2IndexApply
	{
		private:
			// private copy ctor
			CXformInnerJoinWithInnerSelect2DynamicIndexGetApply
				(
				const CXformInnerJoinWithInnerSelect2DynamicIndexGetApply &
				);

		public:
			// ctor
			explicit
			CXformInnerJoinWithInnerSelect2DynamicIndexGetApply(IMemoryPool *pmp);

			// dtor
			virtual
			~CXformInnerJoinWithInnerSelect2DynamicIndexGetApply()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfInnerJoinWithInnerSelect2DynamicIndexGetApply;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CXformInnerJoinWithInnerSelect2DynamicIndexGetApply";
			}

			// actual transform
			virtual
			void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const;
	};
}


#endif // !GPOPT_CXformInnerJoinWithInnerSelect2DynamicIndexGetApply_H

// EOF
