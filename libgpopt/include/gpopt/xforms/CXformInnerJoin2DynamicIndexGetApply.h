//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoin2DynamicIndexGetApply.h
//
//	@doc:
//		Transform Inner Join to DynamicIndexGet-Apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformInnerJoin2DynamicIndexGetApply_H
#define GPOPT_CXformInnerJoin2DynamicIndexGetApply_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformInnerJoin2IndexApply.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformInnerJoin2DynamicIndexGetApply
	//
	//	@doc:
	//		Transform Inner Join to DynamicIndexGet-Apply
	//
	//---------------------------------------------------------------------------
	class CXformInnerJoin2DynamicIndexGetApply : public CXformInnerJoin2IndexApply
	{

		private:

			// private copy ctor
			CXformInnerJoin2DynamicIndexGetApply(const CXformInnerJoin2DynamicIndexGetApply &);

		public:

			// ctor
			explicit
			CXformInnerJoin2DynamicIndexGetApply(IMemoryPool *pmp);

			// dtor
			virtual
			~CXformInnerJoin2DynamicIndexGetApply()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfInnerJoin2DynamicIndexGetApply;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CXformInnerJoin2DynamicIndexGetApply";
			}

			// actual transform
			virtual
			void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const;

	}; // class CXformInnerJoin2DynamicIndexGetApply

}

#endif // !GPOPT_CXformInnerJoin2DynamicIndexGetApply_H

// EOF
