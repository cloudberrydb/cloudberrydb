//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoin2PartialDynamicIndexGetApply.h
//
//	@doc:
//		Transform inner join over partitioned table into a union-all of dynamic index get applies.
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformInnerJoin2PartialDynamicIndexGetApply_H
#define GPOPT_CXformInnerJoin2PartialDynamicIndexGetApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformInnerJoin2IndexApply.h"

namespace gpopt
{
	using namespace gpos;

	// fwd declarations
	class CExpression;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformInnerJoin2PartialDynamicIndexGetApply
	//
	//	@doc:
	//		Transform inner join over partitioned table into a union-all of
	//		dynamic index get applies.
	//
	//---------------------------------------------------------------------------
	class CXformInnerJoin2PartialDynamicIndexGetApply : public CXformInnerJoin2IndexApply
	{
		public:
			// ctor
			explicit
			CXformInnerJoin2PartialDynamicIndexGetApply(IMemoryPool *pmp);

			// dtor
			virtual
			~CXformInnerJoin2PartialDynamicIndexGetApply()
			{}

			// compute xform promise for a given expression handle
			virtual
			EXformPromise Exfp(CExpressionHandle &exprhdl) const;

			// ident accessor
			virtual
			EXformId Exfid() const
			{
				return ExfInnerJoin2PartialDynamicIndexGetApply;
			}

			// xform name
			virtual
			const CHAR *SzId() const
			{
				return "CXformInnerJoin2PartialDynamicIndexGetApply";
			}

			// actual transform
			virtual
			void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const;
	};
}

#endif // !GPOPT_CXformInnerJoin2PartialDynamicIndexGetApply_H

// EOF
