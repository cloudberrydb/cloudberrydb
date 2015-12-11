//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoinWithInnerSelect2PartialDynamicIndexGetApply.h
//
//	@doc:
//		Transform inner join over Select over a partitioned table into
//		a union-all of dynamic index get applies
//
//	@owner:
//		n
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformInnerJoinWithInnerSelect2PartialDynamicIndexGetApply_H
#define GPOPT_CXformInnerJoinWithInnerSelect2PartialDynamicIndexGetApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformInnerJoin2IndexApply.h"

namespace gpopt
{
	using namespace gpos;

	// fwd declarations
	class CExpression;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformInnerJoinWithInnerSelect2PartialDynamicIndexGetApply
	//
	//	@doc:
	//		Transform inner join over Select over a partitioned table into a union-all
	//		of dynamic index get applies
	//
	//---------------------------------------------------------------------------
	class CXformInnerJoinWithInnerSelect2PartialDynamicIndexGetApply : public CXformInnerJoin2IndexApply
	{
		public:
			// ctor
			explicit
			CXformInnerJoinWithInnerSelect2PartialDynamicIndexGetApply(IMemoryPool *pmp);

			// dtor
			virtual
			~CXformInnerJoinWithInnerSelect2PartialDynamicIndexGetApply()
			{}

			// compute xform promise for a given expression handle
			virtual
			EXformPromise Exfp(CExpressionHandle &exprhdl) const;

			// ident accessor
			virtual
			EXformId Exfid() const
			{
				return ExfInnerJoinWithInnerSelect2PartialDynamicIndexGetApply;
			}

			// xform name
			virtual
			const CHAR *SzId() const
			{
				return "CXformInnerJoinWithInnerSelect2PartialDynamicIndexGetApply";
			}

			// actual transform
			virtual
			void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const;
	};
}

#endif // !GPOPT_CXformInnerJoinWithInnerSelect2PartialDynamicIndexGetApply_H

// EOF
