//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformPushGbBelowSetOp.h
//
//	@doc:
//		Push grouping below set operation
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformPushGbBelowSetOp_H
#define GPOPT_CXformPushGbBelowSetOp_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformPushGbBelowSetOp
	//
	//	@doc:
	//		Push grouping below set operation
	//
	//---------------------------------------------------------------------------
	template<class TSetOp>
	class CXformPushGbBelowSetOp : public CXformExploration
	{

		private:

			// private copy ctor
			CXformPushGbBelowSetOp(const CXformPushGbBelowSetOp &);

		public:

			// ctor
			explicit
			CXformPushGbBelowSetOp(IMemoryPool *pmp);

			// dtor
			virtual
			~CXformPushGbBelowSetOp() {}

			// compute xform promise for a given expression handle
			virtual
			EXformPromise Exfp(CExpressionHandle &exprhdl) const;

			// actual transform
			virtual
			void Transform
					(
					CXformContext *pxfctxt,
					CXformResult *pxfres,
					CExpression *pexpr
					)
					const;

	}; // class CXformPushGbBelowSetOp

}

// include inlined implementation
#include "CXformPushGbBelowSetOp.inl"

#endif // !GPOPT_CXformPushGbBelowSetOp_H

// EOF
