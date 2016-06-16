//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementCorrelatedApply.h
//
//	@doc:
//		Base class for implementing correlated NLJ
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementCorrelatedApply_H
#define GPOPT_CXformImplementCorrelatedApply_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformImplementation.h"


namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformImplementCorrelatedApply
	//
	//	@doc:
	//		Implement correlated Apply
	//
	//---------------------------------------------------------------------------
	template<class TLogicalApply, class TPhysicalJoin>
	class CXformImplementCorrelatedApply : public CXformImplementation
	{

		private:

			// private copy ctor
			CXformImplementCorrelatedApply(const CXformImplementCorrelatedApply &);
		

		public:

			// ctor
			explicit
			CXformImplementCorrelatedApply(IMemoryPool *pmp);

			// dtor
			virtual
			~CXformImplementCorrelatedApply()
			{}

			// compute xform promise for a given expression handle
			virtual
			EXformPromise Exfp(CExpressionHandle &exprhdl) const;

			// actual transform
			void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const;

	}; // class CXformImplementCorrelatedApply

}

// include inline
#include "CXformImplementCorrelatedApply.inl"

#endif // !GPOPT_CXformImplementCorrelatedApply_H

// EOF
