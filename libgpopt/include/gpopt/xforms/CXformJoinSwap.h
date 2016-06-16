//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformJoinSwap.h
//
//	@doc:
//		Join swap transformation
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformJoinSwap_H
#define GPOPT_CXformJoinSwap_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformJoinSwap
	//
	//	@doc:
	//		Join swap transformation
	//
	//---------------------------------------------------------------------------
	template<class TJoinTop, class TJoinBottom>
	class CXformJoinSwap : public CXformExploration
	{

		private:

			// private copy ctor
			CXformJoinSwap(const CXformJoinSwap &);

		public:

			// ctor
			explicit
			CXformJoinSwap(IMemoryPool *pmp);

			// dtor
			virtual
			~CXformJoinSwap() {}

			// compute xform promise for a given expression handle
			virtual
			EXformPromise Exfp
				(
				CExpressionHandle & // exprhdl
				)
				const
			{
				return CXform::ExfpHigh;
			}

			// actual transform
			void Transform
					(
					CXformContext *pxfctxt,
					CXformResult *pxfres,
					CExpression *pexpr
					)
					const;

	}; // class CXformJoinSwap

}

// include inline
#include "CXformJoinSwap.inl"

#endif // !GPOPT_CXformJoinSwap_H

// EOF
