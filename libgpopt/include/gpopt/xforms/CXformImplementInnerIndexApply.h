//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformImplementInnerIndexApply.h
//
//	@doc:
//		Implementing Inner Index Apply
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementInnerIndexApply_H
#define GPOPT_CXformImplementInnerIndexApply_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformImplementation.h"


namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformImplementInnerIndexApply
	//
	//	@doc:
	//		Implement Inner Index Apply
	//
	//---------------------------------------------------------------------------
	class CXformImplementInnerIndexApply : public CXformImplementation
	{

		private:

			// private copy ctor
			CXformImplementInnerIndexApply(const CXformImplementInnerIndexApply &);


		public:

			// ctor
			explicit
			CXformImplementInnerIndexApply(IMemoryPool *pmp);

			// dtor
			virtual
			~CXformImplementInnerIndexApply()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfImplementInnerIndexApply;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CXformImplementInnerIndexApply";
			}


			// compute xform promise for a given expression handle
			virtual
			EXformPromise Exfp
				(
				CExpressionHandle & // exprhdl
				)
				const
			{
				return ExfpHigh;
			}

			// actual transform
			virtual
			void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const;

	}; // class CXformImplementInnerIndexApply

}

#endif // !GPOPT_CXformImplementInnerIndexApply_H

// EOF
