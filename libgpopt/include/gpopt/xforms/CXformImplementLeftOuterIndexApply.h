//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal, Inc.
//
//	Implementing Left Outer Index Apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementLeftOuterIndexApply_H
#define GPOPT_CXformImplementLeftOuterIndexApply_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformImplementation.h"


namespace gpopt
{
	using namespace gpos;

	class CXformImplementLeftOuterIndexApply : public CXformImplementation
	{

		private:

			// private copy ctor
			CXformImplementLeftOuterIndexApply(const CXformImplementLeftOuterIndexApply &);

		public:

			// ctor
			explicit
			CXformImplementLeftOuterIndexApply(IMemoryPool *pmp);

			// dtor
			virtual
			~CXformImplementLeftOuterIndexApply()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfImplementLeftOuterIndexApply;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CXformImplementLeftOuterIndexApply";
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

	}; // class CXformImplementLeftOuterIndexApply

}

#endif // !GPOPT_CXformImplementLeftOuterIndexApply_H

// EOF
