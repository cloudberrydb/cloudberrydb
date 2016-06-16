//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformExpandNAryJoin.h
//
//	@doc:
//		Expand n-ary join into series of binary joins
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformExpandNAryJoin_H
#define GPOPT_CXformExpandNAryJoin_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformExpandNAryJoin
	//
	//	@doc:
	//		Expand n-ary join into series of binary joins
	//
	//---------------------------------------------------------------------------
	class CXformExpandNAryJoin : public CXformExploration
	{

		private:

			// private copy ctor
			CXformExpandNAryJoin(const CXformExpandNAryJoin &);

			//	expand NAry join in the specified order of inputs
			static
			void AddSpecifiedJoinOrder(IMemoryPool *pmp, CExpression *pexpr, CXformResult *pxfres);

		public:

			// ctor
			explicit
			CXformExpandNAryJoin(IMemoryPool *pmp);

			// dtor
			virtual
			~CXformExpandNAryJoin() 
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfExpandNAryJoin;
			}

			// return a string for xform name
			virtual
			const CHAR *SzId() const
			{
				return "CXformExpandNAryJoin";
			}

			// compute xform promise for a given expression handle
			virtual
			EXformPromise Exfp(CExpressionHandle &exprhdl) const;

			// actual transform
			void Transform
					(
					CXformContext *pxfctxt,
					CXformResult *pxfres,
					CExpression *pexpr
					) const;

	}; // class CXformExpandNAryJoin

}


#endif // !GPOPT_CXformExpandNAryJoin_H

// EOF
