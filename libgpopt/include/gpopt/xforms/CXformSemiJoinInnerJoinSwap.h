//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSemiJoinInnerJoinSwap.h
//
//	@doc:
//		Swap cascaded semi-join and inner join
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSemiJoinInnerJoinSwap_H
#define GPOPT_CXformSemiJoinInnerJoinSwap_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformJoinSwap.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformSemiJoinInnerJoinSwap
	//
	//	@doc:
	//		Swap cascaded semi-join and inner join
	//
	//---------------------------------------------------------------------------
	class CXformSemiJoinInnerJoinSwap : public CXformJoinSwap<CLogicalLeftSemiJoin, CLogicalInnerJoin>
	{

		private:

			// private copy ctor
			CXformSemiJoinInnerJoinSwap(const CXformSemiJoinInnerJoinSwap &);

		public:

			// ctor
			explicit
			CXformSemiJoinInnerJoinSwap
				(
				IMemoryPool *pmp
				)
				:
				CXformJoinSwap<CLogicalLeftSemiJoin, CLogicalInnerJoin>(pmp)
			{}

			// dtor
			virtual
			~CXformSemiJoinInnerJoinSwap()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfSemiJoinInnerJoinSwap;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CXformSemiJoinInnerJoinSwap";
			}

	}; // class CXformSemiJoinInnerJoinSwap

}

#endif // !GPOPT_CXformSemiJoinInnerJoinSwap_H

// EOF
