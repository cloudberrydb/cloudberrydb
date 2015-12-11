//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformInnerJoinSemiJoinSwap.h
//
//	@doc:
//		Swap cascaded inner join and semi-join
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformInnerJoinSemiJoinSwap_H
#define GPOPT_CXformInnerJoinSemiJoinSwap_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformJoinSwap.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformInnerJoinSemiJoinSwap
	//
	//	@doc:
	//		Swap cascaded inner join and semi-join
	//
	//---------------------------------------------------------------------------
	class CXformInnerJoinSemiJoinSwap : public CXformJoinSwap<CLogicalInnerJoin, CLogicalLeftSemiJoin>
	{

		private:

			// private copy ctor
			CXformInnerJoinSemiJoinSwap(const CXformInnerJoinSemiJoinSwap &);

		public:

			// ctor
			explicit
			CXformInnerJoinSemiJoinSwap
				(
				IMemoryPool *pmp
				)
				:
				CXformJoinSwap<CLogicalInnerJoin, CLogicalLeftSemiJoin>(pmp)
			{}

			// dtor
			virtual
			~CXformInnerJoinSemiJoinSwap()
			{}

			// Compatibility function
			virtual
			BOOL FCompatible
				(
				CXform::EXformId exfid
				)
			{
				return ExfSemiJoinInnerJoinSwap != exfid;
			}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfInnerJoinSemiJoinSwap;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CXformInnerJoinSemiJoinSwap";
			}

	}; // class CXformInnerJoinSemiJoinSwap

}

#endif // !GPOPT_CXformInnerJoinSemiJoinSwap_H

// EOF
