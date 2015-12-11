//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformAntiSemiJoinNotInAntiSemiJoinSwap.h
//
//	@doc:
//		Swap cascaded anti semi-join (NotIn) and anti semi-join
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformAntiSemiJoinNotInAntiSemiJoinSwap_H
#define GPOPT_CXformAntiSemiJoinNotInAntiSemiJoinSwap_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformJoinSwap.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformAntiSemiJoinNotInAntiSemiJoinSwap
	//
	//	@doc:
	//		Swap cascaded anti semi-join (NotIn) and anti semi-join
	//
	//---------------------------------------------------------------------------
	class CXformAntiSemiJoinNotInAntiSemiJoinSwap : public CXformJoinSwap<CLogicalLeftAntiSemiJoinNotIn, CLogicalLeftAntiSemiJoin>
	{

		private:

			// private copy ctor
			CXformAntiSemiJoinNotInAntiSemiJoinSwap(const CXformAntiSemiJoinNotInAntiSemiJoinSwap &);

		public:

			// ctor
			explicit
			CXformAntiSemiJoinNotInAntiSemiJoinSwap
				(
				IMemoryPool *pmp
				)
				:
				CXformJoinSwap<CLogicalLeftAntiSemiJoinNotIn, CLogicalLeftAntiSemiJoin>(pmp)
			{}

			// dtor
			virtual
			~CXformAntiSemiJoinNotInAntiSemiJoinSwap()
			{}

			// Compatibility function
			virtual
			BOOL FCompatible
				(
				CXform::EXformId exfid
				)
			{
				return ExfAntiSemiJoinAntiSemiJoinNotInSwap != exfid;
			}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfAntiSemiJoinNotInAntiSemiJoinSwap;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CXformAntiSemiJoinNotInAntiSemiJoinSwap";
			}

	}; // class CXformAntiSemiJoinNotInAntiSemiJoinSwap

}

#endif // !GPOPT_CXformAntiSemiJoinNotInAntiSemiJoinSwap_H

// EOF
