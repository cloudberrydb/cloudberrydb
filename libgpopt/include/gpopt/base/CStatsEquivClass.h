//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CStatsEquivClass.h
//
//	@doc:
//		Equivalence classes between columns for the purpose of statistic
//		computation
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CStatsEquivClass_H
#define GPOPT_CStatsEquivClass_H

#include "gpos/base.h"
#include "gpopt/base/CConstraint.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CStatsEquivClass
	//
	//	@doc:
	//		Equivalence classes between columns for the purpose of statistic
	//
	//---------------------------------------------------------------------------
	class CStatsEquivClass
	{
			// create equivalence classes from scalar comparison
			static
			DrgPcrs *PdrgpcrsEquivClassFromScalarCmp(IMemoryPool *pmp, CExpression *pexpr);

			// create equivalence classes from scalar boolean expression
			static
			DrgPcrs *PdrgpcrsEquivClassFromScalarBoolOp(IMemoryPool *pmp, CExpression *pexpr);

		public:

			// create equivalence classes from a given scalar expression
			static
			DrgPcrs *PdrgpcrsEquivClassFromScalarExpr(IMemoryPool *pmp, CExpression *pexpr);

			// create a new equivalence class between the two column references
			static
			CColRefSet *PcrsEquivClass(IMemoryPool *pmp, const CColRef *pcrLeft, const CColRef *pcrRight);

	}; // class CStatsEquivClass

}

#endif // !GPOPT_CStatsEquivClass_H

// EOF
