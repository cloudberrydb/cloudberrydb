//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformSemiJoinAntiSemiJoinNotInSwap.h
//
//	@doc:
//		Swap cascaded semi-join and anti semi-join with NotIn semantics
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSemiJoinAntiSemiJoinNotInSwap_H
#define GPOPT_CXformSemiJoinAntiSemiJoinNotInSwap_H

#include "gpos/base.h"

#include "gpopt/operators/CLogicalLeftAntiSemiJoinNotIn.h"
#include "gpopt/operators/CLogicalLeftSemiJoin.h"
#include "gpopt/xforms/CXformJoinSwap.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformSemiJoinAntiSemiJoinNotInSwap
//
//	@doc:
//		Swap cascaded semi-join and anti semi-join with NotIn semantics
//
//---------------------------------------------------------------------------
class CXformSemiJoinAntiSemiJoinNotInSwap
	: public CXformJoinSwap<CLogicalLeftSemiJoin, CLogicalLeftAntiSemiJoinNotIn>
{
private:
public:
	CXformSemiJoinAntiSemiJoinNotInSwap(
		const CXformSemiJoinAntiSemiJoinNotInSwap &) = delete;

	// ctor
	explicit CXformSemiJoinAntiSemiJoinNotInSwap(CMemoryPool *mp)
		: CXformJoinSwap<CLogicalLeftSemiJoin, CLogicalLeftAntiSemiJoinNotIn>(
			  mp)
	{
	}

	// dtor
	virtual ~CXformSemiJoinAntiSemiJoinNotInSwap() = default;

	// Compatibility function
	virtual BOOL
	FCompatible(CXform::EXformId exfid)
	{
		return ExfAntiSemiJoinNotInSemiJoinSwap != exfid;
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfSemiJoinAntiSemiJoinNotInSwap;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformSemiJoinAntiSemiJoinNotInSwap";
	}

};	// class CXformSemiJoinAntiSemiJoinNotInSwap

}  // namespace gpopt

#endif	// !GPOPT_CXformSemiJoinAntiSemiJoinNotInSwap_H

// EOF
