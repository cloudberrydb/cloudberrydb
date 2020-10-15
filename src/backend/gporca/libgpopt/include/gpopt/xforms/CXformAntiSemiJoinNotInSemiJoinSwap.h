//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformAntiSemiJoinNotInSemiJoinSwap.h
//
//	@doc:
//		Swap cascaded anti semi-join (NotIn) and semi-join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformAntiSemiJoinNotInSemiJoinSwap_H
#define GPOPT_CXformAntiSemiJoinNotInSemiJoinSwap_H

#include "gpos/base.h"

#include "gpopt/operators/CLogicalLeftAntiSemiJoinNotIn.h"
#include "gpopt/operators/CLogicalLeftSemiJoin.h"
#include "gpopt/xforms/CXformJoinSwap.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformAntiSemiJoinNotInSemiJoinSwap
//
//	@doc:
//		Swap cascaded anti semi-join (NotIn) and semi-join
//
//---------------------------------------------------------------------------
class CXformAntiSemiJoinNotInSemiJoinSwap
	: public CXformJoinSwap<CLogicalLeftAntiSemiJoinNotIn, CLogicalLeftSemiJoin>
{
private:
	CXformAntiSemiJoinNotInSemiJoinSwap(
		const CXformAntiSemiJoinNotInSemiJoinSwap &) = delete;

public:
	// ctor
	explicit CXformAntiSemiJoinNotInSemiJoinSwap(CMemoryPool *mp)
		: CXformJoinSwap<CLogicalLeftAntiSemiJoinNotIn, CLogicalLeftSemiJoin>(
			  mp)
	{
	}

	// dtor
	virtual ~CXformAntiSemiJoinNotInSemiJoinSwap() = default;

	// Compatibility function
	virtual BOOL
	FCompatible(CXform::EXformId exfid)
	{
		return ExfSemiJoinAntiSemiJoinNotInSwap != exfid;
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfAntiSemiJoinNotInSemiJoinSwap;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformAntiSemiJoinNotInSemiJoinSwap";
	}

};	// class CXformAntiSemiJoinNotInSemiJoinSwap

}  // namespace gpopt

#endif	// !GPOPT_CXformAntiSemiJoinNotInSemiJoinSwap_H

// EOF
