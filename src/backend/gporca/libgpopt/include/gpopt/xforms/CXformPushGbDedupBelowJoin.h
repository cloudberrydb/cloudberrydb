//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformPushGbDedupBelowJoin.h
//
//	@doc:
//		Push dedup group by below join transform
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformPushGbDedupBelowJoin_H
#define GPOPT_CXformPushGbDedupBelowJoin_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformPushGbBelowJoin.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformPushGbDedupBelowJoin
//
//	@doc:
//		Push dedup group by below join transform
//
//---------------------------------------------------------------------------
class CXformPushGbDedupBelowJoin : public CXformPushGbBelowJoin
{
private:
public:
	CXformPushGbDedupBelowJoin(const CXformPushGbDedupBelowJoin &) = delete;

	// ctor
	explicit CXformPushGbDedupBelowJoin(CMemoryPool *mp);

	// dtor
	virtual ~CXformPushGbDedupBelowJoin() = default;

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfPushGbDedupBelowJoin;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformPushGbDedupBelowJoin";
	}

};	// class CXformPushGbDedupBelowJoin

}  // namespace gpopt

#endif	// !GPOPT_CXformPushGbDedupBelowJoin_H

// EOF
