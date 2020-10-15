//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformPushGbBelowJoin.h
//
//	@doc:
//		Push group by below join transform
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformPushGbBelowJoin_H
#define GPOPT_CXformPushGbBelowJoin_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformPushGbBelowJoin
//
//	@doc:
//		Push group by below join transform
//
//---------------------------------------------------------------------------
class CXformPushGbBelowJoin : public CXformExploration
{
private:
	CXformPushGbBelowJoin(const CXformPushGbBelowJoin &) = delete;

public:
	// ctor
	explicit CXformPushGbBelowJoin(CMemoryPool *mp);

	// ctor
	explicit CXformPushGbBelowJoin(CExpression *pexprPattern);

	// dtor
	virtual ~CXformPushGbBelowJoin() = default;

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfPushGbBelowJoin;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformPushGbBelowJoin";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformPushGbBelowJoin

}  // namespace gpopt

#endif	// !GPOPT_CXformPushGbBelowJoin_H

// EOF
