//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp
//
//	@filename:
//		CXformImplementRowTrigger.h
//
//	@doc:
//		Transform Logical Row Trigger to Physical Row Trigger
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementRowTrigger_H
#define GPOPT_CXformImplementRowTrigger_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformImplementRowTrigger
//
//	@doc:
//		Transform Logical RowTrigger to Physical RowTrigger
//
//---------------------------------------------------------------------------
class CXformImplementRowTrigger : public CXformImplementation
{
private:
public:
	CXformImplementRowTrigger(const CXformImplementRowTrigger &) = delete;

	// ctor
	explicit CXformImplementRowTrigger(CMemoryPool *mp);

	// dtor
	virtual ~CXformImplementRowTrigger() = default;

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfImplementRowTrigger;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformImplementRowTrigger";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformImplementRowTrigger
}  // namespace gpopt

#endif	// !GPOPT_CXformImplementRowTrigger_H

// EOF
