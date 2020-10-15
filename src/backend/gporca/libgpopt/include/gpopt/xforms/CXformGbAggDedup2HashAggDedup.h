//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformGbAggDedup2HashAggDedup.h
//
//	@doc:
//		Transform GbAggDeduplicate to HashAggDeduplicate
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformGbAggDedup2HashAggDedup_H
#define GPOPT_CXformGbAggDedup2HashAggDedup_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformGbAgg2HashAgg.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformGbAggDedup2HashAggDedup
//
//	@doc:
//		Transform GbAggDeduplicate to HashAggDeduplicate
//
//---------------------------------------------------------------------------
class CXformGbAggDedup2HashAggDedup : public CXformGbAgg2HashAgg
{
private:
	CXformGbAggDedup2HashAggDedup(const CXformGbAggDedup2HashAggDedup &) =
		delete;

public:
	// ctor
	CXformGbAggDedup2HashAggDedup(CMemoryPool *mp);

	// dtor
	virtual ~CXformGbAggDedup2HashAggDedup() = default;

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfGbAggDedup2HashAggDedup;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformGbAggDedup2HashAggDedup";
	}

	// actual transform
	virtual void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
						   CExpression *pexpr) const;

};	// class CXformGbAggDedup2HashAggDedup

}  // namespace gpopt


#endif	// !GPOPT_CXformGbAggDedup2HashAggDedup_H

// EOF
