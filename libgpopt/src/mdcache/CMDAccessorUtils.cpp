//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDAccessorUtils.cpp
//
//	@doc:
//		Implementation of the utility function associated with the metadata
//		accessor
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/error/CException.h"
#include "gpos/error/CErrorHandlerStandard.h"
#include "gpos/task/CAutoTraceFlag.h"

#include "gpopt/mdcache/CMDAccessorUtils.h"

#include "naucrates/exception.h"
#include "naucrates/dxl/gpdb_types.h"

#include "naucrates/md/IMDAggregate.h"
#include "naucrates/md/IMDFunction.h"
#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/CMDIdGPDB.h"

using namespace gpmd;
using namespace gpos;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorUtils::PstrWindowFuncName
//
//	@doc:
//		Return the name of the window operation
//
//---------------------------------------------------------------------------
const CWStringConst *
CMDAccessorUtils::PstrWindowFuncName
	(
	CMDAccessor *pmda,
	IMDId *pmdidFunc
	)
{
	if (pmda->FAggWindowFunc(pmdidFunc))
	{
		const IMDAggregate *pmdagg = pmda->Pmdagg(pmdidFunc);
		
		return pmdagg->Mdname().Pstr();
	}

	const IMDFunction *pmdfunc = pmda->Pmdfunc(pmdidFunc);

	return pmdfunc->Mdname().Pstr();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorUtils::PmdidWindowReturnType
//
//	@doc:
//		Return the return type of the window function
//
//---------------------------------------------------------------------------
IMDId *
CMDAccessorUtils::PmdidWindowReturnType
	(
	CMDAccessor *pmda,
	IMDId *pmdidFunc
	)
{

	if (pmda->FAggWindowFunc(pmdidFunc))
	{
		const IMDAggregate *pmdagg = pmda->Pmdagg(pmdidFunc);
		return pmdagg->PmdidTypeResult();
	}

	const IMDFunction *pmdfunc = pmda->Pmdfunc(pmdidFunc);
	return pmdfunc->PmdidTypeResult();
}


//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorUtils::FCmpExists
//
//	@doc:
//		Does a scalar comparison object between given types exists
//
//---------------------------------------------------------------------------
BOOL
CMDAccessorUtils::FCmpExists
	(
	CMDAccessor *pmda,
	IMDId *pmdidLeft,
	IMDId *pmdidRight,
	IMDType::ECmpType ecmpt
	)
{
	GPOS_ASSERT(NULL != pmda);
	GPOS_ASSERT(NULL != pmdidLeft);
	GPOS_ASSERT(NULL != pmdidLeft);
	GPOS_ASSERT(IMDType::EcmptOther > ecmpt);

	CAutoTraceFlag atf1(EtraceSimulateOOM, false);
	CAutoTraceFlag atf2(EtraceSimulateAbort, false);
	CAutoTraceFlag atf3(EtraceSimulateIOError, false);
	CAutoTraceFlag atf4(EtraceSimulateNetError, false);

	if (pmdidLeft->FEquals(pmdidRight))
	{
		const IMDType *pmdtypeLeft = pmda->Pmdtype(pmdidLeft);
		return IMDId::FValid(pmdtypeLeft->PmdidCmp(ecmpt));
	}

	GPOS_TRY
	{
		(void) pmda->Pmdsccmp(pmdidLeft, pmdidRight, ecmpt);

		return true;
	}
	GPOS_CATCH_EX(ex)
	{
		GPOS_ASSERT(GPOS_MATCH_EX(ex, gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound));
		GPOS_RESET_EX;

		return false;
	}
	GPOS_CATCH_END;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorUtils::FCastExists
//
//	@doc:
//		Does if a cast object between given source and destination types exists
//
//---------------------------------------------------------------------------
BOOL
CMDAccessorUtils::FCastExists
	(
	CMDAccessor *pmda,
	IMDId *pmdidSrc,
	IMDId *pmdidDest
	)
{
	GPOS_ASSERT(NULL != pmda);
	GPOS_ASSERT(NULL != pmdidSrc);
	GPOS_ASSERT(NULL != pmdidDest);

	CAutoTraceFlag atf1(EtraceSimulateOOM, false);
	CAutoTraceFlag atf2(EtraceSimulateAbort, false);
	CAutoTraceFlag atf3(EtraceSimulateIOError, false);
	CAutoTraceFlag atf4(EtraceSimulateNetError, false);

	GPOS_TRY
	{
		(void) pmda->Pmdcast(pmdidSrc, pmdidDest);

		return true;
	}
	GPOS_CATCH_EX(ex)
	{
		GPOS_ASSERT(GPOS_MATCH_EX(ex, gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound));
		GPOS_RESET_EX;

		return false;
	}
	GPOS_CATCH_END;
}


//---------------------------------------------------------------------------
//	@function:
//		CUtils::FScalarOpReturnsNullOnNullInput
//
//	@doc:
//		Does scalar operator return NULL on NULL input?
//
//---------------------------------------------------------------------------
BOOL
CMDAccessorUtils::FScalarOpReturnsNullOnNullInput
	(
	CMDAccessor *pmda,
	IMDId *pmdidOp
	)
{
	GPOS_ASSERT(NULL != pmda);

	if (NULL == pmdidOp || !pmdidOp->FValid())
	{
		// invalid mdid
		return false;
	}

	CAutoTraceFlag atf1(EtraceSimulateOOM, false);
	CAutoTraceFlag atf2(EtraceSimulateAbort, false);
	CAutoTraceFlag atf3(EtraceSimulateIOError, false);
	CAutoTraceFlag atf4(EtraceSimulateNetError, false);

	GPOS_TRY
	{
		const IMDScalarOp *pmdscop = pmda->Pmdscop(pmdidOp);

		return pmdscop->FReturnsNullOnNullInput();
	}
	GPOS_CATCH_EX(ex)
	{
		GPOS_ASSERT(GPOS_MATCH_EX(ex, gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound));
		GPOS_RESET_EX;
	}
	GPOS_CATCH_END;

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CUtils::FBoolType
//
//	@doc:
//		Return True if passed mdid is for BOOL type
//
//---------------------------------------------------------------------------
BOOL
CMDAccessorUtils::FBoolType
	(
	CMDAccessor *pmda,
	IMDId *pmdidType
	)
{
	GPOS_ASSERT(NULL != pmda);

	if (NULL != pmdidType && pmdidType->FValid())
	{
		return (IMDType::EtiBool == pmda->Pmdtype(pmdidType)->Eti());
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorUtils::FCommutativeScalarOp
//
//	@doc:
//		Is scalar operator commutative? This can be used with ScalarOp and ScalarCmp
//
//---------------------------------------------------------------------------
BOOL
CMDAccessorUtils::FCommutativeScalarOp
	(
	CMDAccessor *pmda,
	IMDId *pmdidOp
	)
{
	GPOS_ASSERT(NULL != pmda);
	GPOS_ASSERT(NULL != pmdidOp);

	const IMDScalarOp *pmdscop = pmda->Pmdscop(pmdidOp);

	return pmdidOp->FEquals(pmdscop->PmdidOpCommute());
}


// EOF
