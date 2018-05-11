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
	CMDAccessor *md_accessor,
	IMDId *mdid_func
	)
{
	if (md_accessor->FAggWindowFunc(mdid_func))
	{
		const IMDAggregate *pmdagg = md_accessor->RetrieveAgg(mdid_func);
		
		return pmdagg->Mdname().GetMDName();
	}

	const IMDFunction *pmdfunc = md_accessor->RetrieveFunc(mdid_func);

	return pmdfunc->Mdname().GetMDName();
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
	CMDAccessor *md_accessor,
	IMDId *mdid_func
	)
{

	if (md_accessor->FAggWindowFunc(mdid_func))
	{
		const IMDAggregate *pmdagg = md_accessor->RetrieveAgg(mdid_func);
		return pmdagg->GetResultTypeMdid();
	}

	const IMDFunction *pmdfunc = md_accessor->RetrieveFunc(mdid_func);
	return pmdfunc->GetResultTypeMdid();
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
	CMDAccessor *md_accessor,
	IMDId *left_mdid,
	IMDId *right_mdid,
	IMDType::ECmpType cmp_type
	)
{
	GPOS_ASSERT(NULL != md_accessor);
	GPOS_ASSERT(NULL != left_mdid);
	GPOS_ASSERT(NULL != left_mdid);
	GPOS_ASSERT(IMDType::EcmptOther > cmp_type);

	CAutoTraceFlag atf1(EtraceSimulateOOM, false);
	CAutoTraceFlag atf2(EtraceSimulateAbort, false);
	CAutoTraceFlag atf3(EtraceSimulateIOError, false);
	CAutoTraceFlag atf4(EtraceSimulateNetError, false);

	if (left_mdid->Equals(right_mdid))
	{
		const IMDType *pmdtypeLeft = md_accessor->RetrieveType(left_mdid);
		return IMDId::IsValid(pmdtypeLeft->GetMdidForCmpType(cmp_type));
	}

	GPOS_TRY
	{
		(void) md_accessor->Pmdsccmp(left_mdid, right_mdid, cmp_type);

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
	CMDAccessor *md_accessor,
	IMDId *mdid_src,
	IMDId *mdid_dest
	)
{
	GPOS_ASSERT(NULL != md_accessor);
	GPOS_ASSERT(NULL != mdid_src);
	GPOS_ASSERT(NULL != mdid_dest);

	CAutoTraceFlag atf1(EtraceSimulateOOM, false);
	CAutoTraceFlag atf2(EtraceSimulateAbort, false);
	CAutoTraceFlag atf3(EtraceSimulateIOError, false);
	CAutoTraceFlag atf4(EtraceSimulateNetError, false);

	GPOS_TRY
	{
		(void) md_accessor->Pmdcast(mdid_src, mdid_dest);

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
	CMDAccessor *md_accessor,
	IMDId *mdid_op
	)
{
	GPOS_ASSERT(NULL != md_accessor);

	if (NULL == mdid_op || !mdid_op->IsValid())
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
		const IMDScalarOp *md_scalar_op = md_accessor->RetrieveScOp(mdid_op);

		return md_scalar_op->ReturnsNullOnNullInput();
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
	CMDAccessor *md_accessor,
	IMDId *mdid_type
	)
{
	GPOS_ASSERT(NULL != md_accessor);

	if (NULL != mdid_type && mdid_type->IsValid())
	{
		return (IMDType::EtiBool == md_accessor->RetrieveType(mdid_type)->GetDatumType());
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
	CMDAccessor *md_accessor,
	IMDId *mdid_op
	)
{
	GPOS_ASSERT(NULL != md_accessor);
	GPOS_ASSERT(NULL != mdid_op);

	const IMDScalarOp *md_scalar_op = md_accessor->RetrieveScOp(mdid_op);

	return mdid_op->Equals(md_scalar_op->GetCommuteOpMdid());
}


// EOF
