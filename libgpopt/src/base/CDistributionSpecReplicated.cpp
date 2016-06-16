//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDistributionSpecReplicated.cpp
//
//	@doc:
//		Specification of replicated distribution
//---------------------------------------------------------------------------

#include "naucrates/traceflags/traceflags.h"
#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/base/CDistributionSpecNonSingleton.h"
#include "gpopt/operators/CPhysicalMotionBroadcast.h"

#define GPOPT_DISTR_SPEC_COLREF_HASHED      (ULONG(5))
#define GPOPT_DISTR_SPEC_COLREF_MATCH_NL    (ULONG(20))

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecReplicated::FSatisfies
//
//	@doc:
//		Check if this distribution satisfy the given one
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecReplicated::FSatisfies
	(
	const CDistributionSpec *pdss
	)
	const
{
	if (FMatch(pdss))
	{
		// exact match implies satisfaction
		return true;
	 }

	if (EdtNonSingleton == pdss->Edt())
	{
		// a replicated distribution satisfies the non-singleton
		// distribution, only if allowed by non-singleton distribution object
		return CDistributionSpecNonSingleton::PdsConvert(const_cast<CDistributionSpec *>(pdss))->FAllowReplicated();
	}

	// a replicated distribution satisfies any non-singleton one,
	// as well as singleton distributions that are not master-only
	return !(EdtSingleton == pdss->Edt() &&
		   (dynamic_cast<const CDistributionSpecSingleton *>(pdss))->FOnMaster());
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecReplicated::AppendEnforcers
//
//	@doc:
//		Add required enforcers to dynamic array
//
//---------------------------------------------------------------------------
void
CDistributionSpecReplicated::AppendEnforcers
	(
	IMemoryPool *pmp,
	CExpressionHandle &, // exprhdl
	CReqdPropPlan *
	#ifdef GPOS_DEBUG
	prpp
	#endif // GPOS_DEBUG
	,
	DrgPexpr *pdrgpexpr, 
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(!GPOS_FTRACE(EopttraceDisableMotions));
	GPOS_ASSERT(this == prpp->Ped()->PdsRequired() &&
				"required plan properties don't match enforced distribution spec");

	if (GPOS_FTRACE(EopttraceDisableMotionBroadcast))
	{
		// broadcast Motion is disabled
		return;
	}

	pexpr->AddRef();
	CExpression *pexprMotion = GPOS_NEW(pmp) CExpression
										(
										pmp,
										GPOS_NEW(pmp) CPhysicalMotionBroadcast(pmp),
										pexpr
										);
	pdrgpexpr->Append(pexprMotion);
}

// EOF
