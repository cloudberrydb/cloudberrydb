//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDistributionSpecSingleton.cpp
//
//	@doc:
//		Specification of singleton distribution
//---------------------------------------------------------------------------

#include "naucrates/traceflags/traceflags.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/operators/CPhysicalMotionGather.h"


using namespace gpopt;


// initialization of static variables
const CHAR *CDistributionSpecSingleton::m_szSegmentType[EstSentinel] =
{
	"master",
	"segment"
};


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecSingleton::CDistributionSpecSingleton
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDistributionSpecSingleton::CDistributionSpecSingleton
	(
	ESegmentType est
	)
	:
	m_est(est)
{
	GPOS_ASSERT(EstSentinel != est);
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecSingleton::FSatisfies
//
//	@doc:
//		Check if this distribution spec satisfies the given one
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecSingleton::FSatisfies
	(
	const CDistributionSpec *pds
	)
	const
{	
	if (FMatch(pds))
	{
		// exact match implies satisfaction
		return true;
	 }

	if (EdtNonSingleton == pds->Edt())
	{
		// singleton does not satisfy non-singleton requirements
		return false;
	}

	if (EdtAny == pds->Edt())
	{
		// a singleton distribution satisfies "any" distributions
		return true;
	}
	
	if (EdtHashed == pds->Edt() &&
		CDistributionSpecHashed::PdsConvert(pds)->FSatisfiedBySingleton())
	{
		// a singleton distribution satisfies hashed distributions, if the hashed distribution allows satisfaction
		return true;
	}

	return (EdtSingleton == pds->Edt() && m_est == ((CDistributionSpecSingleton *) pds)->Est());
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecSingleton::AppendEnforcers
//
//	@doc:
//		Add required enforcers to dynamic array
//
//---------------------------------------------------------------------------
void
CDistributionSpecSingleton::AppendEnforcers
	(
	IMemoryPool *pmp,
	CExpressionHandle &, // exprhdl
	CReqdPropPlan *prpp,
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


	if (GPOS_FTRACE(EopttraceDisableMotionGather))
	{
		// gather Motion is disabled
		return;
	}

	pexpr->AddRef();
	CExpression *pexprMotion = GPOS_NEW(pmp) CExpression
										(
										pmp,
										GPOS_NEW(pmp) CPhysicalMotionGather(pmp, m_est),
										pexpr
										);
	pdrgpexpr->Append(pexprMotion);

	if (!prpp->Peo()->PosRequired()->FEmpty() &&
	    CDistributionSpecSingleton::EstMaster == m_est)
	{
		COrderSpec *pos = prpp->Peo()->PosRequired();
		pos->AddRef();
		pexpr->AddRef();
		
		CExpression *pexprGatherMerge = GPOS_NEW(pmp) CExpression
													(
													pmp,
													GPOS_NEW(pmp) CPhysicalMotionGather(pmp, m_est, pos),
													pexpr
													);
		pdrgpexpr->Append(pexprGatherMerge);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecSingleton::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CDistributionSpecSingleton::OsPrint
	(
	IOstream &os
	)
	const
{
	return os << "SINGLETON (" << m_szSegmentType[m_est] << ")";
}


// EOF

