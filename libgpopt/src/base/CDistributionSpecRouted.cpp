//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDistributionSpecRouted.cpp
//
//	@doc:
//		Specification of routed distribution
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/traceflags/traceflags.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CDistributionSpecRouted.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CPhysicalMotionBroadcast.h"
#include "gpopt/operators/CPhysicalMotionRoutedDistribute.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::CDistributionSpecRouted
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDistributionSpecRouted::CDistributionSpecRouted
	(
	CColRef *pcrSegmentId
	)
	:
	m_pcrSegmentId(pcrSegmentId)
{
	GPOS_ASSERT(NULL != pcrSegmentId);
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::~CDistributionSpecRouted
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDistributionSpecRouted::~CDistributionSpecRouted()
{
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::FSatisfies
//
//	@doc:
//		Check if this distribution spec satisfies the given one
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecRouted::FSatisfies
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

	if (EdtAny == pds->Edt())
	{
		// routed distribution satisfies the "any" distribution requirement
		return true;
	}
	
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::PdsCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the distribution spec with remapped columns
//
//---------------------------------------------------------------------------
CDistributionSpec *
CDistributionSpecRouted::PdsCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	ULONG ulId = m_pcrSegmentId->UlId();
	CColRef *pcrSegmentId = phmulcr->PtLookup(&ulId);
	if (NULL == pcrSegmentId)
	{
		if (fMustExist)
		{
			// not found in hashmap, so create a new colref and add to hashmap
			CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();
			pcrSegmentId = pcf->PcrCopy(m_pcrSegmentId);

#ifdef GPOS_DEBUG
			BOOL fResult =
#endif // GPOS_DEBUG
			phmulcr->FInsert(GPOS_NEW(pmp) ULONG(ulId), pcrSegmentId);
			GPOS_ASSERT(fResult);
		}
		else
		{
			pcrSegmentId = m_pcrSegmentId;
		}
	}

	return GPOS_NEW(pmp) CDistributionSpecRouted(pcrSegmentId);
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::AppendEnforcers
//
//	@doc:
//		Add required enforcers to dynamic array
//
//---------------------------------------------------------------------------
void
CDistributionSpecRouted::AppendEnforcers
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

	if (GPOS_FTRACE(EopttraceDisableMotionRountedDistribute))
	{
		// routed-distribute Motion is disabled
		return;
	}

	// add a routed distribution enforcer
	AddRef();
	pexpr->AddRef();
	CExpression *pexprMotion = GPOS_NEW(pmp) CExpression
										(
										pmp,
										GPOS_NEW(pmp) CPhysicalMotionRoutedDistribute(pmp, this),
										pexpr
										);
	pdrgpexpr->Append(pexprMotion);
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG 
CDistributionSpecRouted::UlHash() const
{
	return gpos::UlCombineHashes((ULONG) Edt(), gpos::UlHashPtr<CColRef>(m_pcrSegmentId));
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::PcrsUsed
//
//	@doc:
//		Extract columns used by the distribution spec
//
//---------------------------------------------------------------------------
CColRefSet *
CDistributionSpecRouted::PcrsUsed
	(
	IMemoryPool *pmp
	)
	const
{
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrs->Include(m_pcrSegmentId);

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::FMatch
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL 
CDistributionSpecRouted::FMatch
	(
	const CDistributionSpec *pds
	) 
	const
{
	if (Edt() != pds->Edt())
	{
		return false;
	}

	const CDistributionSpecRouted *pdsRouted = CDistributionSpecRouted::PdsConvert(pds);
	return m_pcrSegmentId == pdsRouted->Pcr();
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CDistributionSpecRouted::OsPrint
	(
	IOstream &os
	)
	const
{
	os << "ROUTED: [ ";
	m_pcrSegmentId->OsPrint(os);
	return os <<  " ]";
}

// EOF

