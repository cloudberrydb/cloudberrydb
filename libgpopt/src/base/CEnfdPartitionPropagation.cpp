//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CEnfdPartitionPropagation.cpp
//
//	@doc:
//		Implementation of enforced partition propagation property
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CEnfdPartitionPropagation.h"
#include "gpopt/base/CReqdPropPlan.h"

#include "gpopt/operators/CPhysical.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CEnfdPartitionPropagation::CEnfdPartitionPropagation
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CEnfdPartitionPropagation::CEnfdPartitionPropagation
	(
	CPartitionPropagationSpec *ppps,
	EPartitionPropagationMatching eppm,
	CPartFilterMap *ppfm
	)
	:
	m_ppps(ppps),
	m_eppm(eppm),
	m_ppfmDerived(ppfm)

{
	GPOS_ASSERT(NULL != ppps);
	GPOS_ASSERT(EppmSentinel > eppm);
	GPOS_ASSERT(NULL != ppfm);
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdPartitionPropagation::~CEnfdPartitionPropagation
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CEnfdPartitionPropagation::~CEnfdPartitionPropagation()
{
	m_ppps->Release();
	m_ppfmDerived->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdPartitionPropagation::UlHash
//
//	@doc:
// 		Hash function
//
//---------------------------------------------------------------------------
ULONG
CEnfdPartitionPropagation::UlHash() const
{
	return m_ppps->UlHash();
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdPartitionPropagation::Epet
//
//	@doc:
// 		Get partition propagation enforcing type for the given operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CEnfdPartitionPropagation::Epet
	(
	CExpressionHandle &exprhdl,
	CPhysical *popPhysical,
	BOOL fPropagationReqd
	)
	const
{
	if (fPropagationReqd)
	{
		return popPhysical->EpetPartitionPropagation(exprhdl, this);
	}

	return EpetUnnecessary;
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdPartitionPropagation::FResolved
//
//	@doc:
// 		Is required partition propagation resolved by the given part index map
//
//---------------------------------------------------------------------------
BOOL 
CEnfdPartitionPropagation::FResolved
	(
	IMemoryPool *pmp,
	CPartIndexMap *ppim
	)
	const
{
	GPOS_ASSERT(NULL != ppim);
	
	CPartIndexMap *ppimReqd = m_ppps->Ppim();
	if (!ppimReqd->FContainsUnresolved())
	{
		return true;
	}
	
	DrgPul *pdrgpulPartIndexIds = ppimReqd->PdrgpulScanIds(pmp);
	const ULONG ulLength = pdrgpulPartIndexIds->UlLength();
			
	BOOL fResolved = true;
	for (ULONG ul = 0; ul < ulLength && fResolved; ul++)
	{
		ULONG ulPartIndexId = *((*pdrgpulPartIndexIds)[ul]);
		GPOS_ASSERT(CPartIndexMap::EpimConsumer == ppimReqd->Epim(ulPartIndexId));
		
		// check whether part index id has been resolved in the derived map
		fResolved = false;
		if (ppim->FContains(ulPartIndexId))
		{
			CPartIndexMap::EPartIndexManipulator epim = ppim->Epim(ulPartIndexId);
			ULONG ulExpectedPropagators = ppim->UlExpectedPropagators(ulPartIndexId);

			fResolved = CPartIndexMap::EpimResolver == epim ||
						CPartIndexMap::EpimPropagator == epim ||
						(CPartIndexMap::EpimConsumer == epim && 0 < ulExpectedPropagators &&
								ppimReqd->UlExpectedPropagators(ulPartIndexId) == ulExpectedPropagators);
		}
	}
	
	// cleanup
	pdrgpulPartIndexIds->Release();
	
	return fResolved;
}

//---------------------------------------------------------------------------
//	@function:
//		CEnfdPartitionPropagation::FInScope
//
//	@doc:
// 		Is required partition propagation in the scope defined by the given part index map
//
//---------------------------------------------------------------------------
BOOL 
CEnfdPartitionPropagation::FInScope
	(
	IMemoryPool *pmp,
	CPartIndexMap *ppim
	)
	const
{
	GPOS_ASSERT(NULL != ppim);
	
	CPartIndexMap *ppimReqd = m_ppps->Ppim();
	
	DrgPul *pdrgpulPartIndexIds = ppimReqd->PdrgpulScanIds(pmp);
	const ULONG ulLength = pdrgpulPartIndexIds->UlLength();

	if (0 == ulLength)
	{
		pdrgpulPartIndexIds->Release();
		return true;
	}
	
	BOOL fInScope = true;
	for (ULONG ul = 0; ul < ulLength && fInScope; ul++)
	{
		ULONG ulPartIndexId = *((*pdrgpulPartIndexIds)[ul]);
		GPOS_ASSERT(CPartIndexMap::EpimConsumer == ppimReqd->Epim(ulPartIndexId));
		
		// check whether part index id exists in the derived part consumers
		fInScope = ppim->FContains(ulPartIndexId);
	}
	
	// cleanup
	pdrgpulPartIndexIds->Release();
	
	return fInScope;
}

//---------------------------------------------------------------------------
//	@function:
//		CEnfdPartitionPropagation::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CEnfdPartitionPropagation::OsPrint
	(
	IOstream &os
	)
	const
{
	return os << (*m_ppps) << " match: " << SzPropagationMatching(m_eppm) << " ";
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdPartitionPropagation::SzPropagationMatching
//
//	@doc:
//		Propagation matching string
//
//---------------------------------------------------------------------------
const CHAR *
CEnfdPartitionPropagation::SzPropagationMatching
	(
	EPartitionPropagationMatching eppm
	)
{
	GPOS_ASSERT(EppmSentinel > eppm);
	const CHAR *rgszPropagationMatching[EppmSentinel] =
	{
		"satisfy"
	};
	
	return rgszPropagationMatching[eppm];
}

// EOF
