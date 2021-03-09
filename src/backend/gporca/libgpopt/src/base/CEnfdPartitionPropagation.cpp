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

#include "gpopt/base/CEnfdPartitionPropagation.h"

#include "gpos/base.h"

#include "gpopt/base/CReqdPropPlan.h"
#include "gpopt/base/CUtils.h"
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
CEnfdPartitionPropagation::CEnfdPartitionPropagation(
	CPartitionPropagationSpec *ppps, EPartitionPropagationMatching eppm)
	: m_ppps(ppps), m_eppm(eppm)

{
	GPOS_ASSERT(nullptr != ppps);
	GPOS_ASSERT(EppmSentinel > eppm);
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
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdPartitionPropagation::HashValue
//
//	@doc:
// 		Hash function
//
//---------------------------------------------------------------------------
ULONG
CEnfdPartitionPropagation::HashValue() const
{
	return m_ppps->HashValue();
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
CEnfdPartitionPropagation::Epet(CExpressionHandle &exprhdl,
								CPhysical *popPhysical,
								BOOL fPropagationReqd) const
{
	if (fPropagationReqd)
	{
		return popPhysical->EpetPartitionPropagation(exprhdl, this);
	}

	return EpetUnnecessary;
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
CEnfdPartitionPropagation::OsPrint(IOstream &os) const
{
	return os << (*m_ppps) << " match: " << SzPropagationMatching(m_eppm)
			  << " ";
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
CEnfdPartitionPropagation::SzPropagationMatching(
	EPartitionPropagationMatching eppm)
{
	GPOS_ASSERT(EppmSentinel > eppm);
	const CHAR *rgszPropagationMatching[EppmSentinel] = {"satisfy"};

	return rgszPropagationMatching[eppm];
}

BOOL
CEnfdPartitionPropagation::Matches(CEnfdPartitionPropagation *pepp)
{
	GPOS_ASSERT(nullptr != pepp);

	return m_eppm == pepp->Eppm() && m_ppps->Equals(pepp->PppsRequired());
}

BOOL
CEnfdPartitionPropagation::FCompatible(
	CPartitionPropagationSpec *pps_drvd) const
{
	GPOS_ASSERT(nullptr != pps_drvd);

	switch (m_eppm)
	{
		case EppmSatisfy:
			return pps_drvd->FSatisfies(m_ppps);

		case EppmSentinel:
			GPOS_ASSERT("invalid matching type");
	}

	return false;
}

// EOF
