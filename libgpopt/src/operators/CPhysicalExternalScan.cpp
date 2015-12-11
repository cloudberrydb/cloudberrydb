//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CPhysicalExternalScan.cpp
//
//	@doc:
//		Implementation of external scan operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CUtils.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalExternalScan.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/metadata/CName.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalExternalScan::CPhysicalExternalScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalExternalScan::CPhysicalExternalScan
	(
	IMemoryPool *pmp,
	const CName *pnameAlias,
	CTableDescriptor *ptabdesc,
	DrgPcr *pdrgpcrOutput
	)
	:
	CPhysicalTableScan(pmp, pnameAlias, ptabdesc, pdrgpcrOutput)
{}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalExternalScan::FMatch
//
//	@doc:
//		match operator
//
//---------------------------------------------------------------------------
BOOL
CPhysicalExternalScan::FMatch
	(
	COperator *pop
	)
	const
{
	if (Eopid() != pop->Eopid())
	{
		return false;
	}

	CPhysicalExternalScan *popExternalScan = CPhysicalExternalScan::PopConvert(pop);
	return m_ptabdesc == popExternalScan->Ptabdesc() &&
			m_pdrgpcrOutput->FEqual(popExternalScan->PdrgpcrOutput());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalExternalScan::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalExternalScan::EpetRewindability
	(
	CExpressionHandle &exprhdl,
	const CEnfdRewindability *per
	)
	const
{
	CRewindabilitySpec *prs = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs();
	if (per->FCompatible(prs))
	{
		return CEnfdProp::EpetUnnecessary;
	}

	return CEnfdProp::EpetRequired;
}

// EOF

