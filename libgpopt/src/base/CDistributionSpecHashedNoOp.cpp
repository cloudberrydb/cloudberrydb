//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.

#include "gpopt/base/CDistributionSpecHashedNoOp.h"
#include "gpopt/operators/CPhysicalMotionHashDistribute.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;

CDistributionSpecHashedNoOp::CDistributionSpecHashedNoOp
	(
	DrgPexpr *pdrgpexpr
	)
:
CDistributionSpecHashed(pdrgpexpr, true)
{
}

CDistributionSpec::EDistributionType CDistributionSpecHashedNoOp::Edt() const
{
	return CDistributionSpec::EdtHashedNoOp;
}

BOOL CDistributionSpecHashedNoOp::FMatch(const CDistributionSpec *pds) const
{
	return pds->Edt() == Edt();
}

void
CDistributionSpecHashedNoOp::AppendEnforcers
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CReqdPropPlan *,
	DrgPexpr *pdrgpexpr,
	CExpression *pexpr
	)
{
	CDrvdProp *pdp = exprhdl.Pdp();
	CDistributionSpec *pdsChild = CDrvdPropPlan::Pdpplan(pdp)->Pds();
	CDistributionSpecHashed *pdsChildHashed = dynamic_cast<CDistributionSpecHashed *>(pdsChild);
	if (NULL == pdsChildHashed)
	{
		return;
	}
	
	DrgPexpr *pdrgpexprNoOpRedistributionColumns = pdsChildHashed->Pdrgpexpr();
	pdrgpexprNoOpRedistributionColumns->AddRef();
	CDistributionSpecHashedNoOp* pdsNoOp = GPOS_NEW(pmp) CDistributionSpecHashedNoOp(pdrgpexprNoOpRedistributionColumns);
	pexpr->AddRef();
	CExpression *pexprMotion = GPOS_NEW(pmp) CExpression
			(
					pmp,
					GPOS_NEW(pmp) CPhysicalMotionHashDistribute(pmp, pdsNoOp),
					pexpr
			);
	pdrgpexpr->Append(pexprMotion);
}
