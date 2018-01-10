//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Greenplum, Inc.
//
//	Implementation of index inner nested-loops join operator
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecNonSingleton.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CPhysicalLeftOuterIndexNLJoin.h"

using namespace gpopt;

CPhysicalLeftOuterIndexNLJoin::CPhysicalLeftOuterIndexNLJoin
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcr
	)
	:
	CPhysicalLeftOuterNLJoin(pmp),
	m_pdrgpcrOuterRefs(pdrgpcr)
{
	GPOS_ASSERT(NULL != pdrgpcr);
}


CPhysicalLeftOuterIndexNLJoin::~CPhysicalLeftOuterIndexNLJoin()
{
	m_pdrgpcrOuterRefs->Release();
}


BOOL
CPhysicalLeftOuterIndexNLJoin::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		return m_pdrgpcrOuterRefs->FEqual(CPhysicalLeftOuterIndexNLJoin::PopConvert(pop)->PdrgPcrOuterRefs());
	}

	return false;
}


CDistributionSpec *
CPhysicalLeftOuterIndexNLJoin::PdsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &,//exprhdl,
	CDistributionSpec *,//pdsRequired,
	ULONG ulChildIndex,
	DrgPdp *pdrgpdpCtxt,
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(2 > ulChildIndex);

	if (1 == ulChildIndex)
	{
		// inner (index-scan side) is requested for Any distribution,
		// we allow outer references on the inner child of the join since it needs
		// to refer to columns in join's outer child
		return GPOS_NEW(pmp) CDistributionSpecAny(this->Eopid(), true /*fAllowOuterRefs*/);
	}

	// we need to match distribution of inner
	CDistributionSpec *pdsInner = CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();
	CDistributionSpec::EDistributionType edtInner = pdsInner->Edt();
	if (CDistributionSpec::EdtSingleton == edtInner ||
		CDistributionSpec::EdtStrictSingleton == edtInner ||
		CDistributionSpec::EdtUniversal == edtInner)
	{
		// enforce executing on the master
		return GPOS_NEW(pmp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
	}

	if (CDistributionSpec::EdtHashed == edtInner)
	{
		// check if we could create an equivalent hashed distribution request to the inner child
		CDistributionSpecHashed *pdshashed = CDistributionSpecHashed::PdsConvert(pdsInner);
		CDistributionSpecHashed *pdshashedEquiv = pdshashed->PdshashedEquiv();
		if (NULL != pdshashedEquiv)
		{
			// request hashed distribution from outer
			pdshashedEquiv->Pdrgpexpr()->AddRef();
			return GPOS_NEW(pmp) CDistributionSpecHashed(pdshashedEquiv->Pdrgpexpr(), pdshashedEquiv->FNullsColocated());
		}
	}

	// otherwise, require outer child to be replicated
	return GPOS_NEW(pmp) CDistributionSpecReplicated();
}


// EOF

