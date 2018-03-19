//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp
//
//	@filename:
//		CPhysicalTVF.cpp
//
//	@doc:
//		Implementation of table-valued functions
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CDistributionSpecUniversal.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CCTEMap.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalTVF.h"
#include "gpopt/metadata/CName.h"
#include "gpopt/base/CColRefSet.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::CPhysicalTVF
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalTVF::CPhysicalTVF
	(
	IMemoryPool *pmp,
	IMDId *pmdidFunc,
	IMDId *pmdidRetType,
	CWStringConst *pstr,
	DrgPcoldesc *pdrgpcoldesc,
	CColRefSet *pcrsOutput
	)
	:
	CPhysical(pmp),
	m_pmdidFunc(pmdidFunc),
	m_pmdidRetType(pmdidRetType),
	m_pstr(pstr),
	m_pdrgpcoldesc(pdrgpcoldesc),
	m_pcrsOutput(pcrsOutput)
{
	GPOS_ASSERT(m_pmdidFunc->FValid());
	GPOS_ASSERT(m_pmdidRetType->FValid());
	GPOS_ASSERT(NULL != m_pstr);
	GPOS_ASSERT(NULL != m_pdrgpcoldesc);
	GPOS_ASSERT(NULL != m_pcrsOutput);

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	m_pmdfunc = pmda->Pmdfunc(m_pmdidFunc);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::~CPhysicalTVF
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalTVF::~CPhysicalTVF()
{
	m_pmdidFunc->Release();
	m_pmdidRetType->Release();
	m_pdrgpcoldesc->Release();
	m_pcrsOutput->Release();
	GPOS_DELETE(m_pstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CPhysicalTVF::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		CPhysicalTVF *popTVF = CPhysicalTVF::PopConvert(pop);

		return m_pmdidFunc->FEquals(popTVF->PmdidFunc()) &&
				m_pmdidRetType->FEquals(popTVF->PmdidRetType()) &&
				m_pdrgpcoldesc == popTVF->Pdrgpcoldesc() &&
				m_pcrsOutput->FEqual(popTVF->PcrsOutput());
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::FInputOrderSensitive
//
//	@doc:
//		Sensitivity to input order
//
//---------------------------------------------------------------------------
BOOL
CPhysicalTVF::FInputOrderSensitive() const
{
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalTVF::PcrsRequired
	(
	IMemoryPool *, // pmp,
	CExpressionHandle &, // exprhdl,
	CColRefSet *, // pcrsRequired,
	ULONG , // ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
{
	GPOS_ASSERT(!"CPhysicalTVF has no relational children");
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalTVF::PosRequired
	(
	IMemoryPool *, // pmp,
	CExpressionHandle &, // exprhdl,
	COrderSpec *, // posRequired,
	ULONG ,// ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(!"CPhysicalTVF has no relational children");
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalTVF::PdsRequired
	(
	IMemoryPool *, // pmp,
	CExpressionHandle &, // exprhdl,
	CDistributionSpec *, // pdsRequired,
	ULONG , //ulChildIndex
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(!"CPhysicalTVF has no relational children");
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalTVF::PrsRequired
	(
	IMemoryPool *, // pmp,
	CExpressionHandle &, // exprhdl,
	CRewindabilitySpec *, // prsRequired,
	ULONG , // ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(!"CPhysicalTVF has no relational children");
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalTVF::PcteRequired
	(
	IMemoryPool *, //pmp,
	CExpressionHandle &, //exprhdl,
	CCTEReq *, //pcter,
	ULONG , //ulChildIndex,
	DrgPdp *, //pdrgpdpCtxt,
	ULONG //ulOptReq
	)
	const
{
	GPOS_ASSERT(!"CPhysicalTVF has no relational children");
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalTVF::FProvidesReqdCols
	(
	CExpressionHandle &, // exprhdl,
	CColRefSet *pcrsRequired,
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(NULL != pcrsRequired);

	return m_pcrsOutput->FSubset(pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalTVF::PosDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle & // exprhdl
	)
	const
{
	return GPOS_NEW(pmp) COrderSpec(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalTVF::PdsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	if (exprhdl.FMasterOnly())
	{
		return GPOS_NEW(pmp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
	}

	return GPOS_NEW(pmp) CDistributionSpecUniversal();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalTVF::PrsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	if (IMDFunction::EfsVolatile == exprhdl.Pdprel()->Pfp()->Efs() || 0 < exprhdl.UlArity())
	{
		return GPOS_NEW(pmp) CRewindabilitySpec(CRewindabilitySpec::ErtNone /*ert*/);
	}

	return GPOS_NEW(pmp) CRewindabilitySpec(CRewindabilitySpec::ErtGeneral /*ert*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::PcmDerive
//
//	@doc:
//		Common case of combining cte maps of all logical children
//
//---------------------------------------------------------------------------
CCTEMap *
CPhysicalTVF::PcmDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle & //exprhdl
	)
	const
{
	return GPOS_NEW(pmp) CCTEMap(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalTVF::EpetOrder
	(
	CExpressionHandle &, // exprhdl
	const CEnfdOrder *
#ifdef GPOS_DEBUG
	peo
#endif // GPOS_DEBUG
	)
	const
{
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(!peo->PosRequired()->FEmpty());

	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTVF::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalTVF::EpetRewindability
	(
	CExpressionHandle &exprhdl,
	const CEnfdRewindability *per
	)
	const
{
	// get rewindability delivered by the TVF node
	CRewindabilitySpec *prs = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs();
	if (per->FCompatible(prs))
	{
	 	// required distribution is already provided
	 	return CEnfdProp::EpetUnnecessary;
	}

	if (exprhdl.FHasOuterRefs())
	{
		// a TVF should not have a materialize on top of it if it has an outer ref
		return CEnfdProp::EpetProhibited;
	}

	return CEnfdProp::EpetRequired;
}

// EOF
