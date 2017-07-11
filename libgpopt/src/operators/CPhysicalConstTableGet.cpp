//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalConstTableGet.cpp
//
//	@doc:
//		Implementation of physical const table get operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecUniversal.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CCTEMap.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CPhysicalConstTableGet.h"
#include "gpopt/operators/CExpressionHandle.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalConstTableGet::CPhysicalConstTableGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalConstTableGet::CPhysicalConstTableGet
	(
	IMemoryPool *pmp,
	DrgPcoldesc *pdrgpcoldesc,
	DrgPdrgPdatum *pdrgpdrgpdatum,
	DrgPcr *pdrgpcrOutput
	)
	:
	CPhysical(pmp),
	m_pdrgpcoldesc(pdrgpcoldesc),
	m_pdrgpdrgpdatum(pdrgpdrgpdatum),
	m_pdrgpcrOutput(pdrgpcrOutput)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalConstTableGet::~CPhysicalConstTableGet
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalConstTableGet::~CPhysicalConstTableGet()
{
	m_pdrgpcoldesc->Release();
	m_pdrgpdrgpdatum->Release();
	m_pdrgpcrOutput->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalConstTableGet::FMatch
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalConstTableGet::FMatch
	(
	COperator *pop
	)
	const
{
	if (Eopid() == pop->Eopid())
	{
		CPhysicalConstTableGet *popCTG = CPhysicalConstTableGet::PopConvert(pop); 
		return m_pdrgpcoldesc == popCTG->Pdrgpcoldesc() && 
				m_pdrgpdrgpdatum == popCTG->Pdrgpdrgpdatum() && 
				m_pdrgpcrOutput == popCTG->PdrgpcrOutput();
	}
	
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalConstTableGet::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalConstTableGet::PcrsRequired
	(
	IMemoryPool *, // pmp,
	CExpressionHandle &, // exprhdl,
	CColRefSet *, // pcrsRequired,
	ULONG , // ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
{
	GPOS_ASSERT(!"CPhysicalConstTableGet has no children");
	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalConstTableGet::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalConstTableGet::PosRequired
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
	GPOS_ASSERT(!"CPhysicalConstTableGet has no children");
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalConstTableGet::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalConstTableGet::PdsRequired
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
	GPOS_ASSERT(!"CPhysicalConstTableGet has no children");
	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalConstTableGet::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalConstTableGet::PrsRequired
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
	GPOS_ASSERT(!"CPhysicalConstTableGet has no children");
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalConstTableGet::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalConstTableGet::PcteRequired
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
	GPOS_ASSERT(!"CPhysicalConstTableGet has no children");
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalConstTableGet::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalConstTableGet::FProvidesReqdCols
	(
	CExpressionHandle &, // exprhdl,
	CColRefSet *pcrsRequired,
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(NULL != pcrsRequired);
	
	CColRefSet *pcrs = GPOS_NEW(m_pmp) CColRefSet(m_pmp);
	pcrs->Include(m_pdrgpcrOutput);

	BOOL fResult = pcrs->FSubset(pcrsRequired);
	
	pcrs->Release();
	
	return fResult;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalConstTableGet::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalConstTableGet::PosDerive
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
//		CPhysicalConstTableGet::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalConstTableGet::PdsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle & // exprhdl
	)
	const
{
	return GPOS_NEW(pmp) CDistributionSpecUniversal();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalConstTableGet::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalConstTableGet::PrsDerive
	(
	IMemoryPool *pmp, 
	CExpressionHandle & // exprhdl
	)
	const
{
	return GPOS_NEW(pmp) CRewindabilitySpec(CRewindabilitySpec::ErtGeneral /*ert*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalConstTableGet::PcmDerive
//
//	@doc:
//		Derive cte map
//
//---------------------------------------------------------------------------
CCTEMap *
CPhysicalConstTableGet::PcmDerive
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
//		CPhysicalConstTableGet::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalConstTableGet::EpetOrder
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
//		CPhysicalConstTableGet::EpetDistribution
//
//	@doc:
//		Return the enforcing type for distribution property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalConstTableGet::EpetDistribution
	(
	CExpressionHandle &exprhdl,
	const CEnfdDistribution *ped
	)
	const
{
	GPOS_ASSERT(NULL != ped);

	CDistributionSpec *pds = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pds();
	
	if (ped->FCompatible(pds))
	{
		return CEnfdProp::EpetUnnecessary;
	}

	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalConstTableGet::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalConstTableGet::EpetRewindability
	(
	CExpressionHandle &, // exprhdl
	const CEnfdRewindability * // per
	)
	const
{
	// rewindability is already provided
	return CEnfdProp::EpetUnnecessary;
}

// print values in const table
IOstream &
CPhysicalConstTableGet::OsPrint
(
	IOstream &os
	)
const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}
	else
	{
		os << SzId() << " ";
		os << "Columns: [";
		CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
		os << "] ";
		os << "Values: [";
		for (ULONG ulA = 0; ulA < m_pdrgpdrgpdatum->UlLength(); ulA++)
		{
			if (0 < ulA)
			{
				os << "; ";
			}
			os << "(";
			DrgPdatum *pdrgpdatum = (*m_pdrgpdrgpdatum)[ulA];

			const ULONG ulLen = pdrgpdatum->UlLength();
			for (ULONG ulB = 0; ulB < ulLen; ulB++)
			{
				IDatum *pdatum = (*pdrgpdatum)[ulB];
				pdatum->OsPrint(os);

				if (ulB < ulLen-1)
				{
					os << ", ";
				}
			}
			os << ")";
		}
		os << "]";
	}

	return os;
}


// EOF

