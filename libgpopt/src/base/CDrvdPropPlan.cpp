//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CDrvdPropPlan.cpp
//
//	@doc:
//		Derived plan properties
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/operators/CPhysical.h"
#include "gpopt/operators/CScalar.h"
#include "gpopt/base/CPartIndexMap.h"
#include "gpopt/base/CCTEMap.h"
#include "gpopt/base/CDrvdPropPlan.h"
#include "gpopt/base/CDrvdPropCtxtPlan.h"
#include "gpopt/base/CReqdPropPlan.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalCTEConsumer.h"


using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::CDrvdPropPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDrvdPropPlan::CDrvdPropPlan
	()
	:
	m_pos(NULL),
	m_pds(NULL),
	m_prs(NULL),
	m_ppim(NULL),
	m_ppfm(NULL),
	m_pcm(NULL)
{}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::~CDrvdPropPlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDrvdPropPlan::~CDrvdPropPlan()
{
	CRefCount::SafeRelease(m_pos);
	CRefCount::SafeRelease(m_pds);
	CRefCount::SafeRelease(m_prs);
	CRefCount::SafeRelease(m_ppim);
	CRefCount::SafeRelease(m_ppfm);
	CRefCount::SafeRelease(m_pcm);
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::Pdpplan
//
//	@doc:
//		Short hand for conversion
//
//---------------------------------------------------------------------------
CDrvdPropPlan *
CDrvdPropPlan::Pdpplan
	(
	CDrvdProp *pdp
	)
{
	GPOS_ASSERT(NULL != pdp);
	GPOS_ASSERT(EptPlan == pdp->Ept() && "This is not a plan properties container");

	return dynamic_cast<CDrvdPropPlan*>(pdp);
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::Derive
//
//	@doc:
//		Derive plan props
//
//---------------------------------------------------------------------------
void
CDrvdPropPlan::Derive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CDrvdPropCtxt *pdpctxt
	)
{
	CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
	if (NULL != pdpctxt && COperator::EopPhysicalCTEConsumer == popPhysical->Eopid())
	{
		CopyCTEProducerPlanProps(pmp, pdpctxt, popPhysical);
	}
	else
	{
		// call property derivation functions on the operator
		m_pos = popPhysical->PosDerive(pmp, exprhdl);
		m_pds = popPhysical->PdsDerive(pmp, exprhdl);
		m_prs = popPhysical->PrsDerive(pmp, exprhdl);
		m_ppim = popPhysical->PpimDerive(pmp, exprhdl, pdpctxt);
		m_ppfm = popPhysical->PpfmDerive(pmp, exprhdl);

		GPOS_ASSERT(NULL != m_ppim);
		GPOS_ASSERT(CDistributionSpec::EdtAny != m_pds->Edt() && "CDistributionAny is a require-only, cannot be derived");
	}

	m_pcm = popPhysical->PcmDerive(pmp, exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::CopyCTEProducerPlanProps
//
//	@doc:
//		Copy CTE producer plan properties from given context to current object
//
//---------------------------------------------------------------------------
void
CDrvdPropPlan::CopyCTEProducerPlanProps
	(
	IMemoryPool *pmp,
	CDrvdPropCtxt *pdpctxt,
	COperator *pop
	)
{
	CDrvdPropCtxtPlan *pdpctxtplan = CDrvdPropCtxtPlan::PdpctxtplanConvert(pdpctxt);
	CPhysicalCTEConsumer *popCTEConsumer = CPhysicalCTEConsumer::PopConvert(pop);
	ULONG ulCTEId = popCTEConsumer->UlCTEId();
	HMUlCr *phmulcr = popCTEConsumer->Phmulcr();
	CDrvdPropPlan *pdpplan = pdpctxtplan->PdpplanCTEProducer(ulCTEId);
	if (NULL != pdpplan)
	{
		// copy producer plan properties after remapping columns
		m_pos = pdpplan->Pos()->PosCopyWithRemappedColumns(pmp, phmulcr, true /*fMustExist*/);
		m_pds = pdpplan->Pds()->PdsCopyWithRemappedColumns(pmp, phmulcr, true /*fMustExist*/);

		// rewindability and partition filter map do not need column remapping,
		// we add-ref producer's properties directly
		pdpplan->Prs()->AddRef();
		m_prs = pdpplan->Prs();

		pdpplan->Ppfm()->AddRef();
		m_ppfm = pdpplan->Ppfm();

		// no need to copy the part index map. return an empty one. This is to
		// distinguish between a CTE consumer and the inlined expression
		m_ppim = GPOS_NEW(pmp) CPartIndexMap(pmp);

		GPOS_ASSERT(CDistributionSpec::EdtAny != m_pds->Edt() && "CDistributionAny is a require-only, cannot be derived");
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::FSatisfies
//
//	@doc:
//		Check for satisfying required properties
//
//---------------------------------------------------------------------------
BOOL
CDrvdPropPlan::FSatisfies
	(
	const CReqdPropPlan *prpp
	)
	const
{
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT(NULL != prpp->Peo());
	GPOS_ASSERT(NULL != prpp->Ped());
	GPOS_ASSERT(NULL != prpp->Per());
	GPOS_ASSERT(NULL != prpp->Pepp());
	GPOS_ASSERT(NULL != prpp->Pcter());

	return
		m_pos->FSatisfies(prpp->Peo()->PosRequired()) &&
		m_pds->FSatisfies(prpp->Ped()->PdsRequired()) &&
		m_prs->FSatisfies(prpp->Per()->PrsRequired()) && 
		m_ppim->FSatisfies(prpp->Pepp()->PppsRequired()) &&
		m_pcm->FSatisfies(prpp->Pcter());
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CDrvdPropPlan::UlHash() const
{
	ULONG ulHash = gpos::UlCombineHashes(m_pos->UlHash(), m_pds->UlHash());
	ulHash = gpos::UlCombineHashes(ulHash, m_prs->UlHash());
	ulHash = gpos::UlCombineHashes(ulHash, m_ppim->UlHash());
	ulHash = gpos::UlCombineHashes(ulHash, m_pcm->UlHash());

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::FEqual
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
ULONG
CDrvdPropPlan::FEqual
	(
	const CDrvdPropPlan *pdpplan
	)
	const
{
	return m_pos->FMatch(pdpplan->Pos()) &&
			m_pds->FMatch(pdpplan->Pds()) &&
			m_prs->FMatch(pdpplan->Prs()) &&
			m_ppim->FEqual(pdpplan->Ppim()) &&
			m_pcm->FEqual(pdpplan->Pcm());
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CDrvdPropPlan::OsPrint
	(
	IOstream &os
	)
	const
{
		os	<< "Drvd Plan Props ("
			<< "ORD: " << (*m_pos)
			<< ", DIST: " << (*m_pds)
			<< ", REWIND: " << (*m_prs) << ")"
			<< ", Part-Index Map: [" << *m_ppim << "]";
			os << ", Part Filter Map: ";
			m_ppfm->OsPrint(os);
			os << ", CTE Map: [" << *m_pcm << "]";

		return os;
}

// EOF
