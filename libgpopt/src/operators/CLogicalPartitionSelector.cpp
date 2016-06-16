//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CLogicalPartitionSelector.cpp
//
//	@doc:
//		Implementation of Logical partition selector
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalPartitionSelector.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalPartitionSelector::CLogicalPartitionSelector
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalPartitionSelector::CLogicalPartitionSelector
	(
	IMemoryPool *pmp
	)
	:
	CLogical(pmp),
	m_pmdid(NULL),
	m_pdrgpexprFilters(NULL),
	m_pcrOid(NULL)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalPartitionSelector::CLogicalPartitionSelector
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalPartitionSelector::CLogicalPartitionSelector
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	DrgPexpr *pdrgpexprFilters,
	CColRef *pcrOid
	)
	:
	CLogical(pmp),
	m_pmdid(pmdid),
	m_pdrgpexprFilters(pdrgpexprFilters),
	m_pcrOid(pcrOid)
{
	GPOS_ASSERT(pmdid->FValid());
	GPOS_ASSERT(NULL != pdrgpexprFilters);
	GPOS_ASSERT(0 < pdrgpexprFilters->UlLength());
	GPOS_ASSERT(NULL != pcrOid);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalPartitionSelector::~CLogicalPartitionSelector
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalPartitionSelector::~CLogicalPartitionSelector()
{
	CRefCount::SafeRelease(m_pmdid);
	CRefCount::SafeRelease(m_pdrgpexprFilters);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalPartitionSelector::FMatch
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CLogicalPartitionSelector::FMatch
	(
	COperator *pop
	)
	const
{
	if (Eopid() != pop->Eopid())
	{
		return false;
	}

	CLogicalPartitionSelector *popPartSelector = CLogicalPartitionSelector::PopConvert(pop);

	return popPartSelector->PcrOid() == m_pcrOid &&
			popPartSelector->Pmdid()->FEquals(m_pmdid) &&
			popPartSelector->m_pdrgpexprFilters->FEqual(m_pdrgpexprFilters);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalPartitionSelector::UlHash
//
//	@doc:
//		Hash operator
//
//---------------------------------------------------------------------------
ULONG
CLogicalPartitionSelector::UlHash() const
{
	return gpos::UlCombineHashes(Eopid(), m_pmdid->UlHash());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalPartitionSelector::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalPartitionSelector::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	CColRef *pcrOid = CUtils::PcrRemap(m_pcrOid, phmulcr, fMustExist);
	DrgPexpr *pdrgpexpr = CUtils::PdrgpexprRemap(pmp, m_pdrgpexprFilters, phmulcr);

	m_pmdid->AddRef();

	return GPOS_NEW(pmp) CLogicalPartitionSelector(pmp, m_pmdid, pdrgpexpr, pcrOid);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalPartitionSelector::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalPartitionSelector::PcrsDeriveOutput
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
{
	CColRefSet *pcrsOutput = GPOS_NEW(pmp) CColRefSet(pmp);

	pcrsOutput->Union(exprhdl.Pdprel(0)->PcrsOutput());
	pcrsOutput->Include(m_pcrOid);

	return pcrsOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalPartitionSelector::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalPartitionSelector::Maxcard
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	// pass on max card of first child
	return exprhdl.Pdprel(0)->Maxcard();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalPartitionSelector::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalPartitionSelector::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfImplementPartitionSelector);
	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalPartitionSelector::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalPartitionSelector::OsPrint
	(
	IOstream &os
	)
	const
{
	os	<< SzId()
		<< ", Part Table: ";
	m_pmdid->OsPrint(os);

	return os;
}

// EOF
