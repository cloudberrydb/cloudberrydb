//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalDynamicGet.cpp
//
//	@doc:
//		Implementation of dynamic table access
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CConstraintInterval.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CPartIndexMap.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/COptCtxt.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalDynamicGet.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/metadata/CName.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::CLogicalDynamicGet
//
//	@doc:
//		ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalDynamicGet::CLogicalDynamicGet
	(
	IMemoryPool *pmp
	)
	:
	CLogicalDynamicGetBase(pmp)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::CLogicalDynamicGet
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalDynamicGet::CLogicalDynamicGet
	(
	IMemoryPool *pmp,
	const CName *pnameAlias,
	CTableDescriptor *ptabdesc,
	ULONG ulPartIndex,
	DrgPcr *pdrgpcrOutput,
	DrgDrgPcr *pdrgpdrgpcrPart,
	ULONG ulSecondaryPartIndexId,
	BOOL fPartial,
	CPartConstraint *ppartcnstr, 
	CPartConstraint *ppartcnstrRel
	)
	:
	CLogicalDynamicGetBase(pmp, pnameAlias, ptabdesc, ulPartIndex, pdrgpcrOutput, pdrgpdrgpcrPart, ulSecondaryPartIndexId, fPartial, ppartcnstr, ppartcnstrRel)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::CLogicalDynamicGet
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalDynamicGet::CLogicalDynamicGet
	(
	IMemoryPool *pmp,
	const CName *pnameAlias,
	CTableDescriptor *ptabdesc,
	ULONG ulPartIndex
	)
	:
	CLogicalDynamicGetBase(pmp, pnameAlias, ptabdesc, ulPartIndex)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::~CLogicalDynamicGet
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CLogicalDynamicGet::~CLogicalDynamicGet()
{
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::UlHash
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalDynamicGet::UlHash() const
{
	ULONG ulHash = gpos::UlCombineHashes(COperator::UlHash(), m_ptabdesc->Pmdid()->UlHash());
	ulHash = gpos::UlCombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrOutput));

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalDynamicGet::FMatch
	(
	COperator *pop
	)
	const
{
	return CUtils::FMatchDynamicScan(this, pop);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalDynamicGet::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcrOutput = NULL;
	if (fMustExist)
	{
		pdrgpcrOutput = CUtils::PdrgpcrRemapAndCreate(pmp, m_pdrgpcrOutput, phmulcr);
	}
	else
	{
		pdrgpcrOutput = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrOutput, phmulcr, fMustExist);
	}
	DrgDrgPcr *pdrgpdrgpcrPart = PdrgpdrgpcrCreatePartCols(pmp, pdrgpcrOutput, m_ptabdesc->PdrgpulPart());
	CName *pnameAlias = GPOS_NEW(pmp) CName(pmp, *m_pnameAlias);
	m_ptabdesc->AddRef();

	CPartConstraint *ppartcnstr = m_ppartcnstr->PpartcnstrCopyWithRemappedColumns(pmp, phmulcr, fMustExist);
	CPartConstraint *ppartcnstrRel = m_ppartcnstrRel->PpartcnstrCopyWithRemappedColumns(pmp, phmulcr, fMustExist);

	return GPOS_NEW(pmp) CLogicalDynamicGet(pmp, pnameAlias, m_ptabdesc, m_ulScanId, pdrgpcrOutput, pdrgpdrgpcrPart, m_ulSecondaryScanId, m_fPartial, ppartcnstr, ppartcnstrRel);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::FInputOrderSensitive
//
//	@doc:
//		Not called for leaf operators
//
//---------------------------------------------------------------------------
BOOL
CLogicalDynamicGet::FInputOrderSensitive() const
{
	GPOS_ASSERT(!"Unexpected function call of FInputOrderSensitive");
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalDynamicGet::PxfsCandidates
	(
	IMemoryPool *pmp
	) 
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfDynamicGet2DynamicTableScan);
	return pxfs;
}



//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalDynamicGet::OsPrint
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
		// alias of table as referenced in the query
		m_pnameAlias->OsPrint(os);

		// actual name of table in catalog and columns
		os << " (";
		m_ptabdesc->Name().OsPrint(os);
		os <<"), ";
		m_ppartcnstr->OsPrint(os);
		os << "), Columns: [";
		CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
		os << "] Scan Id: " << m_ulScanId << "." << m_ulSecondaryScanId;
		
		if (!m_ppartcnstr->FUnbounded())
		{
			os << ", ";
			m_ppartcnstr->OsPrint(os);
		}
	}
		
	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::PstatsDerive
//
//	@doc:
//		Load up statistics from metadata
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalDynamicGet::PstatsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	DrgPstat * // not used
	)
	const
{
	CReqdPropRelational *prprel = CReqdPropRelational::Prprel(exprhdl.Prp());
	IStatistics *pstats = PstatsDeriveFilter(pmp, exprhdl, prprel->PexprPartPred());

	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp, m_pdrgpcrOutput);
	CUpperBoundNDVs *pubndv = GPOS_NEW(pmp) CUpperBoundNDVs(pcrs, pstats->DRows());
	CStatistics::PstatsConvert(pstats)->AddCardUpperBound(pubndv);

	return pstats;
}

// EOF

