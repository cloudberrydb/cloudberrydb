//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CConstraintDisjunction.cpp
//
//	@doc:
//		Implementation of disjunction constraints
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CConstraintInterval.h"
#include "gpopt/base/CConstraintDisjunction.h"
#include "gpopt/operators/CPredicateUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CConstraintDisjunction::CConstraintDisjunction
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CConstraintDisjunction::CConstraintDisjunction
	(
	IMemoryPool *pmp,
	DrgPcnstr *pdrgpcnstr
	)
	:
	CConstraint(pmp),
	m_pdrgpcnstr(NULL)
{
	GPOS_ASSERT(NULL != pdrgpcnstr);
	m_pdrgpcnstr = PdrgpcnstrFlatten(pmp, pdrgpcnstr, EctDisjunction);

	const ULONG ulLen = m_pdrgpcnstr->UlLength();
	GPOS_ASSERT(0 < ulLen);

	m_pcrsUsed = GPOS_NEW(pmp) CColRefSet(pmp);

	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CConstraint *pcnstr = (*m_pdrgpcnstr)[ul];
		m_pcrsUsed->Include(pcnstr->PcrsUsed());
	}

	m_phmcolconstr = Phmcolconstr(pmp, m_pcrsUsed, m_pdrgpcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintDisjunction::~CConstraintDisjunction
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CConstraintDisjunction::~CConstraintDisjunction()
{
	m_pdrgpcnstr->Release();
	m_pcrsUsed->Release();
	m_phmcolconstr->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintDisjunction::FContradiction
//
//	@doc:
//		Check if this constraint is a contradiction
//
//---------------------------------------------------------------------------
BOOL
CConstraintDisjunction::FContradiction() const
{
	const ULONG ulLen = m_pdrgpcnstr->UlLength();

	BOOL fContradiction = true;
	for (ULONG ul = 0; fContradiction && ul < ulLen; ul++)
	{
		fContradiction = (*m_pdrgpcnstr)[ul]->FContradiction();
	}

	return fContradiction;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintDisjunction::FConstraint
//
//	@doc:
//		Check if there is a constraint on the given column
//
//---------------------------------------------------------------------------
BOOL
CConstraintDisjunction::FConstraint
	(
	const CColRef *pcr
	)
	const
{
	DrgPcnstr *pdrgpcnstrCol = m_phmcolconstr->PtLookup(pcr);
	return (NULL != pdrgpcnstrCol && m_pdrgpcnstr->UlLength() == pdrgpcnstrCol->UlLength());
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintDisjunction::PcnstrCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the constraint with remapped columns
//
//---------------------------------------------------------------------------
CConstraint *
CConstraintDisjunction::PcnstrCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcnstr *pdrgpcnstr = GPOS_NEW(pmp) DrgPcnstr(pmp);
	const ULONG ulLen = m_pdrgpcnstr->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CConstraint *pcnstr = (*m_pdrgpcnstr)[ul];
		CConstraint *pcnstrCopy = pcnstr->PcnstrCopyWithRemappedColumns(pmp, phmulcr, fMustExist);
		pdrgpcnstr->Append(pcnstrCopy);
	}
	return GPOS_NEW(pmp) CConstraintDisjunction(pmp, pdrgpcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintDisjunction::Pcnstr
//
//	@doc:
//		Return constraint on a given column
//
//---------------------------------------------------------------------------
CConstraint *
CConstraintDisjunction::Pcnstr
	(
	IMemoryPool *pmp,
	const CColRef *pcr
	)
{
	// all children referencing given column
	DrgPcnstr *pdrgpcnstrCol = m_phmcolconstr->PtLookup(pcr);
	if (NULL == pdrgpcnstrCol)
	{
		return NULL;
	}

	// if not all children have this col, return unbounded constraint
	const ULONG ulLen = pdrgpcnstrCol->UlLength();
	if (ulLen != m_pdrgpcnstr->UlLength())
	{
		return CConstraintInterval::PciUnbounded(pmp, pcr, true /*fIncludesNull*/);
	}

	DrgPcnstr *pdrgpcnstr = GPOS_NEW(pmp) DrgPcnstr(pmp);

	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		// the part of the child that references this column
		CConstraint *pcnstrCol = (*pdrgpcnstrCol)[ul]->Pcnstr(pmp, pcr);
		if (NULL == pcnstrCol)
		{
			pcnstrCol = CConstraintInterval::PciUnbounded(pmp, pcr, true /*fIsNull*/);
		}
		if (pcnstrCol->FUnbounded())
		{
			pdrgpcnstr->Release();
			return pcnstrCol;
		}
		pdrgpcnstr->Append(pcnstrCol);
	}

	return CConstraint::PcnstrDisjunction(pmp, pdrgpcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintDisjunction::Pcnstr
//
//	@doc:
//		Return constraint on a given column set
//
//---------------------------------------------------------------------------
CConstraint *
CConstraintDisjunction::Pcnstr
	(
	IMemoryPool *pmp,
	CColRefSet *pcrs
	)
{
	const ULONG ulLen = m_pdrgpcnstr->UlLength();

	DrgPcnstr *pdrgpcnstr = GPOS_NEW(pmp) DrgPcnstr(pmp);

	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CConstraint *pcnstr = (*m_pdrgpcnstr)[ul];
		if (pcnstr->PcrsUsed()->FDisjoint(pcrs))
		{
			// a child has none of these columns... return unbounded constraint
			pdrgpcnstr->Release();
			return CConstraintInterval::PciUnbounded(pmp, pcrs, true /*fIncludesNull*/);
		}

		// the part of the child that references these columns
		CConstraint *pcnstrCol = pcnstr->Pcnstr(pmp, pcrs);

		if (NULL == pcnstrCol)
		{
			pcnstrCol = CConstraintInterval::PciUnbounded(pmp, pcrs, true /*fIncludesNull*/);
		}
		GPOS_ASSERT(NULL != pcnstrCol);
		pdrgpcnstr->Append(pcnstrCol);
	}

	return CConstraint::PcnstrDisjunction(pmp, pdrgpcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintDisjunction::PcnstrRemapForColumn
//
//	@doc:
//		Return a copy of the constraint for a different column
//
//---------------------------------------------------------------------------
CConstraint *
CConstraintDisjunction::PcnstrRemapForColumn
	(
	IMemoryPool *pmp,
	CColRef *pcr
	)
	const
{
	return PcnstrConjDisjRemapForColumn(pmp, pcr, m_pdrgpcnstr, false /*fConj*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintDisjunction::PexprScalar
//
//	@doc:
//		Scalar expression
//
//---------------------------------------------------------------------------
CExpression *
CConstraintDisjunction::PexprScalar
	(
	IMemoryPool *pmp
	)
{
	if (NULL == m_pexprScalar)
	{
		if (FContradiction())
		{
			m_pexprScalar = CUtils::PexprScalarConstBool(pmp, false /*fval*/, false /*fNull*/);
		}
		else
		{
			m_pexprScalar = PexprScalarConjDisj(pmp, m_pdrgpcnstr, false /*fConj*/);
		}
	}

	return m_pexprScalar;
}

// EOF
