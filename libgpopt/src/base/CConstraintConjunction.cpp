//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CConstraintConjunction.cpp
//
//	@doc:
//		Implementation of conjunction constraints
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
#include "gpopt/base/CConstraintConjunction.h"
#include "gpopt/base/CConstraintInterval.h"
#include "gpopt/operators/CPredicateUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CConstraintConjunction::CConstraintConjunction
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CConstraintConjunction::CConstraintConjunction
	(
	IMemoryPool *pmp,
	DrgPcnstr *pdrgpcnstr
	)
	:
	CConstraint(pmp),
	m_pdrgpcnstr(NULL)
{
	GPOS_ASSERT(NULL != pdrgpcnstr);
	m_pdrgpcnstr = PdrgpcnstrFlatten(pmp, pdrgpcnstr, EctConjunction);

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
//		CConstraintConjunction::~CConstraintConjunction
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CConstraintConjunction::~CConstraintConjunction()
{
	m_pdrgpcnstr->Release();
	m_pcrsUsed->Release();
	m_phmcolconstr->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintConjunction::FContradiction
//
//	@doc:
//		Check if this constraint is a contradiction
//
//---------------------------------------------------------------------------
BOOL
CConstraintConjunction::FContradiction() const
{
	const ULONG ulLen = m_pdrgpcnstr->UlLength();

	BOOL fContradiction = false;
	for (ULONG ul = 0; !fContradiction && ul < ulLen; ul++)
	{
		fContradiction = (*m_pdrgpcnstr)[ul]->FContradiction();
	}

	return fContradiction;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintConjunction::FConstraint
//
//	@doc:
//		Check if there is a constraint on the given column
//
//---------------------------------------------------------------------------
BOOL
CConstraintConjunction::FConstraint
	(
	const CColRef *pcr
	)
	const
{
	DrgPcnstr *pdrgpcnstrCol = m_phmcolconstr->PtLookup(pcr);
	return (NULL != pdrgpcnstrCol && 0 < pdrgpcnstrCol->UlLength());
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintConjunction::PcnstrCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the constraint with remapped columns. If fMustExist is
//		set to true, then every column reference in this constraint must be in
//		the hashmap in order to be replace. Otherwise, some columns may not be
//		in the mapping, and hence will not be replaced
//
//---------------------------------------------------------------------------
CConstraint *
CConstraintConjunction::PcnstrCopyWithRemappedColumns
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
	return GPOS_NEW(pmp) CConstraintConjunction(pmp, pdrgpcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintConjunction::Pcnstr
//
//	@doc:
//		Return constraint on a given column
//
//---------------------------------------------------------------------------
CConstraint *
CConstraintConjunction::Pcnstr
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

	DrgPcnstr *pdrgpcnstr = GPOS_NEW(pmp) DrgPcnstr(pmp);

	const ULONG ulLen = pdrgpcnstrCol->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		// the part of the child that references this column
		CConstraint *pcnstrCol = (*pdrgpcnstrCol)[ul]->Pcnstr(pmp, pcr);
		if (NULL == pcnstrCol || pcnstrCol->FUnbounded())
		{
			CRefCount::SafeRelease(pcnstrCol);
			continue;
		}
		pdrgpcnstr->Append(pcnstrCol);
	}

	return CConstraint::PcnstrConjunction(pmp, pdrgpcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintConjunction::Pcnstr
//
//	@doc:
//		Return constraint on a given column set
//
//---------------------------------------------------------------------------
CConstraint *
CConstraintConjunction::Pcnstr
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
			continue;
		}

		// the part of the child that references these columns
		CConstraint *pcnstrCol = pcnstr->Pcnstr(pmp, pcrs);
		if (NULL != pcnstrCol)
		{
			pdrgpcnstr->Append(pcnstrCol);
		}
	}

	return CConstraint::PcnstrConjunction(pmp, pdrgpcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintConjunction::PcnstrRemapForColumn
//
//	@doc:
//		Return a copy of the constraint for a different column
//
//---------------------------------------------------------------------------
CConstraint *
CConstraintConjunction::PcnstrRemapForColumn
	(
	IMemoryPool *pmp,
	CColRef *pcr
	)
	const
{
	return PcnstrConjDisjRemapForColumn(pmp, pcr, m_pdrgpcnstr, true /*fConj*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintConjunction::PexprScalar
//
//	@doc:
//		Scalar expression
//
//---------------------------------------------------------------------------
CExpression *
CConstraintConjunction::PexprScalar
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
			m_pexprScalar = PexprScalarConjDisj(pmp, m_pdrgpcnstr, true /*fConj*/);
		}
	}

	return m_pexprScalar;
}

// EOF
