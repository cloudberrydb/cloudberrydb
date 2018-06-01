//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CConstraintNegation.cpp
//
//	@doc:
//		Implementation of negation constraints
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CConstraintInterval.h"
#include "gpopt/base/CConstraintNegation.h"
#include "gpopt/operators/CPredicateUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CConstraintNegation::CConstraintNegation
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CConstraintNegation::CConstraintNegation
	(
	IMemoryPool *pmp,
	CConstraint *pcnstr
	)
	:
	CConstraint(pmp),
	m_pcnstr(pcnstr)
{
	GPOS_ASSERT(NULL != pcnstr);

	m_pcrsUsed = pcnstr->PcrsUsed();
	m_pcrsUsed->AddRef();
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintNegation::~CConstraintNegation
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CConstraintNegation::~CConstraintNegation()
{
	m_pcnstr->Release();
	m_pcrsUsed->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintNegation::PcnstrCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the constraint with remapped columns
//
//---------------------------------------------------------------------------
CConstraint *
CConstraintNegation::PcnstrCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	CConstraint *pcnstr = m_pcnstr->PcnstrCopyWithRemappedColumns(pmp, phmulcr, fMustExist);
	return GPOS_NEW(pmp) CConstraintNegation(pmp, pcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintNegation::Pcnstr
//
//	@doc:
//		Return constraint on a given column
//
//---------------------------------------------------------------------------
CConstraint *
CConstraintNegation::Pcnstr
	(
	IMemoryPool *pmp,
	const CColRef *pcr
	)
{
	if (!m_pcrsUsed->FMember(pcr) || (1 != m_pcrsUsed->CElements()))
	{
		// return NULL when the constraint:
		// 1) does not contain the column requested
		// 2) constraint may include other columns as well.
		// for instance, conjunction constraint (NOT a=b) is like:
		//       NOT ({"a" (0), ranges: (-inf, inf) } AND {"b" (1), ranges: (-inf, inf) }))
		// recursing down the constraint will give NOT ({"a" (0), ranges: (-inf, inf) })
		// but that is equivalent to (NOT a) which is not the case.

		return NULL;
	}

	return GPOS_NEW(pmp) CConstraintNegation(pmp, m_pcnstr->Pcnstr(pmp, pcr));
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintNegation::Pcnstr
//
//	@doc:
//		Return constraint on a given column set
//
//---------------------------------------------------------------------------
CConstraint *
CConstraintNegation::Pcnstr
	(
	IMemoryPool *pmp,
	CColRefSet *pcrs
	)
{
	if (!m_pcrsUsed->FEqual(pcrs))
	{
		return NULL;
	}

	return GPOS_NEW(pmp) CConstraintNegation(pmp, m_pcnstr->Pcnstr(pmp, pcrs));
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintNegation::PcnstrRemapForColumn
//
//	@doc:
//		Return a copy of the constraint for a different column
//
//---------------------------------------------------------------------------
CConstraint *
CConstraintNegation::PcnstrRemapForColumn
	(
	IMemoryPool *pmp,
	CColRef *pcr
	)
	const
{
	GPOS_ASSERT(1 == m_pcrsUsed->CElements());

	return GPOS_NEW(pmp) CConstraintNegation(pmp, m_pcnstr->PcnstrRemapForColumn(pmp, pcr));
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintNegation::PexprScalar
//
//	@doc:
//		Scalar expression
//
//---------------------------------------------------------------------------
CExpression *
CConstraintNegation::PexprScalar
	(
	IMemoryPool *pmp
	)
{
	if (NULL == m_pexprScalar)
	{
		EConstraintType ect = m_pcnstr->Ect();
		if (EctNegation == ect)
		{
			CConstraintNegation *pcn = (CConstraintNegation *)m_pcnstr;
			m_pexprScalar = pcn->PcnstrChild()->PexprScalar(pmp);
			m_pexprScalar->AddRef();
		}
		else if (EctInterval == ect)
		{
			CConstraintInterval *pci = (CConstraintInterval *)m_pcnstr;
			CConstraintInterval *pciComp = pci->PciComplement(pmp);
			m_pexprScalar = pciComp->PexprScalar(pmp);
			m_pexprScalar->AddRef();
			pciComp->Release();
		}
		else
		{
			CExpression *pexpr = m_pcnstr->PexprScalar(pmp);
			pexpr->AddRef();
			m_pexprScalar = CUtils::PexprNegate(pmp, pexpr);
		}
	}

	return m_pexprScalar;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintNegation::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CConstraintNegation::OsPrint
	(
	IOstream &os
	)
	const
{
	os << "(NOT " << *m_pcnstr << ")";

	return os;
}

// EOF
