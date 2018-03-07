//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPropConstraint.cpp
//
//	@doc:
//		Implementation of constraint property
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CPropConstraint.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/COptCtxt.h"
#include "gpos/error/CAutoTrace.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::CPropConstraint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPropConstraint::CPropConstraint
	(
	IMemoryPool *pmp,
	DrgPcrs *pdrgpcrs,
	CConstraint *pcnstr
	)
	:
	m_pdrgpcrs(pdrgpcrs),
	m_phmcrcrs(NULL),
	m_pcnstr(pcnstr)
{
	GPOS_ASSERT(NULL != pdrgpcrs);
	InitHashMap(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::~CPropConstraint
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPropConstraint::~CPropConstraint()
{
	m_pdrgpcrs->Release();
	m_phmcrcrs->Release();
	CRefCount::SafeRelease(m_pcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::InitHashMap
//
//	@doc:
//		Initialize mapping between columns and equivalence classes
//
//---------------------------------------------------------------------------
void
CPropConstraint::InitHashMap
	(
	IMemoryPool *pmp
	)
{
	GPOS_ASSERT(NULL == m_phmcrcrs);
	m_phmcrcrs = GPOS_NEW(pmp) HMCrCrs(pmp);
	const ULONG ulEquiv = m_pdrgpcrs->UlLength();
	for (ULONG ul = 0; ul < ulEquiv; ul++)
	{
		CColRefSet *pcrs = (*m_pdrgpcrs)[ul];

		CColRefSetIter crsi(*pcrs);
		while (crsi.FAdvance())
		{
			pcrs->AddRef();
#ifdef GPOS_DEBUG
			BOOL fres =
#endif //GPOS_DEBUG
			m_phmcrcrs->FInsert(crsi.Pcr(), pcrs);
			GPOS_ASSERT(fres);
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::FContradiction
//
//	@doc:
//		Is this a contradiction
//
//---------------------------------------------------------------------------
BOOL
CPropConstraint::FContradiction()
const
{
	return (NULL != m_pcnstr && m_pcnstr->FContradiction());
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::PexprScalarMappedFromEquivCols
//
//	@doc:
//		Return scalar expression on the given column mapped from all constraints
//		on its equivalent columns
//
//---------------------------------------------------------------------------
CExpression *
CPropConstraint::PexprScalarMappedFromEquivCols
	(
	IMemoryPool *pmp,
	CColRef *pcr
	)
	const
{
	if(NULL == m_pcnstr)
	{
		return NULL;
	}

	CColRefSet *pcrs = m_phmcrcrs->PtLookup(pcr);
	if (NULL == pcrs || 1 == pcrs->CElements())
	{
		return NULL;
	}

	// get constraints for all other columns in this equivalence class
	// except the current column
	CColRefSet *pcrsEquiv = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsEquiv->Include(pcrs);
	pcrsEquiv->Exclude(pcr);

	CConstraint *pcnstr = m_pcnstr->Pcnstr(pmp, pcrsEquiv);
	pcrsEquiv->Release();
	if (NULL == pcnstr)
	{
		return NULL;
	}

	// generate a copy of all these constraints for the current column
	CConstraint *pcnstrCol = pcnstr->PcnstrRemapForColumn(pmp, pcr);
	CExpression *pexprScalar = pcnstrCol->PexprScalar(pmp);
	pexprScalar->AddRef();

	pcnstr->Release();
	pcnstrCol->Release();

	return pexprScalar;
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CPropConstraint::OsPrint
	(
	IOstream &os
	)
	const
{
	const ULONG ulLen = m_pdrgpcrs->UlLength();
	if (0 < ulLen)
	{
		os << "Equivalence Classes: { ";

		for (ULONG ul = 0; ul < ulLen; ul++)
		{
			CColRefSet *pcrs = (*m_pdrgpcrs)[ul];
			os << "(" << *pcrs << ") ";
		}

		os << "} ";
	}

	if (NULL != m_pcnstr)
	{
		os << "Constraint:" << *m_pcnstr;
	}

	return os;
}

void
CPropConstraint::DbgPrint() const
{
	IMemoryPool *pmp = COptCtxt::PoctxtFromTLS()->Pmp();
	CAutoTrace at(pmp);
	(void) this->OsPrint(at.Os());
}
// EOF

