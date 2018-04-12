//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CScalarProjectElement.cpp
//
//	@doc:
//		Implementation of scalar project operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/operators/CScalarProjectElement.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CScalarProjectElement::UlHash
//
//	@doc:
//		Hash value built from colref and Eop
//
//---------------------------------------------------------------------------
ULONG
CScalarProjectElement::UlHash() const
{
	return gpos::UlCombineHashes(COperator::UlHash(),
							   gpos::UlHashPtr<CColRef>(m_pcr));
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarProjectElement::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarProjectElement::FMatch
	(
	COperator *pop
	)
const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarProjectElement *popScPrEl = CScalarProjectElement::PopConvert(pop);

		// match if column reference is same
		return Pcr() == popScPrEl->Pcr();
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarProjectElement::FInputOrderSensitive
//
//	@doc:
//		Not called for leaf operators
//
//---------------------------------------------------------------------------
BOOL
CScalarProjectElement::FInputOrderSensitive() const
{
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarProjectElement::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CScalarProjectElement::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	ULONG ulId = m_pcr->UlId();
	CColRef *pcr = phmulcr->PtLookup(&ulId);
	if (NULL == pcr)
	{
		if (fMustExist)
		{
			// not found in hashmap, so create a new colref and add to hashmap
			CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();

			CName name(m_pcr->Name());
			pcr = pcf->PcrCreate(m_pcr->Pmdtype(), m_pcr->ITypeModifier(), m_pcr->OidCollation(), name);

#ifdef GPOS_DEBUG
			BOOL fResult =
#endif // GPOS_DEBUG
			phmulcr->FInsert(GPOS_NEW(pmp) ULONG(ulId), pcr);
			GPOS_ASSERT(fResult);
		}
		else
		{
			pcr = m_pcr;
		}
	}

	return GPOS_NEW(pmp) CScalarProjectElement(pmp, pcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarProjectElement::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CScalarProjectElement::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " ";
	m_pcr->OsPrint(os);

	return os;
}

// EOF
