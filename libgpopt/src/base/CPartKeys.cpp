//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CPartKeys.cpp
//
//	@doc:
//		Implementation of partitioning keys
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CPartKeys.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPartKeys::CPartKeys
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartKeys::CPartKeys
	(
	DrgDrgPcr *pdrgpdrgpcr
	)
	:
	m_pdrgpdrgpcr(pdrgpdrgpcr)
{
	GPOS_ASSERT(NULL != pdrgpdrgpcr);
	m_ulLevels = pdrgpdrgpcr->UlLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartKeys::~CPartKeys
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPartKeys::~CPartKeys()
{
	m_pdrgpdrgpcr->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartKeys::PcrKey
//
//	@doc:
//		Return key at a given level
//
//---------------------------------------------------------------------------
CColRef *
CPartKeys::PcrKey
	(
	ULONG ulLevel
	)
	const
{
	GPOS_ASSERT(ulLevel < m_ulLevels);
	DrgPcr *pdrgpcr = (*m_pdrgpdrgpcr)[ulLevel];
	return (*pdrgpcr)[0];
}

//---------------------------------------------------------------------------
//	@function:
//		CPartKeys::FOverlap
//
//	@doc:
//		Check whether the key columns overlap the given column
//
//---------------------------------------------------------------------------
BOOL
CPartKeys::FOverlap
	(
	CColRefSet *pcrs
	)
	const
{
	for (ULONG ul = 0; ul < m_ulLevels; ul++)
	{
		CColRef *pcr = PcrKey(ul);
		if (pcrs->FMember(pcr))
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartKeys::PpartkeysCopy
//
//	@doc:
//		Copy part key into the given memory pool
//
//---------------------------------------------------------------------------
CPartKeys *
CPartKeys::PpartkeysCopy
	(
	IMemoryPool *pmp
	)
{
	DrgDrgPcr *pdrgpdrgpcrCopy = GPOS_NEW(pmp) DrgDrgPcr(pmp);

	const ULONG ulLength = m_pdrgpdrgpcr->UlLength();
	for (ULONG ul = 0; ul < ulLength; ul++)
	{
		DrgPcr *pdrgpcr = (*m_pdrgpdrgpcr)[ul];
		DrgPcr *pdrgpcrCopy = GPOS_NEW(pmp) DrgPcr(pmp);
		const ULONG ulCols = pdrgpcr->UlLength();
		for (ULONG ulCol = 0; ulCol < ulCols; ulCol++)
		{
			pdrgpcrCopy->Append((*pdrgpcr)[ulCol]);
		}
		pdrgpdrgpcrCopy->Append(pdrgpcrCopy);
	}

	return GPOS_NEW(pmp) CPartKeys(pdrgpdrgpcrCopy);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartKeys::PdrgppartkeysCopy
//
//	@doc:
//		Copy array of part keys into given memory pool
//
//---------------------------------------------------------------------------
DrgPpartkeys *
CPartKeys::PdrgppartkeysCopy
	(
	IMemoryPool *pmp,
	const DrgPpartkeys *pdrgppartkeys
	)
{
	GPOS_ASSERT(NULL != pdrgppartkeys);

	DrgPpartkeys *pdrgppartkeysCopy = GPOS_NEW(pmp) DrgPpartkeys(pmp);
	const ULONG ulLength = pdrgppartkeys->UlLength();
	for (ULONG ul = 0; ul < ulLength; ul++)
	{
		pdrgppartkeysCopy->Append((*pdrgppartkeys)[ul]->PpartkeysCopy(pmp));
	}
	return pdrgppartkeysCopy;
}


//---------------------------------------------------------------------------
//	@function:
//		CPartKeys::PpartkeysRemap
//
//	@doc:
//		Create a new PartKeys object from the current one by remapping the
//		keys using the given hashmap
//
//---------------------------------------------------------------------------
CPartKeys *
CPartKeys::PpartkeysRemap
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr
	)
	const
{
	GPOS_ASSERT(NULL != phmulcr);
	DrgDrgPcr *pdrgpdrgpcr = GPOS_NEW(pmp) DrgDrgPcr(pmp);

	for (ULONG ul = 0; ul < m_ulLevels; ul++)
	{
		CColRef *pcr = CUtils::PcrRemap(PcrKey(ul), phmulcr, false /*fMustExist*/);

		DrgPcr *pdrgpcr = GPOS_NEW(pmp) DrgPcr(pmp);
		pdrgpcr->Append(pcr);

		pdrgpdrgpcr->Append(pdrgpcr);
	}

	return GPOS_NEW(pmp) CPartKeys(pdrgpdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartKeys::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPartKeys::OsPrint
	(
	IOstream &os
	)
	const
{
	os << "(";
	for (ULONG ul = 0; ul < m_ulLevels; ul++)
	{
		CColRef *pcr = PcrKey(ul);
		os << *pcr;

		// separator
		os << (ul == m_ulLevels - 1 ? "" : ", ");
	}

	os << ")";

	return os;
}

#ifdef GPOS_DEBUG
void
CPartKeys::DbgPrint() const
{

	IMemoryPool *pmp = COptCtxt::PoctxtFromTLS()->Pmp();
	CAutoTrace at(pmp);
	(void) this->OsPrint(at.Os());
}
#endif // GPOS_DEBUG
// EOF
