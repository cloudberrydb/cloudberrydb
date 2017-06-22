//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 Greenplum, Inc.
//
//	@filename:
//		CFunctionalDependency.cpp
//
//	@doc:
//		Implementation of functional dependency
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CFunctionalDependency.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::CFunctionalDependency
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CFunctionalDependency::CFunctionalDependency
	(
	CColRefSet *pcrsKey,
	CColRefSet *pcrsDetermined
	)
	:
	m_pcrsKey(pcrsKey),
	m_pcrsDetermined(pcrsDetermined)
{
	GPOS_ASSERT(0 < pcrsKey->CElements());
	GPOS_ASSERT(0 < m_pcrsDetermined->CElements());
}


//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::~CFunctionalDependency
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CFunctionalDependency::~CFunctionalDependency()
{
	m_pcrsKey->Release();
	m_pcrsDetermined->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::FIncluded
//
//	@doc:
//		Determine if all FD columns are included in the given column set
//
//---------------------------------------------------------------------------
BOOL
CFunctionalDependency::FIncluded
	(
	CColRefSet *pcrs
	)
	const
{
	return pcrs->FSubset(m_pcrsKey) && pcrs->FSubset(m_pcrsDetermined);
}


//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CFunctionalDependency::UlHash() const
{
	// FIXME(chasseur): in well-formed C++ code, the implicit 'this' pointer in
	// a member method can never be NULL; however, some callers may invoke this
	// method from a (possibly-NULL) pointer with the '->' operator; callers
	// should be modified to explicitly do NULL-checks on pointers so that this
	// method does not rely on undefined behavior
	if (NULL == this)
	{
		return 0;
	}

	return gpos::UlCombineHashes(m_pcrsKey->UlHash(), m_pcrsDetermined->UlHash());
}


//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::FEqual
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CFunctionalDependency::FEqual
	(
	const CFunctionalDependency *pfd
	)
	const
{
	// FIXME(chasseur): in well-formed C++ code, the implicit 'this' pointer in
	// a member method can never be NULL; however, some callers may invoke this
	// method from a (possibly-NULL) pointer with the '->' operator; callers
	// should be modified to explicitly do NULL-checks on pointers so that this
	// method does not rely on undefined behavior
	if (NULL == this || NULL == pfd)
	{
		return (NULL == this && NULL == pfd);
	}

	return m_pcrsKey->FEqual(pfd->PcrsKey()) && m_pcrsDetermined->FEqual(pfd->PcrsDetermined());
}


//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CFunctionalDependency::OsPrint
	(
	IOstream &os
	)
	const
{
	os << "(" << *m_pcrsKey << ")";
	os << " --> (" << *m_pcrsDetermined << ")";
	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CFunctionalDependency::UlHash
	(
	const DrgPfd *pdrgpfd
	)
{
	ULONG ulHash = 0;
	if (NULL != pdrgpfd)
	{
		const ULONG ulSize = pdrgpfd->UlLength();
		for (ULONG ul = 0; ul < ulSize; ul++)
		{
			ulHash = gpos::UlCombineHashes(ulHash, (*pdrgpfd)[ul]->UlHash());
		}
	}

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::FEqual
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CFunctionalDependency::FEqual
	(
	const DrgPfd *pdrgpfdFst,
	const DrgPfd *pdrgpfdSnd
	)
{
	if (NULL == pdrgpfdFst && NULL == pdrgpfdSnd)
		return true;	/* both empty */

	if (NULL == pdrgpfdFst || NULL == pdrgpfdSnd)
		return false;	/* one is empty, the other is not */

	const ULONG ulLenFst = pdrgpfdFst->UlLength();
	const ULONG ulLenSnd = pdrgpfdSnd->UlLength();

	if (ulLenFst != ulLenSnd)
	  return false;

	BOOL fEqual = true;
	for (ULONG ulFst = 0; fEqual && ulFst < ulLenFst; ulFst++)
	{
		const CFunctionalDependency *pfdFst = (*pdrgpfdFst)[ulFst];
		BOOL fMatch = false;
		for (ULONG ulSnd = 0; !fMatch && ulSnd < ulLenSnd; ulSnd++)
		{
			const CFunctionalDependency *pfdSnd = (*pdrgpfdSnd)[ulSnd];
			fMatch = pfdFst->FEqual(pfdSnd);
		}
		fEqual = fMatch;
	}

	return fEqual;
}


//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::PcrsKeys
//
//	@doc:
//		Create a set of all keys
//
//---------------------------------------------------------------------------
CColRefSet *
CFunctionalDependency::PcrsKeys
	(
	IMemoryPool *pmp,
	const DrgPfd *pdrgpfd
	)
{
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);

	if (pdrgpfd != NULL)
	{
		const ULONG ulSize = pdrgpfd->UlLength();
		for (ULONG ul = 0; ul < ulSize; ul++)
		{
			pcrs->Include((*pdrgpfd)[ul]->PcrsKey());
		}
	}

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependency::PdrgpcrKeys
//
//	@doc:
//		Create an array of all keys
//
//---------------------------------------------------------------------------
DrgPcr *
CFunctionalDependency::PdrgpcrKeys
	(
	IMemoryPool *pmp,
	const DrgPfd *pdrgpfd
	)
{
	CColRefSet *pcrs = PcrsKeys(pmp, pdrgpfd);
	DrgPcr *pdrgpcr = pcrs->Pdrgpcr(pmp);
	pcrs->Release();

	return pdrgpcr;
}


// EOF
