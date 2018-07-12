//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CColRef.cpp
//
//	@doc:
//		Implementation of column reference class
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CColRef.h"

#ifdef GPOS_DEBUG
#include "gpopt/base/COptCtxt.h"
#include "gpos/error/CAutoTrace.h"
#endif // GPOS_DEBUG

using namespace gpopt;

// invalid key
const ULONG CColRef::m_ulInvalid = gpos::ulong_max;

//---------------------------------------------------------------------------
//	@function:
//		CColRef::CColRef
//
//	@doc:
//		ctor
//		takes ownership of string; verify string is properly formatted
//
//---------------------------------------------------------------------------
CColRef::CColRef
	(
	const IMDType *pmdtype,
	const INT iTypeModifier,
	ULONG ulId,
	const CName *pname
	)
	:
	m_pmdtype(pmdtype),
	m_iTypeModifier(iTypeModifier),
	m_pname(pname),
	m_ulId(ulId)
{
	GPOS_ASSERT(NULL != pmdtype);
	GPOS_ASSERT(pmdtype->Pmdid()->FValid());
	GPOS_ASSERT(NULL != pname);
}


//---------------------------------------------------------------------------
//	@function:
//		CColRef::~CColRef
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CColRef::~CColRef()
{
	// we own the name 
	GPOS_DELETE(m_pname);
}


//---------------------------------------------------------------------------
//	@function:
//		CColRef::UlHash
//
//	@doc:
//		static hash function
//
//---------------------------------------------------------------------------
ULONG
CColRef::UlHash
	(
	const ULONG &ulptr
	)
{
	return gpos::UlHash<ULONG>(&ulptr);
}

//---------------------------------------------------------------------------
//	@function:
//		CColRef::UlHash
//
//	@doc:
//		static hash function
//
//---------------------------------------------------------------------------
ULONG
CColRef::UlHash
	(
	const CColRef *pcr
	)
{
	ULONG ulId = pcr->UlId();
	return gpos::UlHash<ULONG>(&ulId);
}


//---------------------------------------------------------------------------
//	@function:
//		CColRef::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CColRef::OsPrint
	(
	IOstream &os
	)
	const
{
	m_pname->OsPrint(os);
	os << " (" << UlId() << ")";
	
	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CColRef::Pdrgpul
//
//	@doc:
//		Extract array of colids from array of colrefs
//
//---------------------------------------------------------------------------
DrgPul *
CColRef::Pdrgpul
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcr
	)
{
	DrgPul *pdrgpul = GPOS_NEW(pmp) DrgPul(pmp);
	const ULONG ulLen = pdrgpcr->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CColRef *pcr = (*pdrgpcr)[ul];
		pdrgpul->Append(GPOS_NEW(pmp) ULONG(pcr->UlId()));
	}

	return pdrgpul;
}

//---------------------------------------------------------------------------
//	@function:
//		CColRef::FEqual
//
//	@doc:
//		Are the two arrays of column references equivalent
//
//---------------------------------------------------------------------------
BOOL
CColRef::FEqual
	(
	const DrgPcr *pdrgpcr1,
	const DrgPcr *pdrgpcr2
	)
{
	if (NULL == pdrgpcr1 || NULL == pdrgpcr2)
	{
		return  (NULL == pdrgpcr1 && NULL == pdrgpcr2);
	}

	return pdrgpcr1->FEqual(pdrgpcr2);
}

// check if the the array of column references are equal. Note that since we have unique
// copy of the column references, we can compare pointers.
BOOL
CColRef::FEqual
	(
	const DrgDrgPcr *pdrgdrgpcr1,
	const DrgDrgPcr *pdrgdrgpcr2
	)
{
	ULONG ulLen1 = (pdrgdrgpcr1 == NULL) ? 0 : pdrgdrgpcr1->UlLength();
	ULONG ulLen2 = (pdrgdrgpcr2 == NULL) ? 0 : pdrgdrgpcr2->UlLength();

	if (ulLen1 != ulLen2)
	{
		return false;
	}

	for (ULONG ulLevel = 0; ulLevel < ulLen1; ulLevel++)
	{
		BOOL fEqual = (*pdrgdrgpcr1)[ulLevel]->FEqual((*pdrgdrgpcr2)[ulLevel]);
		if (!fEqual)
		{
			return false;
		}
	}

	return true;
}

#ifdef GPOS_DEBUG
void
CColRef::DbgPrint() const
{
	IMemoryPool *pmp = COptCtxt::PoctxtFromTLS()->Pmp();
	CAutoTrace at(pmp);
	(void) this->OsPrint(at.Os());
}
#endif // GPOS_DEBUG

// EOF

