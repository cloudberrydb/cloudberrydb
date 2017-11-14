//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		COrderSpec.cpp
//
//	@doc:
//		Specification of order property
//---------------------------------------------------------------------------

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/COrderSpec.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CPhysicalSort.h"

#ifdef GPOS_DEBUG
#include "gpos/error/CAutoTrace.h"
#endif // GPOS_DEBUG

using namespace gpopt;
using namespace gpmd;

// string encoding of null treatment
const CHAR rgszNullCode[][16] = {"Auto", "NULLsFirst", "NULLsLast"};
GPOS_CPL_ASSERT(COrderSpec::EntSentinel == GPOS_ARRAY_SIZE(rgszNullCode));


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::COrderExpression::COrderExpression
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
COrderSpec::COrderExpression::COrderExpression
	(
	gpmd::IMDId *pmdid,
	const CColRef *pcr,
	ENullTreatment ent
	)
	:
	m_pmdid(pmdid),
	m_pcr(pcr),
	m_ent(ent)
{
	GPOS_ASSERT(NULL != pcr);
	GPOS_ASSERT(pmdid->FValid());
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::COrderExpression::~COrderExpression
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
COrderSpec::COrderExpression::~COrderExpression()
{
	m_pmdid->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::COrderExpression::FMatch
//
//	@doc:
//		Check if order expression equal to given one;
//
//---------------------------------------------------------------------------
BOOL
COrderSpec::COrderExpression::FMatch
	(
	const COrderExpression *poe
	)
	const
{
	GPOS_ASSERT(NULL != poe);
		
	return
		poe->m_pmdid->FEquals(m_pmdid) && 
		poe->m_pcr == m_pcr &&
		poe->m_ent == m_ent;
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::COrderExpression::OsPrint
//
//	@doc:
//		Print order expression
//
//---------------------------------------------------------------------------
IOstream &
COrderSpec::COrderExpression::OsPrint
	(
	IOstream &os
	)
	const
{
	os << "( ";
	m_pmdid->OsPrint(os);
	os << ", ";
	m_pcr->OsPrint(os);
	os << ", " << rgszNullCode[m_ent] << " )";

	return os;
}

#ifdef GPOS_DEBUG
void
COrderSpec::COrderExpression::DbgPrint() const
{
	IMemoryPool *pmp = COptCtxt::PoctxtFromTLS()->Pmp();
	CAutoTrace at(pmp);
	(void) this->OsPrint(at.Os());
}
#endif // GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::COrderSpec
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
COrderSpec::COrderSpec
	(
	IMemoryPool *pmp
	)
	:
	m_pmp(pmp),
	m_pdrgpoe(NULL)
{
	m_pdrgpoe = GPOS_NEW(pmp) DrgPoe(pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::~COrderSpec
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
COrderSpec::~COrderSpec()
{
	m_pdrgpoe->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::Append
//
//	@doc:
//		Append order expression;
//
//---------------------------------------------------------------------------
void
COrderSpec::Append
	(
	gpmd::IMDId *pmdid,
	const CColRef *pcr,
	ENullTreatment ent
	)
{
	COrderExpression *poe = GPOS_NEW(m_pmp) COrderExpression(pmdid, pcr, ent);
	m_pdrgpoe->Append(poe);
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::FMatch
//
//	@doc:
//		Check for equality between order specs
//
//---------------------------------------------------------------------------
BOOL
COrderSpec::FMatch
	(
	const COrderSpec *pos
	)
	const
{
	BOOL fMatch = 
			m_pdrgpoe->UlLength() == pos->m_pdrgpoe->UlLength() && 
			FSatisfies(pos);
		
	GPOS_ASSERT_IMP(fMatch, pos->FSatisfies(this));
		
	return fMatch;
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::FSatisfies
//
//	@doc:
//		Check if this order spec satisfies the given one
//
//---------------------------------------------------------------------------
BOOL
COrderSpec::FSatisfies
	(
	const COrderSpec *pos
	)
	const
{	
	const ULONG ulArity = pos->m_pdrgpoe->UlLength();
	BOOL fSatisfies = (m_pdrgpoe->UlLength() >= ulArity);
	
	for (ULONG ul = 0; fSatisfies && ul < ulArity; ul++)
	{
		fSatisfies = (*m_pdrgpoe)[ul]->FMatch((*(pos->m_pdrgpoe))[ul]);
	}
	
	return fSatisfies;	
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::AppendEnforcers
//
//	@doc:
//		Add required enforcers enforcers to dynamic array
//
//---------------------------------------------------------------------------
void
COrderSpec::AppendEnforcers
	(
	IMemoryPool *pmp,
	CExpressionHandle &, // exprhdl
	CReqdPropPlan *
#ifdef GPOS_DEBUG
	prpp
#endif // GPOS_DEBUG
	,
	DrgPexpr *pdrgpexpr,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(this == prpp->Peo()->PosRequired() &&
				"required plan properties don't match enforced order spec");

	AddRef();
	pexpr->AddRef();
	CExpression *pexprSort = GPOS_NEW(pmp) CExpression
										(
										pmp, 
										GPOS_NEW(pmp) CPhysicalSort(pmp, this),
										pexpr
										);
	pdrgpexpr->Append(pexprSort);
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::UlHash
//
//	@doc:
//		Hash of components
//
//---------------------------------------------------------------------------
ULONG
COrderSpec::UlHash() const
{
	ULONG ulHash = 0;
	ULONG ulArity = m_pdrgpoe->UlLength();
	
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		COrderExpression *poe = (*m_pdrgpoe)[ul];
		ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHashPtr<CColRef>(poe->Pcr()));
	}
	
	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::PosCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the order spec with remapped columns
//
//---------------------------------------------------------------------------
COrderSpec *
COrderSpec::PosCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	COrderSpec *pos = GPOS_NEW(pmp) COrderSpec(pmp);

	const ULONG ulCols = m_pdrgpoe->UlLength();
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		COrderExpression *poe = (*m_pdrgpoe)[ul];
		IMDId *pmdid = poe->PmdidSortOp();
		pmdid->AddRef();

		const CColRef *pcr = poe->Pcr();
		ULONG ulId = pcr->UlId();
		CColRef *pcrMapped = phmulcr->PtLookup(&ulId);
		if (NULL == pcrMapped)
		{
			if (fMustExist)
			{
				CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();
				// not found in hashmap, so create a new colref and add to hashmap
				pcrMapped = pcf->PcrCopy(pcr);

#ifdef GPOS_DEBUG
				BOOL fResult =
#endif // GPOS_DEBUG
				phmulcr->FInsert(GPOS_NEW(pmp) ULONG(ulId), pcrMapped);
				GPOS_ASSERT(fResult);
			}
			else
			{
				pcrMapped = const_cast<CColRef*>(pcr);
			}
		}

		COrderSpec::ENullTreatment ent = poe->Ent();
		pos->Append(pmdid, pcrMapped, ent);
	}

	return pos;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::PosExcludeColumns
//
//	@doc:
//		Return a copy of the order spec after excluding the given columns
//
//---------------------------------------------------------------------------
COrderSpec *
COrderSpec::PosExcludeColumns
	(
	IMemoryPool *pmp,
	CColRefSet *pcrs
	)
{
	GPOS_ASSERT(NULL != pcrs);

	COrderSpec *pos = GPOS_NEW(pmp) COrderSpec(pmp);

	const ULONG ulCols = m_pdrgpoe->UlLength();
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		COrderExpression *poe = (*m_pdrgpoe)[ul];
		const CColRef *pcr = poe->Pcr();

		if (pcrs->FMember(pcr))
		{
			continue;
		}

		IMDId *pmdid = poe->PmdidSortOp();
		pmdid->AddRef();
		pos->Append(pmdid, pcr, poe->Ent());
	}

	return pos;
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::ExtractCols
//
//	@doc:
//		Extract columns from order spec into the given column set
//
//---------------------------------------------------------------------------
void
COrderSpec::ExtractCols
	(
	CColRefSet *pcrs
	)
	const
{
	GPOS_ASSERT(NULL != pcrs);

	const ULONG ulOrderExprs = m_pdrgpoe->UlLength();
	for (ULONG ul = 0; ul < ulOrderExprs; ul++)
	{
		pcrs->Include((*m_pdrgpoe)[ul]->Pcr());
	}
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::PcrsUsed
//
//	@doc:
//		Extract colref set from order components
//
//---------------------------------------------------------------------------
CColRefSet *
COrderSpec::PcrsUsed
	(
	IMemoryPool *pmp
	)
	const
{
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	ExtractCols(pcrs);
	
	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::Pcrs
//
//	@doc:
//		Extract colref set from order specs in the given array
//
//---------------------------------------------------------------------------
CColRefSet *
COrderSpec::Pcrs
	(
	IMemoryPool *pmp,
	DrgPos *pdrgpos
	)
{
	GPOS_ASSERT(NULL != pdrgpos);

	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	const ULONG ulOrderSpecs = pdrgpos->UlLength();
	for (ULONG ulSpec = 0; ulSpec < ulOrderSpecs; ulSpec++)
	{
		COrderSpec *pos = (*pdrgpos)[ulSpec];
		pos->ExtractCols(pcrs);
	}

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::PdrgposExclude
//
//	@doc:
//		Filter out array of order specs from order expressions using the
//		passed columns
//
//---------------------------------------------------------------------------
DrgPos *
COrderSpec::PdrgposExclude
	(
	IMemoryPool *pmp,
	DrgPos *pdrgpos,
	CColRefSet *pcrsToExclude
	)
{
	GPOS_ASSERT(NULL != pdrgpos);
	GPOS_ASSERT(NULL != pcrsToExclude);

	if (0 == pcrsToExclude->CElements())
	{
		// no columns to exclude
		pdrgpos->AddRef();
		return pdrgpos;
	}

	DrgPos *pdrgposNew = GPOS_NEW(pmp) DrgPos(pmp);
	const ULONG ulOrderSpecs = pdrgpos->UlLength();
	for (ULONG ulSpec = 0; ulSpec < ulOrderSpecs; ulSpec++)
	{
		COrderSpec *pos = (*pdrgpos)[ulSpec];
		COrderSpec *posNew = pos->PosExcludeColumns(pmp, pcrsToExclude);
		pdrgposNew->Append(posNew);
	}

	return pdrgposNew;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::OsPrint
//
//	@doc:
//		Print order spec
//
//---------------------------------------------------------------------------
IOstream &
COrderSpec::OsPrint
	(
	IOstream &os
	)
	const
{
	const ULONG ulArity = m_pdrgpoe->UlLength();
	if (0 == ulArity)
	{
		os << "<empty>";
	}
	else 
	{
		for (ULONG ul = 0; ul < ulArity; ul++)
		{
			(*m_pdrgpoe)[ul]->OsPrint(os) << " ";
		}
	}

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::FEqual
//
//	@doc:
//		 Matching function over order spec arrays
//
//---------------------------------------------------------------------------
BOOL
COrderSpec::FEqual
	(
	const DrgPos *pdrgposFirst,
	const DrgPos *pdrgposSecond
	)
{
	if (NULL == pdrgposFirst || NULL == pdrgposSecond)
	{
		return (NULL == pdrgposFirst && NULL ==pdrgposSecond);
	}

	if (pdrgposFirst->UlLength() != pdrgposSecond->UlLength())
	{
		return false;
	}

	const ULONG ulSize = pdrgposFirst->UlLength();
	BOOL fMatch = true;
	for (ULONG ul = 0; fMatch && ul < ulSize; ul++)
	{
		fMatch = (*pdrgposFirst)[ul]->FMatch((*pdrgposSecond)[ul]);
	}

	return fMatch;
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::UlHash
//
//	@doc:
//		 Combine hash values of a maximum number of entries
//
//---------------------------------------------------------------------------
ULONG
COrderSpec::UlHash
	(
	const DrgPos *pdrgpos,
	ULONG ulMaxSize
	)
{
	GPOS_ASSERT(NULL != pdrgpos);
	ULONG ulSize = std::min(ulMaxSize, pdrgpos->UlLength());

	ULONG ulHash = 0;
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		ulHash = gpos::UlCombineHashes(ulHash, (*pdrgpos)[ul]->UlHash());
	}

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::OsPrint
//
//	@doc:
//		 Print array of order spec objects
//
//---------------------------------------------------------------------------
IOstream &
COrderSpec::OsPrint
	(
	IOstream &os,
	const DrgPos *pdrgpos
	)
{
	const ULONG ulSize = pdrgpos->UlLength();
	os	<< "[";
	if (0 < ulSize)
	{
		for (ULONG ul = 0; ul < ulSize - 1; ul++)
		{
			(void) (*pdrgpos)[ul]->OsPrint(os);
			os <<	", ";
		}

		(void) (*pdrgpos)[ulSize - 1]->OsPrint(os);
	}

	return os << "]";
}


// EOF

