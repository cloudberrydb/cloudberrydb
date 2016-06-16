//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalIndexScan.cpp
//
//	@doc:
//		Implementation of index scan operator
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/error/CAutoTrace.h"

#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalIndexScan.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/cost/ICostModel.h"



using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexScan::CPhysicalIndexScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalIndexScan::CPhysicalIndexScan
	(
	IMemoryPool *pmp,
	CIndexDescriptor *pindexdesc,
	CTableDescriptor *ptabdesc,
	ULONG ulOriginOpId,
	const CName *pnameAlias,
	DrgPcr *pdrgpcrOutput,
	COrderSpec *pos
	)
	:
	CPhysicalScan(pmp, pnameAlias, ptabdesc, pdrgpcrOutput),
	m_pindexdesc(pindexdesc),
	m_ulOriginOpId(ulOriginOpId),
	m_pos(pos)
{
	GPOS_ASSERT(NULL != pindexdesc);
	GPOS_ASSERT(NULL != pos);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexScan::~CPhysicalIndexScan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalIndexScan::~CPhysicalIndexScan()
{
	m_pindexdesc->Release();
	m_pos->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexScan::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalIndexScan::EpetOrder
	(
	CExpressionHandle &, // exprhdl
	const CEnfdOrder *peo
	)
	const
{
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(!peo->PosRequired()->FEmpty());

	if (peo->FCompatible(m_pos))
	{
		// required order is already established by the index
		return CEnfdProp::EpetUnnecessary;
	}

	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexScan::UlHash
//
//	@doc:
//		Combine pointers for table descriptor, index descriptor and Eop
//
//---------------------------------------------------------------------------
ULONG
CPhysicalIndexScan::UlHash() const
{
	ULONG ulHash = gpos::UlCombineHashes
					(
					COperator::UlHash(),
					gpos::UlCombineHashes
							(
							m_pindexdesc->Pmdid()->UlHash(),
							gpos::UlHashPtr<CTableDescriptor>(m_ptabdesc)
							)
					);
	ulHash = gpos::UlCombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrOutput));

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexScan::FMatch
//
//	@doc:
//		match operator
//
//---------------------------------------------------------------------------
BOOL
CPhysicalIndexScan::FMatch
	(
	COperator *pop
	)
	const
{
	return CUtils::FMatchIndex(this, pop);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexScan::PexprMatchEqualitySide
//
//	@doc:
//		Search the given array of predicates for an equality predicate
//		that has one side equal to the given expression,
//		if found, return the other side of equality, otherwise return NULL
//
//---------------------------------------------------------------------------
CExpression *
CPhysicalIndexScan::PexprMatchEqualitySide
	(
	CExpression *pexprToMatch,
	DrgPexpr *pdrgpexpr // array of predicates to inspect
	)
{
	GPOS_ASSERT(NULL != pexprToMatch);
	GPOS_ASSERT(NULL != pdrgpexpr);

	CExpression *pexprMatching = NULL;
	const ULONG ulSize = pdrgpexpr->UlLength();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CExpression *pexprPred = (*pdrgpexpr)[ul];
		if (!CPredicateUtils::FEquality(pexprPred))
		{
			continue;
		}

		// extract equality sides
		CExpression *pexprPredOuter = (*pexprPred)[0];
		CExpression *pexprPredInner = (*pexprPred)[1];

		IMDId *pmdidTypeOuter = CScalar::PopConvert(pexprPredOuter->Pop())->PmdidType();
		IMDId *pmdidTypeInner = CScalar::PopConvert(pexprPredInner->Pop())->PmdidType();
		if (!pmdidTypeOuter->FEquals(pmdidTypeInner))
		{
			// only consider equality of identical types
			continue;
		}

		if (CUtils::FEqual(pexprPredOuter, pexprToMatch))
		{
			pexprMatching = pexprPredInner;
			break;
		}

		if (CUtils::FEqual(pexprPredInner, pexprToMatch))
		{
			pexprMatching = pexprPredOuter;
			break;
		}
	}

	return pexprMatching;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexScan::PdshashedDeriveWithOuterRefs
//
//	@doc:
//		Derive hashed distribution when index conditions have outer
//		references
//
//---------------------------------------------------------------------------
CDistributionSpecHashed *
CPhysicalIndexScan::PdshashedDeriveWithOuterRefs
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	GPOS_ASSERT(exprhdl.FHasOuterRefs());
	GPOS_ASSERT(CDistributionSpec::EdtHashed == m_pds->Edt());

	CExpression *pexprIndexPred = exprhdl.PexprScalarChild(0 /*ulChildIndex*/);
	DrgPexpr *pdrgpexpr = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprIndexPred);

	DrgPexpr *pdrgpexprMatching = GPOS_NEW(pmp) DrgPexpr(pmp);
	CDistributionSpecHashed *pdshashed = CDistributionSpecHashed::PdsConvert(m_pds);
	DrgPexpr *pdrgpexprHashed = pdshashed->Pdrgpexpr();
	const ULONG ulSize = pdrgpexprHashed->UlLength();

	BOOL fSuccess = true;
	for (ULONG ul = 0; fSuccess && ul < ulSize; ul++)
	{
		CExpression *pexpr = (*pdrgpexprHashed)[ul];
		CExpression *pexprMatching = PexprMatchEqualitySide(pexpr, pdrgpexpr);
		fSuccess = (NULL != pexprMatching);
		if (fSuccess)
		{
			pexprMatching->AddRef();
			pdrgpexprMatching->Append(pexprMatching);
		}
	}
	pdrgpexpr->Release();

	if (fSuccess)
	{
		GPOS_ASSERT(pdrgpexprMatching->UlLength() == pdrgpexprHashed->UlLength());

		// create a matching hashed distribution request
		BOOL fNullsColocated = pdshashed->FNullsColocated();
		CDistributionSpecHashed *pdshashedEquiv = GPOS_NEW(pmp) CDistributionSpecHashed(pdrgpexprMatching, fNullsColocated);

		pdrgpexprHashed->AddRef();
		CDistributionSpecHashed *pdshashedResult = GPOS_NEW(pmp) CDistributionSpecHashed(pdrgpexprHashed, fNullsColocated, pdshashedEquiv);

		return pdshashedResult;
	}

	pdrgpexprMatching->Release();

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexScan::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalIndexScan::PdsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	if (CDistributionSpec::EdtHashed == m_pds->Edt() && exprhdl.FHasOuterRefs())
	{
		// if index conditions have outer references and index relation is hashed,
		// check to see if we can derive an equivalent hashed distribution for the
		// outer references
		CDistributionSpecHashed *pdshashed = PdshashedDeriveWithOuterRefs(pmp, exprhdl);
		if (NULL != pdshashed)
		{
			return pdshashed;
		}
	}

	// otherwise, return local distribution
	m_pds->AddRef();

	return m_pds;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexScan::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalIndexScan::OsPrint
	(
	IOstream &os
	)
	const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	os << SzId() << " ";
	// index name
	os << "  Index Name: (";
	m_pindexdesc->Name().OsPrint(os);
	// table name
	os <<")";
	os << ", Table Name: (";
	m_ptabdesc->Name().OsPrint(os);
	os <<")";
	os << ", Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
	os << "]";

	return os;
}

// EOF

