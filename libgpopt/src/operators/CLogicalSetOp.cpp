//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2012 EMC Corp.
//
//	@filename:
//		CLogicalSetOp.cpp
//
//	@doc:
//		Implementation of setops
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/error/CAutoTrace.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/CConstraintInterval.h"
#include "gpopt/base/CConstraintNegation.h"
#include "gpopt/operators/CLogicalSetOp.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::CLogicalSetOp
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalSetOp::CLogicalSetOp
	(
	IMemoryPool *pmp
	)
	:
	CLogical(pmp),
	m_pdrgpcrOutput(NULL),
	m_pdrgpdrgpcrInput(NULL),
	m_pcrsOutput(NULL),
	m_pdrgpcrsInput(NULL)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::CLogicalSetOp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalSetOp::CLogicalSetOp
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcrOutput,
	DrgDrgPcr *pdrgpdrgpcrInput
	)
	:
	CLogical(pmp),
	m_pdrgpcrOutput(pdrgpcrOutput),
	m_pdrgpdrgpcrInput(pdrgpdrgpcrInput),
	m_pcrsOutput(NULL),
	m_pdrgpcrsInput(NULL)
{
	GPOS_ASSERT(NULL != pdrgpcrOutput);
	GPOS_ASSERT(NULL != pdrgpdrgpcrInput);

	BuildColumnSets(pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::CLogicalSetOp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalSetOp::CLogicalSetOp
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcrOutput,
	DrgPcr *pdrgpcrLeft,
	DrgPcr *pdrgpcrRight
	)
	:
	CLogical(pmp),
	m_pdrgpcrOutput(pdrgpcrOutput),
	m_pdrgpdrgpcrInput(NULL)
{	
	GPOS_ASSERT(NULL != pdrgpcrOutput);
	GPOS_ASSERT(NULL != pdrgpcrLeft);
	GPOS_ASSERT(NULL != pdrgpcrRight);

	m_pdrgpdrgpcrInput = GPOS_NEW(pmp) DrgDrgPcr(pmp, 2);

	m_pdrgpdrgpcrInput->Append(pdrgpcrLeft);
	m_pdrgpdrgpcrInput->Append(pdrgpcrRight);

	BuildColumnSets(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::~CLogicalSetOp
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalSetOp::~CLogicalSetOp()
{
	CRefCount::SafeRelease(m_pdrgpcrOutput);
	CRefCount::SafeRelease(m_pdrgpdrgpcrInput);
	CRefCount::SafeRelease(m_pcrsOutput);
	CRefCount::SafeRelease(m_pdrgpcrsInput);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::BuildColumnSets
//
//	@doc:
//		Build set representation of input/output columns for faster
//		set operations
//
//---------------------------------------------------------------------------
void
CLogicalSetOp::BuildColumnSets
	(
	IMemoryPool *pmp
	)
{
	GPOS_ASSERT(NULL != m_pdrgpcrOutput);
	GPOS_ASSERT(NULL != m_pdrgpdrgpcrInput);
	GPOS_ASSERT(NULL == m_pcrsOutput);
	GPOS_ASSERT(NULL == m_pdrgpcrsInput);

	m_pcrsOutput = GPOS_NEW(pmp) CColRefSet(pmp, m_pdrgpcrOutput);
	m_pdrgpcrsInput = GPOS_NEW(pmp) DrgPcrs(pmp);
	const ULONG ulChildren = m_pdrgpdrgpcrInput->UlLength();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CColRefSet *pcrsInput = GPOS_NEW(pmp) CColRefSet(pmp, (*m_pdrgpdrgpcrInput)[ul]);
		m_pdrgpcrsInput->Append(pcrsInput);

		m_pcrsLocalUsed->Include(pcrsInput);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalSetOp::PcrsDeriveOutput
	(
	IMemoryPool *, // pmp
	CExpressionHandle &
#ifdef GPOS_DEBUG
	 exprhdl
#endif // GPOS_DEBUG
	)
{
#ifdef GPOS_DEBUG
	const ULONG ulArity = exprhdl.UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CColRefSet *pcrsChildOutput = exprhdl.Pdprel(ul)->PcrsOutput();
		CColRefSet *pcrsInput = (*m_pdrgpcrsInput)[ul];
		GPOS_ASSERT(pcrsChildOutput->FSubset(pcrsInput) &&
				"Unexpected outer references in SetOp input");
	}
#endif // GPOS_DEBUG

	m_pcrsOutput->AddRef();

	return m_pcrsOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalSetOp::PkcDeriveKeys
	(
	IMemoryPool *pmp,
	CExpressionHandle & // exprhdl
	)
	const
{
	// TODO: 3/3/2012 - ; we can do better by remapping the keys between
	// all children and check if they align

	// True set ops return sets, hence, all output columns are keys
	m_pdrgpcrOutput->AddRef();
	return GPOS_NEW(pmp) CKeyCollection(pmp, m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PpartinfoDerive
//
//	@doc:
//		Derive partition consumer info
//
//---------------------------------------------------------------------------
CPartInfo *
CLogicalSetOp::PpartinfoDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	const ULONG ulArity = exprhdl.UlArity();
	GPOS_ASSERT(0 < ulArity);

	// start with the part info of the first child
	CPartInfo *ppartinfo = exprhdl.Pdprel(0 /*ulChildIndex*/)->Ppartinfo();
	ppartinfo->AddRef();

	for (ULONG ul = 1; ul < ulArity; ul++)
	{
		CPartInfo *ppartinfoChild = exprhdl.Pdprel(ul)->Ppartinfo();
		GPOS_ASSERT(NULL != ppartinfoChild);

		DrgPcr *pdrgpcrInput = (*m_pdrgpdrgpcrInput)[ul];
		GPOS_ASSERT(pdrgpcrInput->UlLength() == m_pdrgpcrOutput->UlLength());

		CPartInfo *ppartinfoRemapped = ppartinfoChild->PpartinfoWithRemappedKeys(pmp, pdrgpcrInput, m_pdrgpcrOutput);
		CPartInfo *ppartinfoCombined = CPartInfo::PpartinfoCombine(pmp, ppartinfo, ppartinfoRemapped);
		ppartinfoRemapped->Release();

		ppartinfo->Release();
		ppartinfo = ppartinfoCombined;
	}

	return ppartinfo;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalSetOp::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalSetOp *popSetOp = CLogicalSetOp::PopConvert(pop);
	DrgDrgPcr *pdrgpdrgpcrInput = popSetOp->PdrgpdrgpcrInput();
	const ULONG ulArity = pdrgpdrgpcrInput->UlLength();

	if (ulArity != m_pdrgpdrgpcrInput->UlLength() ||
		!m_pdrgpcrOutput->FEqual(popSetOp->PdrgpcrOutput()))
	{
		return false;
	}

	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		if (!(*m_pdrgpdrgpcrInput)[ul]->FEqual((*pdrgpdrgpcrInput)[ul]))
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PdrgpcrsOutputEquivClasses
//
//	@doc:
//		Get output equivalence classes
//
//---------------------------------------------------------------------------
DrgPcrs *
CLogicalSetOp::PdrgpcrsOutputEquivClasses
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	BOOL fIntersect
	)
	const
{
	const ULONG ulChildren = exprhdl.UlArity();
	DrgPcrs *pdrgpcrs = PdrgpcrsInputMapped(pmp, exprhdl, 0 /*ulChild*/);

	for (ULONG ul = 1; ul < ulChildren; ul++)
	{
		DrgPcrs *pdrgpcrsChild = PdrgpcrsInputMapped(pmp, exprhdl, ul);
		DrgPcrs *pdrgpcrsMerged = NULL;

		if (fIntersect)
		{
			// merge with the equivalence classes we have so far
			pdrgpcrsMerged = CUtils::PdrgpcrsMergeEquivClasses(pmp, pdrgpcrs, pdrgpcrsChild);
		}
		else
		{
			// in case of a union, an equivalence class must be coming from all
			// children to be part of the output
			pdrgpcrsMerged = CUtils::PdrgpcrsIntersectEquivClasses(pmp, pdrgpcrs, pdrgpcrsChild);

		}
		pdrgpcrsChild->Release();
		pdrgpcrs->Release();
		pdrgpcrs = pdrgpcrsMerged;
	}

	return pdrgpcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PdrgpcrsInputMapped
//
//	@doc:
//		Get equivalence classes from one input child, mapped to output columns
//
//---------------------------------------------------------------------------
DrgPcrs *
CLogicalSetOp::PdrgpcrsInputMapped
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	ULONG ulChild
	)
	const
{
	DrgPcrs *pdrgpcrsInput = exprhdl.Pdprel(ulChild)->Ppc()->PdrgpcrsEquivClasses();
	const ULONG ulLen = pdrgpcrsInput->UlLength();

	CColRefSet* pcrsChildInput = (*m_pdrgpcrsInput)[ulChild];
	DrgPcrs *pdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
		pcrs->Include((*pdrgpcrsInput)[ul]);
		pcrs->Intersection(pcrsChildInput);

		if (0 == pcrs->CElements())
		{
			pcrs->Release();
			continue;
		}

		// replace each input column with its corresponding output column
		pcrs->Replace((*m_pdrgpdrgpcrInput)[ulChild], m_pdrgpcrOutput);

		pdrgpcrs->Append(pcrs);
	}

	return pdrgpcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PdrgpcnstrColumn
//
//	@doc:
//		Get constraints for a given output column from all children
//
//---------------------------------------------------------------------------
DrgPcnstr *
CLogicalSetOp::PdrgpcnstrColumn
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	ULONG ulColIndex,
	ULONG ulStart
	)
	const
{
	DrgPcnstr *pdrgpcnstr = GPOS_NEW(pmp) DrgPcnstr(pmp);

	CColRef *pcr = (*m_pdrgpcrOutput)[ulColIndex];
	if (!CUtils::FConstrainableType(pcr->Pmdtype()->Pmdid()))
	{
		return pdrgpcnstr;
	}

	const ULONG ulChildren = exprhdl.UlArity();
	for (ULONG ul = ulStart; ul < ulChildren; ul++)
	{
		CConstraint *pcnstr = PcnstrColumn(pmp, exprhdl, ulColIndex, ul);
		if (NULL == pcnstr)
		{
			pcnstr = CConstraintInterval::PciUnbounded(pmp, pcr, true /*fIsNull*/);
		}
		GPOS_ASSERT (NULL != pcnstr);
		pdrgpcnstr->Append(pcnstr);
	}

	return pdrgpcnstr;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PcnstrColumn
//
//	@doc:
//		Get constraint for a given output column from a given children
//
//---------------------------------------------------------------------------
CConstraint *
CLogicalSetOp::PcnstrColumn
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	ULONG ulColIndex,
	ULONG ulChild
	)
	const
{
	GPOS_ASSERT(ulChild < exprhdl.UlArity());

	// constraint from child
	CConstraint *pcnstrChild = exprhdl.Pdprel(ulChild)->Ppc()->Pcnstr();
	if (NULL == pcnstrChild)
	{
		return NULL;
	}

	// part of constraint on the current input column
	CConstraint *pcnstrCol = pcnstrChild->Pcnstr(pmp, (*(*m_pdrgpdrgpcrInput)[ulChild])[ulColIndex]);
	if (NULL == pcnstrCol)
	{
		return NULL;
	}

	// make a copy of this constraint but for the output column instead
	CConstraint *pcnstrOutput = pcnstrCol->PcnstrRemapForColumn(pmp, (*m_pdrgpcrOutput)[ulColIndex]);
	pcnstrCol->Release();
	return pcnstrOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PpcDeriveConstraintIntersectUnion
//
//	@doc:
//		Derive constraint property for intersect and union operators
//
//---------------------------------------------------------------------------
CPropConstraint *
CLogicalSetOp::PpcDeriveConstraintIntersectUnion
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	BOOL fIntersect
	)
	const
{
	const ULONG ulCols = m_pdrgpcrOutput->UlLength();

	DrgPcnstr *pdrgpcnstr = GPOS_NEW(pmp) DrgPcnstr(pmp);
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		// get constraints for this column from all children
		DrgPcnstr *pdrgpcnstrCol = PdrgpcnstrColumn(pmp, exprhdl, ul, 0 /*ulStart*/);

		CConstraint *pcnstrCol = NULL;
		if (fIntersect)
		{
			pcnstrCol = CConstraint::PcnstrConjunction(pmp, pdrgpcnstrCol);
		}
		else
		{
			pcnstrCol = CConstraint::PcnstrDisjunction(pmp, pdrgpcnstrCol);
		}

		if (NULL != pcnstrCol)
		{
			pdrgpcnstr->Append(pcnstrCol);
		}
	}

	CConstraint *pcnstrAll = CConstraint::PcnstrConjunction(pmp, pdrgpcnstr);

	DrgPcrs *pdrgpcrs = PdrgpcrsOutputEquivClasses(pmp, exprhdl, fIntersect);

	return GPOS_NEW(pmp) CPropConstraint(pmp, pdrgpcrs, pcnstrAll);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PcrsStat
//
//	@doc:
//		Compute required stats columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalSetOp::PcrsStat
	(
	IMemoryPool *, // pmp
	CExpressionHandle &, //exprhdl,
	CColRefSet *, //pcrsInput
	ULONG ulChildIndex
	)
	const
{
	CColRefSet *pcrs = (*m_pdrgpcrsInput)[ulChildIndex];
	pcrs->AddRef();

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalSetOp::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " Output: (";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
	os << ")";
	
	os << ", Input: [";
	const ULONG ulChildren = m_pdrgpdrgpcrInput->UlLength();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		os << "(";
		CUtils::OsPrintDrgPcr(os, (*m_pdrgpdrgpcrInput)[ul]);
		os << ")";

		if (ul < ulChildren - 1)
		{
			os << ", ";
		}
	}
	os << "]";

	return os;
}

// EOF
