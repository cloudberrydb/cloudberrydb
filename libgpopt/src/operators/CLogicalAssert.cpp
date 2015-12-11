//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalAssert.cpp
//
//	@doc:
//		Implementation of assert operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalAssert.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/operators/CPatternTree.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalAssert::CLogicalAssert
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalAssert::CLogicalAssert
	(
	IMemoryPool *pmp
	)
	:
	CLogicalUnary(pmp),
	m_pexc(NULL)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalAssert::CLogicalAssert
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalAssert::CLogicalAssert
	(
	IMemoryPool *pmp,
	CException *pexc
	)
	:
	CLogicalUnary(pmp),
	m_pexc(pexc)
{
	GPOS_ASSERT(NULL != pexc);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalAssert::FMatch
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CLogicalAssert::FMatch
	(
	COperator *pop
	)
	const
{
	if (Eopid() != pop->Eopid())
	{
		return false;
	}
	
	CLogicalAssert *popAssert = CLogicalAssert::PopConvert(pop); 
	return CException::FEqual(*(popAssert->Pexc()), *m_pexc);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalAssert::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalAssert::PcrsDeriveOutput
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
{
	return PcrsDeriveOutputPassThru(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalAssert::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalAssert::PkcDeriveKeys
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	return PkcDeriveKeysPassThru(exprhdl, 0 /* ulChild */);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalAssert::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalAssert::PxfsCandidates
	(
	IMemoryPool *pmp
	) 
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfImplementAssert);
	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalAssert::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalAssert::Maxcard
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	// in case of a false condition or a contradiction, maxcard should be 1
	CExpression *pexprScalar = exprhdl.PexprScalarChild(1);
	GPOS_ASSERT(NULL != pexprScalar);

	if (CUtils::FScalarConstFalse(pexprScalar) ||
		CDrvdPropRelational::Pdprel(exprhdl.Pdp())->Ppc()->FContradiction())
	{
		return CMaxCard(1 /*ull*/);
	}

	// if Assert operator was generated from MaxOneRow operator,
	// then a max cardinality of 1 is expected
	if (NULL != exprhdl.Pgexpr() &&
		CXform::ExfMaxOneRow2Assert == exprhdl.Pgexpr()->ExfidOrigin())
	{
		return CMaxCard(1 /*ull*/);
	}

	// pass on max card of first child
	return exprhdl.Pdprel(0)->Maxcard();
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalAssert::PstatsDerive
//
//	@doc:
//		Derive statistics based on filter predicates
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalAssert::PstatsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	DrgPstat * // not used
	)
	const
{
	CMaxCard maxcard = CLogicalAssert::PopConvert(exprhdl.Pop())->Maxcard(pmp, exprhdl);
	if (1 == maxcard.Ull())
	{
		// a max card of one requires re-scaling stats
		IStatistics *pstats = exprhdl.Pstats(0);
		return  pstats->PstatsScale(pmp, CDouble(1.0 / pstats->DRows()));
	}

	return PstatsPassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalAssert::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalAssert::OsPrint
	(
	IOstream &os
	)
	const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}
	
	os << SzId() << " (Error code: " << m_pexc->SzSQLState() << ")";
	return os;
}

// EOF

