//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalSequence.cpp
//
//	@doc:
//		Implementation of logical sequence operator
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

#include "gpopt/operators/CLogicalSequence.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequence::CLogicalSequence
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalSequence::CLogicalSequence
	(
	IMemoryPool *pmp
	)
	:
	CLogical(pmp)
{
	GPOS_ASSERT(NULL != pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequence::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalSequence::FMatch
	(
	COperator *pop
	)
	const
{
	return pop->Eopid() == Eopid();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequence::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalSequence::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfImplementSequence);
	return pxfs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequence::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalSequence::PcrsDeriveOutput
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
{
	GPOS_ASSERT(1 <= exprhdl.UlArity());
	
	// get output columns of last child
	CColRefSet *pcrs = exprhdl.Pdprel(exprhdl.UlArity() - 1)->PcrsOutput();
	pcrs->AddRef();
	
	return pcrs;
}



//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequence::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalSequence::PkcDeriveKeys
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	// return key of last child
	const ULONG ulArity = exprhdl.UlArity();
	return PkcDeriveKeysPassThru(exprhdl, ulArity - 1 /* ulChild */);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequence::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalSequence::Maxcard
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	// pass on max card of last child
	return exprhdl.Pdprel(exprhdl.UlArity() - 1)->Maxcard();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequence::PpartinfoDerive
//
//	@doc:
//		Derive part consumers
//
//---------------------------------------------------------------------------
CPartInfo *
CLogicalSequence::PpartinfoDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	return PpartinfoDeriveCombine(pmp, exprhdl);
}


// EOF

