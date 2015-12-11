//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalCTEAnchor.cpp
//
//	@doc:
//		Implementation of CTE anchor operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalCTEAnchor.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEAnchor::CLogicalCTEAnchor
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalCTEAnchor::CLogicalCTEAnchor
	(
	IMemoryPool *pmp
	)
	:
	CLogical(pmp),
	m_ulId(0)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEAnchor::CLogicalCTEAnchor
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalCTEAnchor::CLogicalCTEAnchor
	(
	IMemoryPool *pmp,
	ULONG ulId
	)
	:
	CLogical(pmp),
	m_ulId(ulId)
{}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEAnchor::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalCTEAnchor::PcrsDeriveOutput
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
{
	return PcrsDeriveOutputPassThru(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEAnchor::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalCTEAnchor::PkcDeriveKeys
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
//		CLogicalCTEAnchor::PpartinfoDerive
//
//	@doc:
//		Derive part consumer
//
//---------------------------------------------------------------------------
CPartInfo *
CLogicalCTEAnchor::PpartinfoDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	CPartInfo *ppartinfoChild = exprhdl.Pdprel(0 /*ulChildIndex*/)->Ppartinfo();
	GPOS_ASSERT(NULL != ppartinfoChild);

	CExpression *pexprProducer = COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_ulId);
	GPOS_ASSERT(NULL != pexprProducer);
	CPartInfo *ppartinfoCTEProducer = CDrvdPropRelational::Pdprel(pexprProducer->PdpDerive())->Ppartinfo();

	return CPartInfo::PpartinfoCombine(pmp, ppartinfoChild, ppartinfoCTEProducer);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEAnchor::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalCTEAnchor::Maxcard
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	// pass on max card of first child
	return exprhdl.Pdprel(0)->Maxcard();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEAnchor::FMatch
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalCTEAnchor::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalCTEAnchor *popCTEAnchor = CLogicalCTEAnchor::PopConvert(pop);

	return m_ulId == popCTEAnchor->UlId();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEAnchor::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalCTEAnchor::UlHash() const
{
	return gpos::UlCombineHashes(COperator::UlHash(), m_ulId);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEAnchor::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalCTEAnchor::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfCTEAnchor2Sequence);
	(void) pxfs->FExchangeSet(CXform::ExfCTEAnchor2TrivialSelect);
	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEAnchor::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalCTEAnchor::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " (";
	os << m_ulId;
	os << ")";

	return os;
}

// EOF
