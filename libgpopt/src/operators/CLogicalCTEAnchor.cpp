//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalCTEAnchor.cpp
//
//	@doc:
//		Implementation of CTE anchor operator
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
	IMemoryPool *mp
	)
	:
	CLogical(mp),
	m_id(0)
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
	IMemoryPool *mp,
	ULONG id
	)
	:
	CLogical(mp),
	m_id(id)
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
	IMemoryPool *, // mp
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
	IMemoryPool *, // mp
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
	IMemoryPool *mp,
	CExpressionHandle &exprhdl
	)
	const
{
	CPartInfo *ppartinfoChild = exprhdl.GetRelationalProperties(0 /*child_index*/)->Ppartinfo();
	GPOS_ASSERT(NULL != ppartinfoChild);

	CExpression *pexprProducer = COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_id);
	GPOS_ASSERT(NULL != pexprProducer);
	CPartInfo *ppartinfoCTEProducer = CDrvdPropRelational::GetRelationalProperties(pexprProducer->PdpDerive())->Ppartinfo();

	return CPartInfo::PpartinfoCombine(mp, ppartinfoChild, ppartinfoCTEProducer);
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
	IMemoryPool *, // mp
	CExpressionHandle &exprhdl
	)
	const
{
	// pass on max card of first child
	return exprhdl.GetRelationalProperties(0)->Maxcard();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEAnchor::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalCTEAnchor::Matches
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

	return m_id == popCTEAnchor->Id();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEAnchor::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalCTEAnchor::HashValue() const
{
	return gpos::CombineHashes(COperator::HashValue(), m_id);
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
	IMemoryPool *mp
	)
	const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfCTEAnchor2Sequence);
	(void) xform_set->ExchangeSet(CXform::ExfCTEAnchor2TrivialSelect);
	return xform_set;
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
	os << m_id;
	os << ")";

	return os;
}

// EOF
