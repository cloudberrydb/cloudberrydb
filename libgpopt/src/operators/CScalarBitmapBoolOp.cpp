//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CScalarBitmapBoolOp.cpp
//
//	@doc:
//		Bitmap index probe scalar operator
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpopt/base/CColRef.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/metadata/CIndexDescriptor.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CScalarBitmapBoolOp.h"
#include "gpopt/xforms/CXform.h"

#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpopt;

const WCHAR CScalarBitmapBoolOp::m_rgwszBitmapOpType[EbitmapboolSentinel][30] =
{
	GPOS_WSZ_LIT("BitmapAnd"),
	GPOS_WSZ_LIT("BitmapOr")
};

//---------------------------------------------------------------------------
//	@function:
//		CScalarBitmapBoolOp::CScalarBitmapBoolOp
//
//	@doc:
//		Ctor
//		Takes ownership of the bitmap type id.
//
//---------------------------------------------------------------------------
CScalarBitmapBoolOp::CScalarBitmapBoolOp
	(
	IMemoryPool *pmp,
	EBitmapBoolOp ebitmapboolop,
	IMDId *pmdidBitmapType
	)
	:
	CScalar(pmp),
	m_ebitmapboolop(ebitmapboolop),
	m_pmdidBitmapType(pmdidBitmapType)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(EbitmapboolSentinel > ebitmapboolop);
	GPOS_ASSERT(NULL != pmdidBitmapType);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarBitmapBoolOp::~CScalarBitmapBoolOp
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScalarBitmapBoolOp::~CScalarBitmapBoolOp()
{
	m_pmdidBitmapType->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarBitmapBoolOp::UlHash
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CScalarBitmapBoolOp::UlHash() const
{
	ULONG ulBoolop = (ULONG) Ebitmapboolop();
	return gpos::UlCombineHashes(COperator::UlHash(), gpos::UlHash<ULONG>(&ulBoolop));
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarBitmapBoolOp::FMatch
//
//	@doc:
//		Match this operator with the given one.
//
//---------------------------------------------------------------------------
BOOL
CScalarBitmapBoolOp::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}
	CScalarBitmapBoolOp *popBitmapBoolOp = PopConvert(pop);

	return popBitmapBoolOp->Ebitmapboolop() == Ebitmapboolop() &&
		popBitmapBoolOp->PmdidType()->FEquals(m_pmdidBitmapType);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarBitmapBoolOp::OsPrint
//
//	@doc:
//		Debug print of this operator
//
//---------------------------------------------------------------------------
IOstream &
CScalarBitmapBoolOp::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " (";
	os << m_rgwszBitmapOpType[m_ebitmapboolop];
	os << ")";
	
	return os;
}

// EOF
