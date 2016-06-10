//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Inc.
//
//	@filename:
//		CScalarMinMax.cpp
//
//	@doc:
//		Implementation of scalar MinMax operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/COptCtxt.h"

#include "gpopt/mdcache/CMDAccessorUtils.h"

#include "gpopt/operators/CScalarMinMax.h"
#include "gpopt/operators/CExpressionHandle.h"

#include "naucrates/md/IMDTypeBool.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarMinMax::CScalarMinMax
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarMinMax::CScalarMinMax
	(
	IMemoryPool *pmp,
	IMDId *pmdidType,
	EScalarMinMaxType esmmt
	)
	:
	CScalar(pmp),
	m_pmdidType(pmdidType),
	m_esmmt(esmmt),
	m_fBoolReturnType(false)
{
	GPOS_ASSERT(pmdidType->FValid());
	GPOS_ASSERT(EsmmtSentinel > esmmt);

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	m_fBoolReturnType = CMDAccessorUtils::FBoolType(pmda, m_pmdidType);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarMinMax::~CScalarMinMax
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScalarMinMax::~CScalarMinMax()
{
	m_pmdidType->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarMinMax::UlHash
//
//	@doc:
//		Operator specific hash function; combined hash of operator id and
//		return type id
//
//---------------------------------------------------------------------------
ULONG
CScalarMinMax::UlHash() const
{
	ULONG ulminmax = (ULONG) this->Esmmt();

	return gpos::UlCombineHashes
					(
						m_pmdidType->UlHash(),
						gpos::UlCombineHashes(COperator::UlHash(), gpos::UlHash<ULONG>(&ulminmax))
					);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarMinMax::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarMinMax::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CScalarMinMax *popScMinMax = CScalarMinMax::PopConvert(pop);

	// match if return types are identical
	return popScMinMax->Esmmt() == m_esmmt &&
			popScMinMax->PmdidType()->FEquals(m_pmdidType);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarMinMax::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CScalarMinMax::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " (";

	if (EsmmtMin == m_esmmt)
	{
		os << "Min";
	}
	else
	{
		os << "Max";
	}
	os << ")";

	return os;
}

// EOF

