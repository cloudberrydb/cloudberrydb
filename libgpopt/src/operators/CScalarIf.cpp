//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarIf.cpp
//
//	@doc:
//		Implementation of scalar if operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSet.h"

#include "gpopt/operators/CScalarIf.h"
#include "gpopt/operators/CExpressionHandle.h"

#include "gpopt/mdcache/CMDAccessorUtils.h"

#include "naucrates/md/IMDTypeBool.h"

using namespace gpopt;
using namespace gpmd;


//---------------------------------------------------------------------------
//	@function:
//		CScalarIf::CScalarIf
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarIf::CScalarIf
	(
	IMemoryPool *pmp,
	IMDId *pmdid
	)
	:
	CScalar(pmp),
	m_pmdidType(pmdid),
	m_fBoolReturnType(false)
{
	GPOS_ASSERT(pmdid->FValid());

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	m_fBoolReturnType = CMDAccessorUtils::FBoolType(pmda, m_pmdidType);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarIf::UlHash
//
//	@doc:
//		Operator specific hash function; combined hash of operator id and
//		return type id
//
//---------------------------------------------------------------------------
ULONG
CScalarIf::UlHash() const
{
	return gpos::UlCombineHashes(COperator::UlHash(), m_pmdidType->UlHash());
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarIf::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarIf::FMatch
	(
	COperator *pop
	)
	const
{
	if(pop->Eopid() == Eopid())
	{
		CScalarIf *popScIf = CScalarIf::PopConvert(pop);

		// match if return types are identical
		return popScIf->PmdidType()->FEquals(m_pmdidType);
	}

	return false;
}

// EOF

