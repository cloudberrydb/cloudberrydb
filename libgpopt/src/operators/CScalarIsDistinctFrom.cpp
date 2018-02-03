//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CScalarIsDistinctFrom.cpp
//
//	@doc:
//		Implementation of scalar IDF comparison operator
//---------------------------------------------------------------------------

#include "gpopt/base/COptCtxt.h"

#include "gpopt/mdcache/CMDAccessorUtils.h"

#include "gpopt/operators/CScalarIsDistinctFrom.h"

using namespace gpopt;
using namespace gpmd;


// conversion function
CScalarIsDistinctFrom *
CScalarIsDistinctFrom::PopConvert
(
 COperator *pop
 )
{
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(EopScalarIsDistinctFrom == pop->Eopid());
	
	return reinterpret_cast<CScalarIsDistinctFrom*>(pop);
}

CScalar::EBoolEvalResult
CScalarIsDistinctFrom::Eber(DrgPul *pdrgpulChildren) const
{
	GPOS_ASSERT(2 == pdrgpulChildren->UlLength());

	EBoolEvalResult firstResult = (EBoolEvalResult) * (*pdrgpulChildren)[0];
	EBoolEvalResult secondResult = (EBoolEvalResult) * (*pdrgpulChildren)[1];

	if (firstResult == EberUnknown || secondResult == EberUnknown)
	{
		return CScalar::EberUnknown;

	}
	else if (firstResult != secondResult)
	{
		return CScalar::EberTrue;
	}
	return CScalar::EberFalse;
}

BOOL
CScalarIsDistinctFrom::FMatch
(
 COperator *pop
 )
const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarIsDistinctFrom *popIDF = CScalarIsDistinctFrom::PopConvert(pop);
		
		// match if operator mdids are identical
		return PmdidOp()->FEquals(popIDF->PmdidOp());
	}
	
	return false;
}

// get commuted scalar IDF operator
CScalarIsDistinctFrom *
CScalarIsDistinctFrom::PopCommutedOp
	(
	IMemoryPool *pmp,
	COperator *pop
	)
{
	
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	IMDId *pmdid = PmdidCommuteOp(pmda, pop);
	if (NULL != pmdid && pmdid->FValid())
	{
		return GPOS_NEW(pmp) CScalarIsDistinctFrom(pmp, pmdid, Pstr(pmp, pmda, pmdid));
	}
	return NULL;
}

// EOF

