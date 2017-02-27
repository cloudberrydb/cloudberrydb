//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.

#include "gpopt/operators/COperator.h"
#include "gpopt/base/CDatumSortedSet.h"
#include "gpopt/operators/CScalarConst.h"
#include "gpopt/base/CUtils.h"
#include "gpos/common/CAutoRef.h"

using namespace gpopt;

CDatumSortedSet::CDatumSortedSet
	(
	IMemoryPool *pmp,
	CExpression *pexprArray,
	const IComparator *pcomp
	)
	:
	DrgPdatum(pmp),
	m_fIncludesNull(false)
{
	GPOS_ASSERT(COperator::EopScalarArray == pexprArray->Pop()->Eopid());

	const ULONG ulArrayExprArity = CUtils::UlScalarArrayArity(pexprArray);
	GPOS_ASSERT(0 < ulArrayExprArity);

	gpos::CAutoRef<DrgPdatum> aprngdatum(GPOS_NEW(pmp) DrgPdatum(pmp));
	for (ULONG ul = 0; ul < ulArrayExprArity; ul++)
	{
		CScalarConst *popScConst = CUtils::PScalarArrayConstChildAt(pexprArray, ul);
		IDatum *pdatum = popScConst->Pdatum();
		if (pdatum->FNull())
		{
			m_fIncludesNull = true;
		}
		else
		{
			pdatum->AddRef();
			aprngdatum->Append(pdatum);
		}
	}
	aprngdatum->Sort(&CUtils::IDatumCmp);

	// de-duplicate
	const ULONG ulRangeArrayArity = aprngdatum->UlLength();
	IDatum *pdatumPrev = (*aprngdatum)[0];
	pdatumPrev->AddRef();
	Append(pdatumPrev);
	for (ULONG ul = 1; ul < ulRangeArrayArity; ul++)
	{
		if (!pcomp->FEqual((*aprngdatum)[ul], pdatumPrev))
		{
			pdatumPrev = (*aprngdatum)[ul];
			pdatumPrev->AddRef();
			Append(pdatumPrev);
		}
	}
}

BOOL CDatumSortedSet::FIncludesNull() const
{
	return m_fIncludesNull;
}
