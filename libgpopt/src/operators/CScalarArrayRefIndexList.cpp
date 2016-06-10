//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CScalarArrayRefIndexList.cpp
//
//	@doc:
//		Implementation of scalar arrayref index list
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CScalarArrayRefIndexList.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayRefIndexList::CScalarArrayRefIndexList
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarArrayRefIndexList::CScalarArrayRefIndexList
	(
	IMemoryPool *pmp,
	EIndexListType eilt
	)
	:
	CScalar(pmp),
	m_eilt(eilt)
{
	GPOS_ASSERT(EiltSentinel > eilt);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayRefIndexList::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarArrayRefIndexList::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CScalarArrayRefIndexList *popIndexList = CScalarArrayRefIndexList::PopConvert(pop);

	return m_eilt == popIndexList->Eilt();
}

// EOF

