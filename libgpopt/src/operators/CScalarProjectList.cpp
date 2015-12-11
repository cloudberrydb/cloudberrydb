//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CScalarProjectList.cpp
//
//	@doc:
//		Implementation of scalar projection list operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSet.h"

#include "gpopt/operators/CScalarProjectList.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/xforms/CXformUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CScalarProjectList::CScalarProjectList
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CScalarProjectList::CScalarProjectList
	(
	IMemoryPool *pmp
	)
	:
	CScalar(pmp)
{
}

	
//---------------------------------------------------------------------------
//	@function:
//		CScalarProjectList::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarProjectList::FMatch
	(
	COperator *pop
	)
	const
{
	return (pop->Eopid() == Eopid());
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarProjectList::FInputOrderSensitive
//
//	@doc:
//		Prjection lists are insensitive to order; no dependencies between
//		elements;
//
//---------------------------------------------------------------------------
BOOL
CScalarProjectList::FInputOrderSensitive() const
{
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarProjectList::UlDistinctAggs
//
//	@doc:
//		Return number of Distinct Aggs in the given project list
//
//---------------------------------------------------------------------------
ULONG
CScalarProjectList::UlDistinctAggs
	(
	CExpressionHandle &exprhdl
	)
{
	CExpression *pexprPrjList =  exprhdl.PexprScalar();

	GPOS_ASSERT(NULL != pexprPrjList);
	GPOS_ASSERT(COperator::EopScalarProjectList == pexprPrjList->Pop()->Eopid());

	ULONG ulDistinctAggs = 0;
	const ULONG ulArity = pexprPrjList->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprPrjEl = (*pexprPrjList)[ul];
		CExpression *pexprChild = (*pexprPrjEl)[0];
		COperator::EOperatorId eopidChild = pexprChild->Pop()->Eopid();

		if (COperator::EopScalarAggFunc == eopidChild)
		{
			CScalarAggFunc *popScAggFunc = CScalarAggFunc::PopConvert(pexprChild->Pop());
			if (popScAggFunc->FDistinct())
			{
				ulDistinctAggs++;
			}
		}
		else if (COperator::EopScalarWindowFunc == eopidChild)
		{
			CScalarWindowFunc *popScWinFunc = CScalarWindowFunc::PopConvert(pexprChild->Pop());
			if (popScWinFunc->FDistinct() && popScWinFunc->FAgg())
			{
				ulDistinctAggs++;
			}
		}
	}

	return ulDistinctAggs;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarProjectList::FHasMultipleDistinctAggs
//
//	@doc:
//		Check if given project list has multiple distinct aggregates, for example:
//			select count(distinct a), sum(distinct b) from T;
//
//			select count(distinct a) over(), sum(distinct b) over() from T;
//
//---------------------------------------------------------------------------
BOOL
CScalarProjectList::FHasMultipleDistinctAggs
	(
	CExpressionHandle &exprhdl
	)
{
	CExpression *pexprPrjList = exprhdl.PexprScalar();

	GPOS_ASSERT(NULL != pexprPrjList);
	GPOS_ASSERT(COperator::EopScalarProjectList == pexprPrjList->Pop()->Eopid());
	if (0 == UlDistinctAggs(exprhdl))
	{
		return false;
	}

	CAutoMemoryPool amp;
	HMExprDrgPexpr *phmexprdrgpexpr = NULL;
	ULONG ulDifferentDQAs = 0;
	CXformUtils::MapPrjElemsWithDistinctAggs(amp.Pmp(), pexprPrjList, &phmexprdrgpexpr, &ulDifferentDQAs);
	phmexprdrgpexpr->Release();

	return (1 < ulDifferentDQAs);
}


// EOF

