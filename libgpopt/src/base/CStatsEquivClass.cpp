//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CStatsEquivClass.cpp
//
//	@doc:
//		Implementation of constraints
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CStatsEquivClass.h"
#include "gpopt/operators/CPredicateUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClass::PdrgpcrsEquivClassFromScalarExpr
//
//	@doc:
//		Create equivalence classes from scalar expression
//
//---------------------------------------------------------------------------
DrgPcrs *
CStatsEquivClass::PdrgpcrsEquivClassFromScalarExpr
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(pexpr->Pop()->FScalar());

	CDrvdPropScalar *pdpscalar = CDrvdPropScalar::Pdpscalar(pexpr->PdpDerive());

	CColRefSet *pcrs = pdpscalar->PcrsUsed();
	ULONG ulCols = pcrs->CElements();

	if (1 >= ulCols)
	{
		return NULL;
	}

	switch (pexpr->Pop()->Eopid())
	{
		case COperator::EopScalarBoolOp:
			return PdrgpcrsEquivClassFromScalarBoolOp(pmp, pexpr);

		case COperator::EopScalarCmp:
			return PdrgpcrsEquivClassFromScalarCmp(pmp, pexpr);

		default:
			return NULL;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClass::PdrgpcrsEquivClassFromScalarCmp
//
//	@doc:
//		Create an equivalence class from scalar comparison
//
//---------------------------------------------------------------------------
DrgPcrs *
CStatsEquivClass::PdrgpcrsEquivClassFromScalarCmp
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CUtils::FScalarCmp(pexpr));

	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	const CColRef *pcrLeft = CUtils::PcrExtractFromScIdOrCastScId(pexprLeft);
	const CColRef *pcrRight = CUtils::PcrExtractFromScIdOrCastScId(pexprRight);

	if (NULL == pcrLeft || NULL == pcrRight)
	{
		// TODO: , Jan 24 2014; add support for other cases besides:
		// (col cmp col)
		// (cast(col) cmp col)
		// (col cmp cast(col))
		// (cast(col) cmp cast(col))

		return NULL;
	}

	DrgPcrs *pdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);

	if (CPredicateUtils::FEquality(pexpr))
	{
		CColRefSet *pcrsNew = PcrsEquivClass(pmp, pcrLeft, pcrRight);
		pdrgpcrs->Append(pcrsNew);
	}

	return pdrgpcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClass::PdrgpcrsEquivClassFromScalarBoolOp
//
//	@doc:
//		Create equivalence classes from scalar boolean expression
//
//---------------------------------------------------------------------------
DrgPcrs *
CStatsEquivClass::PdrgpcrsEquivClassFromScalarBoolOp
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CUtils::FScalarBoolOp(pexpr));

	const BOOL fExprOr = CPredicateUtils::FOr(pexpr);

	DrgPcrs *pdrgpcrs = NULL;

	const ULONG ulArity = pexpr->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		DrgPcrs *pdrgpcrsChild = PdrgpcrsEquivClassFromScalarExpr(pmp, (*pexpr)[ul]);

		if (NULL == pdrgpcrsChild && fExprOr)
		{
			// for a disjunctive predicate all of its children should at least
			// produce a single equivalence class to warrant a merge
			CRefCount::SafeRelease(pdrgpcrs);

			return NULL;
		}
		else if (NULL == pdrgpcrs)
		{
			pdrgpcrs = pdrgpcrsChild;
		}
		else if (NULL != pdrgpcrsChild)
		{
			DrgPcrs *pdrgpcrsMerged = CConstraint::PdrgpcrsMergeFromBoolOp(pmp, pexpr, pdrgpcrs, pdrgpcrsChild);

			pdrgpcrs->Release();
			pdrgpcrs = pdrgpcrsMerged;
			pdrgpcrsChild->Release();
		}
	}

	return pdrgpcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsEquivClass::PcrsEquivClass
//
//	@doc:
//		Create a new equivalence class between the two column references
//
//---------------------------------------------------------------------------
CColRefSet *
CStatsEquivClass::PcrsEquivClass
	(
	IMemoryPool *pmp,
	const CColRef *pcrLeft,
	const CColRef *pcrRight
	)
{
	GPOS_ASSERT(NULL != pcrLeft);
	GPOS_ASSERT(NULL != pcrRight);

	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrs->Include(pcrLeft);
	pcrs->Include(pcrRight);

	return pcrs;
}


// EOF
