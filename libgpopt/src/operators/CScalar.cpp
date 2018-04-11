//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CScalar.cpp
//
//	@doc:
//		Implementation of base class of scalar operators
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CScalar.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CScalar::PdpCreate
//
//	@doc:
//		Create base container of derived properties
//
//---------------------------------------------------------------------------
CDrvdProp *
CScalar::PdpCreate
	(
	IMemoryPool *pmp
	)
	const
{
	return GPOS_NEW(pmp) CDrvdPropScalar();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalar::PrpCreate
//
//	@doc:
//		Create base container of required properties
//
//---------------------------------------------------------------------------
CReqdProp *
CScalar::PrpCreate
	(
	IMemoryPool * // pmp
	)
	const
{
	GPOS_ASSERT(!"Cannot compute required properties on scalar");
	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalar::FHasSubquery
//
//	@doc:
//		Derive existence of subqueries from expression handle
//
//---------------------------------------------------------------------------
BOOL
CScalar::FHasSubquery
	(
	CExpressionHandle &exprhdl
	)
{
	// if operator is a subquery, return immediately
	if (CUtils::FSubquery(exprhdl.Pop()))
	{
		return true;
	}

	// otherwise, iterate over scalar children
	const ULONG ulArity = exprhdl.UlArity();
	for (ULONG i = 0; i < ulArity; i++)
	{
		if (exprhdl.FScalarChild(i))
		{
			CDrvdPropScalar *pdpscalar = exprhdl.Pdpscalar(i);
			if (pdpscalar->FHasSubquery())
			{
				return true;
			}
		}
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalar::EberConjunction
//
//	@doc:
//		Perform conjunction of boolean evaluation results
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalar::EberConjunction
	(
	DrgPul *pdrgpulChildren
	)
{
	GPOS_ASSERT(NULL != pdrgpulChildren);
	GPOS_ASSERT(1 < pdrgpulChildren->UlLength());

	BOOL fAllChildrenTrue = true;
	BOOL fNullChild = false;
	BOOL fUnknownChild = false;

	const ULONG ulChildren = pdrgpulChildren->UlLength();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		EBoolEvalResult eber = (EBoolEvalResult) *((*pdrgpulChildren)[ul]);
		switch (eber)
		{
			case EberFalse:
				// if any child is False, conjunction returns False
				return EberFalse;

			case EberNull:
				// if a child is NULL, we cannot return here yet since another child
				// might be False, which yields the conjunction to False
				fNullChild = true;
				break;

			case EberUnknown:
				fUnknownChild = true;
				break;

			default:
				break;
		}

		fAllChildrenTrue = fAllChildrenTrue && (EberTrue == eber);
	}

	if (fAllChildrenTrue)
	{
		// conjunction of all True children gives True
		return EberTrue;
	}

	if (fUnknownChild)
	{
		// at least one Unknown child yields conjunction Unknown
		// for example,
		//   (Unknown AND True) = Unknown
		//   (Unknown AND Null) = Unknown
		return EberUnknown;
	}

	if (fNullChild)
	{
		// all children are either True or Null, conjunction returns Null
		return EberNull;
	}

	return EberUnknown;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalar::EberDisjunction
//
//	@doc:
//		Perform disjunction of child boolean evaluation results
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalar::EberDisjunction
	(
	DrgPul *pdrgpulChildren
	)
{
	GPOS_ASSERT(NULL != pdrgpulChildren);
	GPOS_ASSERT(1 < pdrgpulChildren->UlLength());

	BOOL fAllChildrenFalse = true;
	BOOL fNullChild = false;
	BOOL fUnknownChild = false;

	const ULONG ulChildren = pdrgpulChildren->UlLength();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		EBoolEvalResult eber = (EBoolEvalResult) *((*pdrgpulChildren)[ul]);
		switch (eber)
		{
			case EberTrue:
				// if any child is True, disjunction returns True
				return EberTrue;

			case EberNull:
				// if a child is NULL, we cannot return here yet since another child
				// might be True, which yields the disjunction to True
				fNullChild = true;
				break;

			case EberUnknown:
				fUnknownChild = true;
				break;

			default:
				break;
		}

		fAllChildrenFalse = fAllChildrenFalse && (EberFalse == eber);
	}

	if (fAllChildrenFalse)
	{
		// disjunction of all False children gives False
		return EberFalse;
	}

	if (fUnknownChild)
	{
		// at least one Unknown child yields disjunction Unknown
		// for example,
		//   (Unknown OR False) = Unknown
		//   (Unknown OR Null) = Unknown

		return EberUnknown;
	}

	if (fNullChild)
	{
		// children are either False or Null, disjunction returns Null
		return EberNull;
	}

	return EberUnknown;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalar::EberNullOnAnyNullChild
//
//	@doc:
//		Return Null if any child is Null
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalar::EberNullOnAnyNullChild
	(
	DrgPul *pdrgpulChildren
	)
{
	GPOS_ASSERT(NULL != pdrgpulChildren);

	const ULONG ulChildren = pdrgpulChildren->UlLength();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		EBoolEvalResult eber = (EBoolEvalResult) *((*pdrgpulChildren)[ul]);
		if (EberNull == eber)
		{
			return EberNull;
		}
	}

	return EberUnknown;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalar::EberNullOnAllNullChildren
//
//	@doc:
//		Return Null evaluation result if all children results are Null
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalar::EberNullOnAllNullChildren
	(
	DrgPul *pdrgpulChildren
	)
{
	GPOS_ASSERT(NULL != pdrgpulChildren);

	const ULONG ulChildren = pdrgpulChildren->UlLength();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		EBoolEvalResult eber = (EBoolEvalResult) *((*pdrgpulChildren)[ul]);
		if (EberNull != eber)
		{
			return EberUnknown;
		}
	}

	return EberNull;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalar::EberEvaluate
//
//	@doc:
//		Perform boolean evaluation of the given expression
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalar::EberEvaluate
	(
	IMemoryPool *pmp,
	CExpression *pexprScalar
	)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexprScalar);

	COperator *pop = pexprScalar->Pop();
	GPOS_ASSERT(pop->FScalar());

	const ULONG ulArity = pexprScalar->UlArity();
	DrgPul *pdrgpulChildren = NULL;

	if (!CUtils::FSubquery(pop))
	{
		// do not recurse into subqueries
		if (0 < ulArity)
		{
			pdrgpulChildren = GPOS_NEW(pmp) DrgPul(pmp);
		}
		for (ULONG ul = 0; ul < ulArity; ul++)
		{
			CExpression *pexprChild = (*pexprScalar)[ul];
			EBoolEvalResult eberChild = EberEvaluate(pmp, pexprChild);
			pdrgpulChildren->Append(GPOS_NEW(pmp) ULONG(eberChild));
		}
	}

	EBoolEvalResult eber = PopConvert(pop)->Eber(pdrgpulChildren);
	CRefCount::SafeRelease(pdrgpulChildren);

	return eber;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalar::FHasNonScalarFunction
//
//	@doc:
//		Derive existence of non-scalar functions from expression handle
//
//---------------------------------------------------------------------------
BOOL
CScalar::FHasNonScalarFunction
	(
	CExpressionHandle &exprhdl
	)
{
	// if operator is a subquery, return immediately
	if (CUtils::FSubquery(exprhdl.Pop()))
	{
		return false;
	}

	// otherwise, iterate over scalar children
	const ULONG ulArity = exprhdl.UlArity();
	for (ULONG i = 0; i < ulArity; i++)
	{
		if (exprhdl.FScalarChild(i))
		{
			CDrvdPropScalar *pdpscalar = exprhdl.Pdpscalar(i);
			if (pdpscalar->FHasNonScalarFunction())
			{
				return true;
			}
		}
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalar::PdrgppartconDeriveCombineScalar
//
//	@doc:
//		Combine partition consumer arrays of all scalar children
//
//---------------------------------------------------------------------------
CPartInfo *
CScalar::PpartinfoDeriveCombineScalar
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
{
	const ULONG ulArity = exprhdl.UlArity();
	GPOS_ASSERT(0 < ulArity);

	CPartInfo *ppartinfo = GPOS_NEW(pmp) CPartInfo(pmp);
	
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		if (exprhdl.FScalarChild(ul))
		{
			CPartInfo *ppartinfoChild = exprhdl.Pdpscalar(ul)->Ppartinfo();
			GPOS_ASSERT(NULL != ppartinfoChild);
			CPartInfo *ppartinfoCombined = CPartInfo::PpartinfoCombine(pmp, ppartinfo, ppartinfoChild);
			ppartinfo->Release();
			ppartinfo = ppartinfoCombined;
		}
	}

	return ppartinfo;
}

BOOL
CScalar::FHasScalarArrayCmp
	(
	CExpressionHandle &exprhdl
	)
{
	// if operator is a ScalarArrayCmp, return immediately
	if (COperator::EopScalarArrayCmp == exprhdl.Pop()->Eopid())
	{
		return true;
	}

	// otherwise, iterate over scalar children
	const ULONG ulArity = exprhdl.UlArity();
	for (ULONG i = 0; i < ulArity; i++)
	{
		if (exprhdl.FScalarChild(i))
		{
			CDrvdPropScalar *pdpscalar = exprhdl.Pdpscalar(i);
			if (pdpscalar->FHasScalarArrayCmp())
			{
				return true;
			}
		}
	}

	return false;
}

// EOF

