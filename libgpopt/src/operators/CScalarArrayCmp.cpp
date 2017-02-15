//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarArrayCmp.cpp
//
//	@doc:
//		Implementation of scalar array comparison operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSet.h"

#include "gpopt/mdcache/CMDAccessorUtils.h"

#include "gpopt/operators/CScalarArrayCmp.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"

#include "naucrates/md/IMDTypeBool.h"

using namespace gpopt;
using namespace gpmd;

const CHAR CScalarArrayCmp::m_rgszCmpType[EarrcmpSentinel][10] =
{
	"Any",
	"All"
};


//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::CScalarArrayCmp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarArrayCmp::CScalarArrayCmp
	(
	IMemoryPool *pmp,
	IMDId *pmdidOp,
	const CWStringConst *pstrOp,
	EArrCmpType earrcmpt
	)
	:
	CScalar(pmp),
	m_pmdidOp(pmdidOp),
	m_pscOp(pstrOp),
	m_earrccmpt(earrcmpt),
	m_fReturnsNullOnNullInput(false)
{
	GPOS_ASSERT(pmdidOp->FValid());
	GPOS_ASSERT(EarrcmpSentinel > earrcmpt);

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	m_fReturnsNullOnNullInput = CMDAccessorUtils::FScalarOpReturnsNullOnNullInput(pmda, m_pmdidOp);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCmp::Pstr
//
//	@doc:
//		Comparison operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CScalarArrayCmp::Pstr() const
{
	return m_pscOp;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCmp::PmdidOp
//
//	@doc:
//		Comparison operator mdid
//
//---------------------------------------------------------------------------
IMDId *
CScalarArrayCmp::PmdidOp() const
{
	return m_pmdidOp;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCmp::UlHash
//
//	@doc:
//		Operator specific hash function; combined hash of operator id and
//		metadata id
//
//---------------------------------------------------------------------------
ULONG
CScalarArrayCmp::UlHash() const
{
	return gpos::UlCombineHashes
					(
					gpos::UlCombineHashes(COperator::UlHash(), m_pmdidOp->UlHash()),
					m_earrccmpt
					);
}

	
//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCmp::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarArrayCmp::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarArrayCmp *popCmp = CScalarArrayCmp::PopConvert(pop);
		
		// match if operator oid are identical
		return m_earrccmpt == popCmp->Earrcmpt() && m_pmdidOp->FEquals(popCmp->PmdidOp());
	}
	
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCmp::PmdidType
//
//	@doc:
//		Expression type
//
//---------------------------------------------------------------------------
IMDId *
CScalarArrayCmp::PmdidType() const
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	return pmda->PtMDType<IMDTypeBool>()->Pmdid();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCmp::Eber
//
//	@doc:
//		Perform boolean expression evaluation
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalarArrayCmp::Eber
	(
	DrgPul *pdrgpulChildren
	)
	const
{
	if (m_fReturnsNullOnNullInput)
	{
		return EberNullOnAnyNullChild(pdrgpulChildren);
	}

	return EberUnknown;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCmp::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CScalarArrayCmp::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " " <<  m_rgszCmpType[m_earrccmpt] << " (";
	os << Pstr()->Wsz();
	os << ")";
	
	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCmp::PexprExpand
//
//	@doc:
//		Expand array comparison expression into a conjunctive/disjunctive
//		expression
//
//---------------------------------------------------------------------------
CExpression *
CScalarArrayCmp::PexprExpand
	(
	IMemoryPool *pmp,
	CExpression *pexprArrayCmp
	)
{
	GPOS_ASSERT(NULL != pexprArrayCmp);
	GPOS_ASSERT(EopScalarArrayCmp == pexprArrayCmp->Pop()->Eopid());

	CExpression *pexprLeft = (*pexprArrayCmp)[0];
	CExpression *pexprRight = (*pexprArrayCmp)[1];
	CScalarArrayCmp *popArrayCmp = CScalarArrayCmp::PopConvert(pexprArrayCmp->Pop());

	if (CUtils::FScalarArrayCoerce(pexprRight))
	{
		pexprRight = (*pexprRight)[0];
	}
	const ULONG ulArrayElems = pexprRight->UlArity();
	if (!CUtils::FScalarArray(pexprRight) || 0 == ulArrayElems)
	{
		// if right child is not an actual array (e.g., Const of type array), return input
		// expression without expansion
		pexprArrayCmp->AddRef();
		return pexprArrayCmp;
	}

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ul = 0; ul < ulArrayElems; ul++)
	{
		CExpression *pexprArrayElem = (*pexprRight)[ul];
		pexprLeft->AddRef();
		pexprArrayElem->AddRef();
		const CWStringConst *pstrOpName = popArrayCmp->Pstr();
		IMDId *pmdidOp = popArrayCmp->PmdidOp();
		GPOS_ASSERT(IMDId::FValid(pmdidOp));

		pmdidOp->AddRef();

		CExpression *pexprCmp = CUtils::PexprScalarCmp(pmp, pexprLeft, pexprArrayElem, *pstrOpName, pmdidOp);
		pdrgpexpr->Append(pexprCmp);
	}
	GPOS_ASSERT(0 < pdrgpexpr->UlLength());

	// deduplicate resulting array
	DrgPexpr *pdrgpexprDeduped = CUtils::PdrgpexprDedup(pmp, pdrgpexpr);
	pdrgpexpr->Release();

	EArrCmpType earrcmpt = popArrayCmp->Earrcmpt();
	if (EarrcmpAny == earrcmpt)
	{
		return CPredicateUtils::PexprDisjunction(pmp, pdrgpexprDeduped);
	}
	GPOS_ASSERT(EarrcmpAll == earrcmpt);

	return CPredicateUtils::PexprConjunction(pmp, pdrgpexprDeduped);
}



// EOF

