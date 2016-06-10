//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarSubqueryQuantified.cpp
//
//	@doc:
//		Implementation of quantified subquery operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "naucrates/md/IMDScalarOp.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/COptCtxt.h"

#include "gpopt/operators/CScalarSubqueryQuantified.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryQuantified::CScalarSubqueryQuantified
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarSubqueryQuantified::CScalarSubqueryQuantified
	(
	IMemoryPool *pmp,
	IMDId *pmdidScalarOp,
	const CWStringConst *pstrScalarOp,
	const CColRef *pcr
	)
	:
	CScalar(pmp),
	m_pmdidScalarOp(pmdidScalarOp),
	m_pstrScalarOp(pstrScalarOp),
	m_pcr(pcr)
{
	GPOS_ASSERT(pmdidScalarOp->FValid());
	GPOS_ASSERT(NULL != pstrScalarOp);
	GPOS_ASSERT(NULL != pcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryQuantified::~CScalarSubqueryQuantified
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScalarSubqueryQuantified::~CScalarSubqueryQuantified()
{
	m_pmdidScalarOp->Release();
	GPOS_DELETE(m_pstrScalarOp);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryQuantified::PstrOp
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CScalarSubqueryQuantified::PstrOp() const
{
	return m_pstrScalarOp;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryQuantified::PmdidOp
//
//	@doc:
//		Scalar operator metadata id
//
//---------------------------------------------------------------------------
IMDId *
CScalarSubqueryQuantified::PmdidOp() const
{
	return m_pmdidScalarOp;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryQuantified::PmdidType
//
//	@doc:
//		Type of scalar's value
//
//---------------------------------------------------------------------------
IMDId *
CScalarSubqueryQuantified::PmdidType() const
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	IMDId *pmdidType = pmda->Pmdscop(m_pmdidScalarOp)->PmdidTypeResult();

	GPOS_ASSERT(pmda->PtMDType<IMDTypeBool>()->Pmdid()->FEquals(pmdidType));

	return pmdidType;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryQuantified::UlHash
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CScalarSubqueryQuantified::UlHash() const
{
	return gpos::UlCombineHashes
				(
				COperator::UlHash(),
				gpos::UlCombineHashes
						(
						m_pmdidScalarOp->UlHash(),
						gpos::UlHashPtr<CColRef>(m_pcr)
						)
				);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryQuantified::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarSubqueryQuantified::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	// match if contents are identical
	CScalarSubqueryQuantified *popSsq = CScalarSubqueryQuantified::PopConvert(pop);
	return popSsq->Pcr() == m_pcr && popSsq->PmdidOp()->FEquals(m_pmdidScalarOp);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryQuantified::PcrsUsed
//
//	@doc:
//		Locally used columns
//
//---------------------------------------------------------------------------
CColRefSet *
CScalarSubqueryQuantified::PcrsUsed
	(
	IMemoryPool *pmp,
	 CExpressionHandle &exprhdl
	)
{
	// used columns is an empty set unless subquery column is an outer reference
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);

	CColRefSet *pcrsChildOutput = exprhdl.Pdprel(0 /* ulChildIndex */)->PcrsOutput();
	if (!pcrsChildOutput->FMember(m_pcr))
	{
		// subquery column is not produced by relational child, add it to used columns
		 pcrs->Include(m_pcr);
	}

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryQuantified::PpartinfoDerive
//
//	@doc:
//		Derive partition consumers
//
//---------------------------------------------------------------------------
CPartInfo *
CScalarSubqueryQuantified::PpartinfoDerive
	(
	IMemoryPool *, // pmp, 
	CExpressionHandle &exprhdl
	)
	const
{
	CPartInfo *ppartinfoChild = exprhdl.Pdprel(0 /*ulChildIndex*/)->Ppartinfo();
	GPOS_ASSERT(NULL != ppartinfoChild);
	ppartinfoChild->AddRef();
	return ppartinfoChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryQuantified::PexprSubqueryPred
//
//	@doc:
//		Build an expression for the quantified comparison of the subquery
//
//---------------------------------------------------------------------------
CExpression *
CScalarSubqueryQuantified::PexprSubqueryPred
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	GPOS_ASSERT(this == exprhdl.Pop());

	CScalarSubqueryQuantified *popSqQuantified = CScalarSubqueryQuantified::PopConvert(exprhdl.Pop());
	CExpression *pexprScalar = exprhdl.PexprScalarChild(1 /*ulChildIndex*/);
	const CColRef *pcr = popSqQuantified->Pcr();
	IMDId *pmdidOp = popSqQuantified->PmdidOp();
	const CWStringConst *pstr = popSqQuantified->PstrOp();

	pexprScalar->AddRef();
	pmdidOp->AddRef();

	return CUtils::PexprScalarCmp(pmp, pexprScalar, pcr, *pstr, pmdidOp);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryQuantified::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CScalarSubqueryQuantified::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId();
	os << "(" << PstrOp()->Wsz() << ")";
	os << "[";
	m_pcr->OsPrint(os);
	os << "]";

	return os;
}


// EOF

