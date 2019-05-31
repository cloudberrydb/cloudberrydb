//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CDrvdPropScalar.cpp
//
//	@doc:
//		Scalar derived properties
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CScalar.h"
#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CScalarProjectElement.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropScalar::CDrvdPropScalar
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDrvdPropScalar::CDrvdPropScalar()
	:
	m_pcrsDefined(NULL),
	m_pcrsSetReturningFunction(NULL),
	m_pcrsUsed(NULL),
	m_fHasSubquery(false),
	m_ppartinfo(NULL),
	m_pfp(NULL),
	m_fHasNonScalarFunction(false),
	m_ulDistinctAggs(0),
	m_fHasMultipleDistinctAggs(false),
	m_fHasScalarArrayCmp(false)
{}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropScalar::~CDrvdPropScalar
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDrvdPropScalar::~CDrvdPropScalar()
{
	CRefCount::SafeRelease(m_pcrsDefined);
	CRefCount::SafeRelease(m_pcrsSetReturningFunction);
	CRefCount::SafeRelease(m_pcrsUsed);
	CRefCount::SafeRelease(m_ppartinfo);
	CRefCount::SafeRelease(m_pfp);
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropScalar::Derive
//
//	@doc:
//		Derive scalar props
//
//---------------------------------------------------------------------------
void
CDrvdPropScalar::Derive
	(
	CMemoryPool *mp,
	CExpressionHandle &exprhdl,
	CDrvdPropCtxt * // pdpctxt
	)
{
	CScalar *popScalar = CScalar::PopConvert(exprhdl.Pop());
	
	// call derivation functions on the operator
	GPOS_ASSERT(NULL == m_pcrsDefined);
	m_pcrsDefined = popScalar->PcrsDefined(mp, exprhdl);

	GPOS_ASSERT(NULL == m_pcrsSetReturningFunction);
	m_pcrsSetReturningFunction = popScalar->PcrsSetReturningFunction(mp, exprhdl);
	
	GPOS_ASSERT(NULL == m_pcrsUsed);
	m_pcrsUsed = popScalar->PcrsUsed(mp, exprhdl);

	// derive function properties
	m_pfp = popScalar->PfpDerive(mp, exprhdl);

	// add defined and used columns of children
	const ULONG arity = exprhdl.Arity();
	for (ULONG i = 0; i < arity; i++)
	{
		// only propagate properties from scalar children
		if (exprhdl.FScalarChild(i))
		{
			m_pcrsDefined->Union(exprhdl.GetDrvdScalarProps(i)->PcrsDefined());
			m_pcrsUsed->Union(exprhdl.GetDrvdScalarProps(i)->PcrsUsed());
			m_pcrsSetReturningFunction->Union(exprhdl.GetDrvdScalarProps(i)->PcrsSetReturningFunction());
		}
		else
		{
			GPOS_ASSERT(CUtils::FSubquery(popScalar));

			// parent operator is a subquery, add outer references
			// from its relational child as used columns
 			m_pcrsUsed->Union(exprhdl.GetRelationalProperties(0)->PcrsOuter());
		}
	}

	// derive existence of subqueries
	GPOS_ASSERT(!m_fHasSubquery);
	m_fHasSubquery = popScalar->FHasSubquery(exprhdl);
	
	if (m_fHasSubquery)
	{
		m_ppartinfo = popScalar->PpartinfoDerive(mp, exprhdl);
	}
	else
	{
		m_ppartinfo = GPOS_NEW(mp) CPartInfo(mp);
	}

	m_fHasNonScalarFunction = popScalar->FHasNonScalarFunction(exprhdl);

	if (COperator::EopScalarProjectList == exprhdl.Pop()->Eopid())
	{
		m_ulDistinctAggs = CScalarProjectList::UlDistinctAggs(exprhdl);
		m_fHasMultipleDistinctAggs = CScalarProjectList::FHasMultipleDistinctAggs(exprhdl);
	}

	if (COperator::EopScalarProjectElement == exprhdl.Pop()->Eopid())
	{
		if (m_fHasNonScalarFunction)
		{
			CScalarProjectElement *pspeProject = (CScalarProjectElement *)(exprhdl.Pop());
			m_pcrsSetReturningFunction->Include(pspeProject->Pcr());
		}
	}

	m_fHasScalarArrayCmp = popScalar->FHasScalarArrayCmp(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropScalar::GetDrvdScalarProps
//
//	@doc:
//		Short hand for conversion
//
//---------------------------------------------------------------------------
CDrvdPropScalar *
CDrvdPropScalar::GetDrvdScalarProps
	(
	DrvdPropArray *pdp
	)
{
	GPOS_ASSERT(NULL != pdp);
	GPOS_ASSERT(EptScalar == pdp->Ept() && "This is not a scalar properties container");

	return dynamic_cast<CDrvdPropScalar*>(pdp);
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropScalar::FSatisfies
//
//	@doc:
//		Check for satisfying required properties
//
//---------------------------------------------------------------------------
BOOL
CDrvdPropScalar::FSatisfies
	(
	const CReqdPropPlan *prpp
	)
	const
{
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT(NULL != prpp->PcrsRequired());

	BOOL fSatisfies = m_pcrsDefined->ContainsAll(prpp->PcrsRequired());

	return fSatisfies;
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropScalar::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CDrvdPropScalar::OsPrint
	(
	IOstream &os
	)
	const
{
		os	<<	"Defined Columns: [" << *m_pcrsDefined << "], "
			<<	"Used Columns: [" << *m_pcrsUsed << "], "
			<<	"Set Returning Function Columns: [" << *m_pcrsSetReturningFunction << "], "
			<<	"Has Subqs: [" << m_fHasSubquery << "], "
			<<	"Function Properties: [" << *m_pfp << "], "
			<<	"Has Non-scalar Funcs: [" << m_fHasNonScalarFunction << "], ";

		if (0 < m_ulDistinctAggs)
		{
			os
				<<	"Distinct Aggs: [" << m_ulDistinctAggs << "]"
				<<	"Has Multiple Distinct Aggs: [" << m_fHasMultipleDistinctAggs << "]";
		}

		return os;
}

// EOF
