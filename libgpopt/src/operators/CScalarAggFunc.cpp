//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarAggFunc.cpp
//
//	@doc:
//		Implementation of scalar aggregate function call operators
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "naucrates/md/IMDAggregate.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CScalarAggFunc.h"
#include "gpopt/operators/CExpressionHandle.h"

#include "naucrates/md/CMDIdGPDB.h"


using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarAggFunc::CScalarAggFunc
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarAggFunc::CScalarAggFunc
	(
	IMemoryPool *pmp,
	IMDId *pmdidAggFunc,
	IMDId *pmdidResolvedRetType,
	const CWStringConst *pstrAggFunc,
	BOOL fDistinct,
	EAggfuncStage eaggfuncstage,
	BOOL fSplit
	)
	:
	CScalar(pmp),
	m_pmdidAggFunc(pmdidAggFunc),
	m_pmdidResolvedRetType(pmdidResolvedRetType),
	m_pmdidRetType(NULL),
	m_pstrAggFunc(pstrAggFunc),
	m_fDistinct(fDistinct),
	m_eaggfuncstage(eaggfuncstage),
	m_fSplit(fSplit)
{
	GPOS_ASSERT(NULL != pmdidAggFunc);
	GPOS_ASSERT(NULL != pstrAggFunc);
	GPOS_ASSERT(pmdidAggFunc->FValid());
	GPOS_ASSERT_IMP(NULL != pmdidResolvedRetType, pmdidResolvedRetType->FValid());
	GPOS_ASSERT(EaggfuncstageSentinel > eaggfuncstage);

	// store id of type obtained by looking up MD cache
	IMDId *pmdid = PmdidLookupReturnType(m_pmdidAggFunc, (EaggfuncstageGlobal == m_eaggfuncstage));
	pmdid->AddRef();
	m_pmdidRetType = pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarAggFunc::PstrAggFunc
//
//	@doc:
//		Aggregate function name
//
//---------------------------------------------------------------------------
const CWStringConst *
CScalarAggFunc::PstrAggFunc() const
{
	return m_pstrAggFunc;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarAggFunc::Pmdid
//
//	@doc:
//		Aggregate function id
//
//---------------------------------------------------------------------------
IMDId *
CScalarAggFunc::Pmdid() const
{
	return m_pmdidAggFunc;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarAggFunc::FCountStar
//
//	@doc:
//		Is function count(*)?
//
//---------------------------------------------------------------------------
BOOL
CScalarAggFunc::FCountStar() const
{
	// TODO,  04/26/2012, make this function system-independent
	// using MDAccessor
	return m_pmdidAggFunc->FEquals(&CMDIdGPDB::m_mdidCountStar);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarAggFunc::FCountAny
//
//	@doc:
//		Is function count(Any)?
//
//---------------------------------------------------------------------------
BOOL
CScalarAggFunc::FCountAny() const
{
	// TODO,  04/26/2012, make this function system-independent
	// using MDAccessor
	return m_pmdidAggFunc->FEquals(&CMDIdGPDB::m_mdidCountAny);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarAggFunc::UlHash
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CScalarAggFunc::UlHash() const
{
	ULONG ulAggfuncstage = (ULONG) m_eaggfuncstage;
	return gpos::UlCombineHashes
					(
					UlCombineHashes(COperator::UlHash(), m_pmdidAggFunc->UlHash()),
					UlCombineHashes
						(
						gpos::UlHash<ULONG>(&ulAggfuncstage),
						UlCombineHashes(gpos::UlHash<BOOL>(&m_fDistinct),gpos::UlHash<BOOL>(&m_fSplit))
						)
					);
}

	
//---------------------------------------------------------------------------
//	@function:
//		CScalarAggFunc::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarAggFunc::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarAggFunc *popScAggFunc = CScalarAggFunc::PopConvert(pop);
		
		// match if func ids are identical
		return
				(
				(popScAggFunc->FDistinct() ==  m_fDistinct)
				&& (popScAggFunc->Eaggfuncstage() ==  Eaggfuncstage())
				&& (popScAggFunc->FSplit() ==  m_fSplit)
				&& m_pmdidAggFunc->FEquals(popScAggFunc->Pmdid())
				);
	}
	
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarAggFunc::PmdidLookupReturnType
//
//	@doc:
//		Lookup mdid of return type for given Agg function
//
//---------------------------------------------------------------------------
IMDId *
CScalarAggFunc::PmdidLookupReturnType
	(
	IMDId *pmdidAggFunc,
	BOOL fGlobal,
	CMDAccessor *pmdaInput
	)
{
	GPOS_ASSERT(NULL != pmdidAggFunc);
	CMDAccessor *pmda = pmdaInput;

	if (NULL == pmda)
	{
		pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	}
	GPOS_ASSERT(NULL != pmda);

	// get aggregate function return type from the MD cache
	const IMDAggregate *pmdagg = pmda->Pmdagg(pmdidAggFunc);
	if (fGlobal)
	{
		return pmdagg->PmdidTypeResult();
	}

	return pmdagg->PmdidTypeIntermediate();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarAggFunc::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CScalarAggFunc::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " (";
	os << PstrAggFunc()->Wsz();
	os << " , Distinct: ";
	os << (m_fDistinct ? "true" : "false");
	os << " , Aggregate Stage: ";

	switch (m_eaggfuncstage)
	{
		case EaggfuncstageGlobal:
				os << "Global";
				break;

		case EaggfuncstageIntermediate:
				os << "Intermediate";
				break;

		case EaggfuncstageLocal:
				os << "Local";
				break;

		default:
			GPOS_ASSERT(!"Unsupported aggregate type");
	}

	os << ")";
	
	return os;
}


// EOF

