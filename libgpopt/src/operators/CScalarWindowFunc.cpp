//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarWindowFunc.cpp
//
//	@doc:
//		Implementation of scalar window function call operators
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "naucrates/md/IMDAggregate.h"
#include "naucrates/md/IMDFunction.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSet.h"

#include "gpopt/operators/CScalarWindowFunc.h"
#include "gpopt/operators/CScalarFunc.h"
#include "gpopt/operators/CExpressionHandle.h"

#include "gpopt/mdcache/CMDAccessorUtils.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarWindowFunc::CScalarWindowFunc
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarWindowFunc::CScalarWindowFunc
	(
	IMemoryPool *pmp,
	IMDId *pmdidFunc,
	IMDId *pmdidRetType,
	const CWStringConst *pstrFunc,
	EWinStage ewinstage,
	BOOL fDistinct,
	BOOL fStarArg,
	BOOL fSimpleAgg
	)
	:
	CScalarFunc(pmp),
	m_ewinstage(ewinstage),
	m_fDistinct(fDistinct),
	m_fStarArg(fStarArg),
	m_fSimpleAgg(fSimpleAgg),
	m_fAgg(false)
{
	GPOS_ASSERT(pmdidFunc->FValid());
	GPOS_ASSERT(pmdidRetType->FValid());
	m_pmdidFunc = pmdidFunc;
	m_pmdidRetType = pmdidRetType;
	m_pstrFunc = pstrFunc;

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	m_fAgg = pmda->FAggWindowFunc(m_pmdidFunc);
	if (!m_fAgg)
	{
		const IMDFunction *pmdfunc = pmda->Pmdfunc(m_pmdidFunc);
		m_efs = pmdfunc->EfsStability();
		m_efda = pmdfunc->EfdaDataAccess();
	}
	else
	{
 	 	// TODO: , Aug 15, 2012; pull out properties of aggregate functions
		m_efs = IMDFunction::EfsImmutable;
		m_efda = IMDFunction::EfdaNoSQL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarWindowFunc::UlHash
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CScalarWindowFunc::UlHash() const
{
	return gpos::UlCombineHashes
					(
					UlCombineHashes
						(
						UlCombineHashes
							(
							UlCombineHashes
								(
								gpos::UlCombineHashes
									(
									COperator::UlHash(),
									gpos::UlCombineHashes
										(
											m_pmdidFunc->UlHash(),
											m_pmdidRetType->UlHash()
										)
									),
								m_ewinstage
								),
							gpos::UlHash<BOOL>(&m_fDistinct)
							),
						gpos::UlHash<BOOL>(&m_fStarArg)
						),
					gpos::UlHash<BOOL>(&m_fSimpleAgg)
					);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarWindowFunc::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarWindowFunc::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarWindowFunc *popFunc = CScalarWindowFunc::PopConvert(pop);

		// match if the func id, and properties are identical
		return ((popFunc->FDistinct() ==  m_fDistinct)
				&& (popFunc->FStarArg() ==  m_fStarArg)
				&& (popFunc->FSimpleAgg() ==  m_fSimpleAgg)
				&& (popFunc->FAgg() == m_fAgg)
				&& m_pmdidFunc->FEquals(popFunc->PmdidFunc())
				&& m_pmdidRetType->FEquals(popFunc->PmdidType())
				&& (popFunc->Ews() == m_ewinstage));
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarWindowFunc::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CScalarWindowFunc::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " (";
	os << PstrFunc()->Wsz();
	os << " , Agg: " << (m_fAgg ? "true" : "false");
	os << " , Distinct: " << (m_fDistinct ? "true" : "false");
	os << " , StarArgument: " << (m_fStarArg ? "true" : "false");
	os << " , SimpleAgg: " << (m_fSimpleAgg ? "true" : "false");
	os << ")";

	return os;
}

// EOF
