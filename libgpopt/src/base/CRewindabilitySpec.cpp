//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CRewindabilitySpec.cpp
//
//	@doc:
//		Specification of rewindability of intermediate query results
//---------------------------------------------------------------------------

#include "gpopt/base/CRewindabilitySpec.h"
#include "gpopt/operators/CPhysicalSpool.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CRewindabilitySpec::CRewindabilitySpec
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CRewindabilitySpec::CRewindabilitySpec
	(
	ERewindabilityType ert
	)
	:
	m_ert(ert)
{}


//---------------------------------------------------------------------------
//	@function:
//		CRewindabilitySpec::~CRewindabilitySpec
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CRewindabilitySpec::~CRewindabilitySpec()
{}


//---------------------------------------------------------------------------
//	@function:
//		CRewindabilitySpec::Matches
//
//	@doc:
//		Check for equality between rewindability specs
//
//---------------------------------------------------------------------------
BOOL
CRewindabilitySpec::Matches
	(
	const CRewindabilitySpec *prs
	)
	const
{
	GPOS_ASSERT(NULL != prs);

	return Ert() == prs->Ert();
}


//---------------------------------------------------------------------------
//	@function:
//		CRewindabilitySpec::FSatisfies
//
//	@doc:
//		Check if this rewindability spec satisfies the given one
//
//---------------------------------------------------------------------------
BOOL
CRewindabilitySpec::FSatisfies
	(
	const CRewindabilitySpec *prs
	)
	const
{
	return
		Matches(prs) ||
		ErtNone == prs->Ert() ||
		(ErtMarkRestore == Ert() && ErtGeneral == prs->Ert());
}


//---------------------------------------------------------------------------
//	@function:
//		CRewindabilitySpec::HashValue
//
//	@doc:
//		Hash of components
//
//---------------------------------------------------------------------------
ULONG
CRewindabilitySpec::HashValue() const
{
	return gpos::HashValue<ERewindabilityType>(&m_ert);
}


//---------------------------------------------------------------------------
//	@function:
//		CRewindabilitySpec::AppendEnforcers
//
//	@doc:
//		Add required enforcers to dynamic array
//
//---------------------------------------------------------------------------
void
CRewindabilitySpec::AppendEnforcers
	(
	IMemoryPool *mp,
	CExpressionHandle &, // exprhdl
	CReqdPropPlan *
#ifdef GPOS_DEBUG
	prpp
#endif // GPOS_DEBUG
	,
	CExpressionArray *pdrgpexpr, 
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(this == prpp->Per()->PrsRequired() &&
				"required plan properties don't match enforced rewindability spec");

	pexpr->AddRef();
	CExpression *pexprSpool = GPOS_NEW(mp) CExpression
									(
									mp, 
									GPOS_NEW(mp) CPhysicalSpool(mp),
									pexpr
									);
	pdrgpexpr->Append(pexprSpool);
}


//---------------------------------------------------------------------------
//	@function:
//		CRewindabilitySpec::OsPrint
//
//	@doc:
//		Print rewindability spec
//
//---------------------------------------------------------------------------
IOstream &
CRewindabilitySpec::OsPrint
	(
	IOstream &os
	)
	const
{
	switch (Ert())
	{
		case ErtGeneral:
			return os << "REWINDABLE";

		case ErtMarkRestore:
			return os << "MARK-RESTORE";

		case ErtNone:
			return os << "NON-REWINDABLE";

		default:
			GPOS_ASSERT(!"Unrecognized rewindability type");
			return os;
	}
}


// EOF

