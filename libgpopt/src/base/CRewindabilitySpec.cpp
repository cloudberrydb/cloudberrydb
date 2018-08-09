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
#include "gpopt/operators/CExpressionHandle.h"

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
	ERewindabilityType rewindability_type,
	EMotionHazardType motion_hazard
	)
	:
	m_rewindability(rewindability_type),
	m_motion_hazard(motion_hazard)
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

	return Ert() == prs->Ert() && Emht() == prs->Emht();
}


//	Check if this rewindability spec satisfies the given one
//	based on following satisfiability rules:
//
//	prs  = requested rewindability
//	this = derived rewindability
//
//	R -> Rewindable, R' -> Not Rewindable
//	M -> Motion Hazard, M' -> No Motion Hazard
//	+----------------------+----+-----+-----+------+
//	| Requested/Derived -> | RM | RM' | R'M | R'M' |
//	|   |                  |    |     |     |      |
//	|   V                  |    |     |     |      |
//	+----------------------+----+-----+-----+------+
//	| RM                   | F  | T   | F   | F    |
//	| RM'                  | T  | T   | F   | F    |
//	| R'M                  | T  | T   | T   | T    |
//	| R'M'                 | T  | T   | T   | T    |
//	+----------------------+----+-----+-----+------+

BOOL
CRewindabilitySpec::FSatisfies
	(
	const CRewindabilitySpec *prs
	)
	const
{
	// Non-rewindable requests always satisfied
	if (!prs->IsRewindable())
	{
		return true;
	}
	if (!IsRewindable())
	{
		// Rewindable requests not satisfied by a non-rewindable operator
		return false;
	}
	if (prs->HasMotionHazard() && HasMotionHazard())
	{
		// Rewindablity requested along with motion hazard handling can not be satisfied
		// by a rewindable operator with a motion hazard.
		return false;
	}

	// Rewindability requested without motion hazard handling can be satisfied
	// by a rewindable operator with a motion hazard.
	//
	// Rewindability requested with motion hazard handling can be satisfied
	// by a rewindable operator without a motion hazard.
	//
	// Rewindability request with no motion hazard handling,
	// is satisfied by a rewindable operator that does not derive a motion hazard.
	return true;
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
	return gpos::CombineHashes(gpos::HashValue<ERewindabilityType>(&m_rewindability),
							   gpos::HashValue<EMotionHazardType>(&m_motion_hazard));
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
	CExpressionHandle &exprhdl,
	CReqdPropPlan *prpp,
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

	CRewindabilitySpec *prs = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs();

	BOOL eager = false;
	if(!GPOS_FTRACE(EopttraceMotionHazardHandling) ||
	  (prpp->Per()->PrsRequired()->HasMotionHazard() && prs->HasMotionHazard()))
	{
		// If motion hazard handling is disabled then we always want a blocking spool.
		// otherwise, create a blocking spool *only if* the request alerts about motion
		// hazard and the group expression imposes a motion hazard as well, to prevent deadlock
		eager = true;
	}

	pexpr->AddRef();
	CExpression *pexprSpool = GPOS_NEW(mp) CExpression
									(
									mp,
									GPOS_NEW(mp) CPhysicalSpool(mp, eager),
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
		case ErtRewindable:
			os << "REWINDABLE";
			break;

		case ErtNotRewindable:
			os << "NON-REWINDABLE";
			break;

		default:
			GPOS_ASSERT(!"Unrecognized rewindability type");
			return os;
	}

	switch(Emht())
	{
		case EmhtMotion:
			os << " MOTION";
			break;

		case EmhtNoMotion:
			os << " NO-MOTION";
			break;

		default:
			GPOS_ASSERT(!"Unrecognized motion hazard type");
			break;
	}

	return os;
}


// EOF

