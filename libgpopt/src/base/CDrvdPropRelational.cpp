//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2011 EMC CORP.
//
//	@filename:
//		CDrvdPropRelational.cpp
//
//	@doc:
//		Relational derived properties;
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/task/CAutoSuspendAbort.h"
#include "gpos/task/CWorker.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CLogical.h"
#include "gpopt/operators/CLogicalDynamicGet.h"
#include "gpopt/base/CDrvdPropRelational.h"
#include "gpopt/base/CReqdPropPlan.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/CPartInfo.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::CDrvdPropRelational
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CDrvdPropRelational::CDrvdPropRelational
	()
	:
	m_pcrsOutput(NULL),
	m_pcrsOuter(NULL),
	m_pcrsNotNull(NULL),
	m_pcrsCorrelatedApply(NULL),
	m_pkc(NULL),
	m_pdrgpfd(NULL),
	m_ulJoinDepth(0),
	m_ppartinfo(NULL),
	m_ppc(NULL),
	m_pfp(NULL),
	m_fHasPartialIndexes(false)
{}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::~CDrvdPropRelational
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CDrvdPropRelational::~CDrvdPropRelational()
{
	{
		CAutoSuspendAbort asa;

		CRefCount::SafeRelease(m_pcrsOutput);
		CRefCount::SafeRelease(m_pcrsOuter);
		CRefCount::SafeRelease(m_pcrsNotNull);
		CRefCount::SafeRelease(m_pcrsCorrelatedApply);
		CRefCount::SafeRelease(m_pkc);
		CRefCount::SafeRelease(m_pdrgpfd);
		CRefCount::SafeRelease(m_ppartinfo);
		CRefCount::SafeRelease(m_ppc);
		CRefCount::SafeRelease(m_pfp);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::Derive
//
//	@doc:
//		Derive relational props
//
//---------------------------------------------------------------------------
void
CDrvdPropRelational::Derive
	(
	CMemoryPool *mp,
	CExpressionHandle &exprhdl,
	CDrvdPropCtxt * // pdpctxt
	)
{
	GPOS_CHECK_ABORT;

	CLogical *popLogical = CLogical::PopConvert(exprhdl.Pop());

	// call output derivation function on the operator
	m_pcrsOutput = popLogical->PcrsDeriveOutput(mp, exprhdl);

	// derive outer-references
	m_pcrsOuter = popLogical->PcrsDeriveOuter(mp, exprhdl);
	
	// derive not null columns
	m_pcrsNotNull = popLogical->PcrsDeriveNotNull(mp, exprhdl);

	// derive correlated apply columns
	m_pcrsCorrelatedApply = popLogical->PcrsDeriveCorrelatedApply(mp, exprhdl);

	// derive keys
	m_pkc = popLogical->PkcDeriveKeys(mp, exprhdl);
	
	// derive constraint
	m_ppc = popLogical->PpcDeriveConstraint(mp, exprhdl);

	// compute max card
	m_maxcard = popLogical->Maxcard(mp, exprhdl);
	
	// derive join depth
	m_ulJoinDepth = popLogical->JoinDepth(mp, exprhdl);

	// derive function properties
	m_pfp = popLogical->PfpDerive(mp, exprhdl);

	// no key but only one row implies a key
	if (!FHasKey() && 1 == m_maxcard)
	{
		GPOS_ASSERT(NULL == m_pkc);
		
		if (0 < m_pcrsOutput->Size())
		{
			m_pcrsOutput->AddRef();
			m_pkc = GPOS_NEW(mp) CKeyCollection(mp, m_pcrsOutput);
		}
	}

	// derive functional dependencies
	m_pdrgpfd = Pdrgpfd(mp, exprhdl);
	
	// derive partition consumers
	m_ppartinfo = popLogical->PpartinfoDerive(mp, exprhdl);
	GPOS_ASSERT(NULL != m_ppartinfo);

	COperator::EOperatorId op_id = popLogical->Eopid();

	// determine if it is a dynamic get (with or without a select above it) with partial indexes
	if (COperator::EopLogicalDynamicGet == op_id)
	{
		m_fHasPartialIndexes =
				CLogicalDynamicGet::PopConvert(popLogical)->Ptabdesc()->FHasPartialIndexes();
	}
	else if (COperator::EopLogicalSelect == op_id)
	{
		m_fHasPartialIndexes =
				exprhdl.GetRelationalProperties(0 /*child_index*/)->FHasPartialIndexes();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::FSatisfies
//
//	@doc:
//		Check for satisfying required properties
//
//---------------------------------------------------------------------------
BOOL
CDrvdPropRelational::FSatisfies
	(
	const CReqdPropPlan *prpp
	)
	const
{
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT(NULL != prpp->PcrsRequired());

	BOOL fSatisfies = m_pcrsOutput->ContainsAll(prpp->PcrsRequired());

	return fSatisfies;
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::GetRelationalProperties
//
//	@doc:
//		Short hand for conversion
//
//---------------------------------------------------------------------------
CDrvdPropRelational *
CDrvdPropRelational::GetRelationalProperties
	(
	DrvdPropArray *pdp
	)
{
	GPOS_ASSERT(NULL != pdp);
	GPOS_ASSERT(EptRelational == pdp->Ept() && "This is not a relational properties container");

	return dynamic_cast<CDrvdPropRelational*>(pdp);
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::PdrgpfdChild
//
//	@doc:
//		Helper for getting applicable FDs from child
//
//---------------------------------------------------------------------------
CFunctionalDependencyArray *
CDrvdPropRelational::PdrgpfdChild
	(
	CMemoryPool *mp,
	ULONG child_index,
	CExpressionHandle &exprhdl
	)
{
	GPOS_ASSERT(child_index < exprhdl.Arity());
	GPOS_ASSERT(!exprhdl.FScalarChild(child_index));

	// get FD's of the child
	CFunctionalDependencyArray *pdrgpfdChild = exprhdl.GetRelationalProperties(child_index)->Pdrgpfd();

	// get output columns of the parent
	CColRefSet *pcrsOutput = CDrvdPropRelational::GetRelationalProperties(exprhdl.Pdp())->PcrsOutput();

	// collect child FD's that are applicable to the parent
	CFunctionalDependencyArray *pdrgpfd = GPOS_NEW(mp) CFunctionalDependencyArray(mp);
	const ULONG size = pdrgpfdChild->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CFunctionalDependency *pfd = (*pdrgpfdChild)[ul];

		// check applicability of FD's LHS
		if (pcrsOutput->ContainsAll(pfd->PcrsKey()))
		{
			// decompose FD's RHS to extract the applicable part
			CColRefSet *pcrsDetermined = GPOS_NEW(mp) CColRefSet(mp);
			pcrsDetermined->Include(pfd->PcrsDetermined());
			pcrsDetermined->Intersection(pcrsOutput);
			if (0 < pcrsDetermined->Size())
			{
				// create a new FD and add it to the output array
				pfd->PcrsKey()->AddRef();
				pcrsDetermined->AddRef();
				CFunctionalDependency *pfdNew =
					GPOS_NEW(mp) CFunctionalDependency(pfd->PcrsKey(), pcrsDetermined);
				pdrgpfd->Append(pfdNew);
			}
			pcrsDetermined->Release();
		}
	}

	return pdrgpfd;
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::PdrgpfdLocal
//
//	@doc:
//		Helper for deriving local FDs
//
//---------------------------------------------------------------------------
CFunctionalDependencyArray *
CDrvdPropRelational::PdrgpfdLocal
	(
	CMemoryPool *mp,
	CExpressionHandle &exprhdl
	)
{
	CFunctionalDependencyArray *pdrgpfd = GPOS_NEW(mp) CFunctionalDependencyArray(mp);

	// get local key
	CKeyCollection *pkc = CDrvdPropRelational::GetRelationalProperties(exprhdl.Pdp())->Pkc();
	
	if (NULL == pkc)
	{
		return pdrgpfd;
	}
	
	ULONG ulKeys = pkc->Keys();
	for (ULONG ul = 0; ul < ulKeys; ul++)
	{
		CColRefArray *pdrgpcrKey = pkc->PdrgpcrKey(mp, ul);
		CColRefSet *pcrsKey = GPOS_NEW(mp) CColRefSet(mp);
		pcrsKey->Include(pdrgpcrKey);

		// get output columns
		CColRefSet *pcrsOutput = CDrvdPropRelational::GetRelationalProperties(exprhdl.Pdp())->PcrsOutput();
		CColRefSet *pcrsDetermined = GPOS_NEW(mp) CColRefSet(mp);
		pcrsDetermined->Include(pcrsOutput);
		pcrsDetermined->Exclude(pcrsKey);

		if (0 < pcrsDetermined->Size())
		{
			// add FD between key and the rest of output columns
			pcrsKey->AddRef();
			pcrsDetermined->AddRef();
			CFunctionalDependency *pfdLocal = GPOS_NEW(mp) CFunctionalDependency(pcrsKey, pcrsDetermined);
			pdrgpfd->Append(pfdLocal);
		}

		pcrsKey->Release();
		pcrsDetermined->Release();
		CRefCount::SafeRelease(pdrgpcrKey);
	}

	return pdrgpfd;
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::Pdrgpfd
//
//	@doc:
//		Helper for deriving functional dependencies
//
//---------------------------------------------------------------------------
CFunctionalDependencyArray *
CDrvdPropRelational::Pdrgpfd
	(
	CMemoryPool *mp,
	CExpressionHandle &exprhdl
	)
{
	GPOS_ASSERT(exprhdl.Pop()->FLogical());

	CFunctionalDependencyArray *pdrgpfd = GPOS_NEW(mp) CFunctionalDependencyArray(mp);
	const ULONG arity = exprhdl.Arity();

	// collect applicable FD's from logical children
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (!exprhdl.FScalarChild(ul))
		{
			CFunctionalDependencyArray *pdrgpfdChild = PdrgpfdChild(mp, ul, exprhdl);
			CUtils::AddRefAppend<CFunctionalDependency, CleanupRelease>(pdrgpfd, pdrgpfdChild);
			pdrgpfdChild->Release();
		}
	}
	// add local FD's
	CFunctionalDependencyArray *pdrgpfdLocal = PdrgpfdLocal(mp, exprhdl);
	CUtils::AddRefAppend<CFunctionalDependency, CleanupRelease>(pdrgpfd, pdrgpfdLocal);
	pdrgpfdLocal->Release();

	return pdrgpfd;
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CDrvdPropRelational::OsPrint
	(
	IOstream &os
	)
	const
{
	os	<<	"Output Cols: [" << *m_pcrsOutput << "]"
		<<	", Outer Refs: [" << *m_pcrsOuter << "]"
		<<	", Not Null Cols: [" << *m_pcrsNotNull << "]"
		<< ", Corr. Apply Cols: [" << *m_pcrsCorrelatedApply <<"]";

	if (NULL == m_pkc)
	{
		os << ", Keys: []";
	}
	else
	{
		os << ", " << *m_pkc;
	}
	
	os << ", Max Card: " << m_maxcard;

	os << ", Join Depth: " << m_ulJoinDepth;

	os << ", Constraint Property: [" << *m_ppc << "]";

	const ULONG ulFDs = m_pdrgpfd->Size();
	
	os << ", FDs: [";
	for (ULONG ul = 0; ul < ulFDs; ul++)
	{
		CFunctionalDependency *pfd = (*m_pdrgpfd)[ul];
		os << *pfd;
	}
	os << "]";
	
	os << ", Function Properties: [" << *m_pfp << "]";

	os << ", Part Info: [" << *m_ppartinfo << "]";

	if (m_fHasPartialIndexes)
	{
		os <<", Has Partial Indexes";
	}
	return os;
}


// EOF
