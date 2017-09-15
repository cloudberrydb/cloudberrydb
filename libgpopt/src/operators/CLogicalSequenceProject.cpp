//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalSequenceProject.cpp
//
//	@doc:
//		Implementation of sequence project operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpec.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CKeyCollection.h"

#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalSequenceProject.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::CLogicalSequenceProject
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalSequenceProject::CLogicalSequenceProject
	(
	IMemoryPool *pmp,
	CDistributionSpec *pds,
	DrgPos *pdrgpos,
	DrgPwf *pdrgpwf
	)
	:
	CLogicalUnary(pmp),
	m_pds(pds),
	m_pdrgpos(pdrgpos),
	m_pdrgpwf(pdrgpwf),
	m_fHasOrderSpecs(false),
	m_fHasFrameSpecs(false)
{
	GPOS_ASSERT(NULL != pds);
	GPOS_ASSERT(NULL != pdrgpos);
	GPOS_ASSERT(NULL != pdrgpwf);
	GPOS_ASSERT(CDistributionSpec::EdtHashed == pds->Edt() ||
			CDistributionSpec::EdtSingleton == pds->Edt());

	// set flags indicating that current operator has non-empty order specs/frame specs
	SetHasOrderSpecs(pmp);
	SetHasFrameSpecs(pmp);

	// include columns used by Partition By, Order By, and window frame edges
	CColRefSet *pcrsSort = COrderSpec::Pcrs(pmp, m_pdrgpos);
	m_pcrsLocalUsed->Include(pcrsSort);
	pcrsSort->Release();

	if (CDistributionSpec::EdtHashed == m_pds->Edt())
	{
		CColRefSet *pcrsHashed = CDistributionSpecHashed::PdsConvert(m_pds)->PcrsUsed(pmp);
		m_pcrsLocalUsed->Include(pcrsHashed);
		pcrsHashed->Release();
	}

	const ULONG ulFrames = m_pdrgpwf->UlLength();
	for (ULONG ul = 0; ul < ulFrames; ul++)
	{
		CWindowFrame *pwf = (*m_pdrgpwf)[ul];
		if (!CWindowFrame::FEmpty(pwf))
		{
			m_pcrsLocalUsed->Include(pwf->PcrsUsed());
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::CLogicalSequenceProject
//
//	@doc:
//		Ctor for patterns
//
//---------------------------------------------------------------------------
CLogicalSequenceProject::CLogicalSequenceProject
	(
	IMemoryPool *pmp
	)
	:
	CLogicalUnary(pmp),
	m_pds(NULL),
	m_pdrgpos(NULL),
	m_pdrgpwf(NULL),
	m_fHasOrderSpecs(false),
	m_fHasFrameSpecs(false)
{
	m_fPattern = true;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::~CLogicalSequenceProject
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalSequenceProject::~CLogicalSequenceProject()
{
	CRefCount::SafeRelease(m_pds);
	CRefCount::SafeRelease(m_pdrgpos);
	CRefCount::SafeRelease(m_pdrgpwf);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalSequenceProject::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	CDistributionSpec *pds = m_pds->PdsCopyWithRemappedColumns(pmp, phmulcr, fMustExist);

	DrgPos *pdrgpos = GPOS_NEW(pmp) DrgPos(pmp);
	const ULONG ulOrderSpec = m_pdrgpos->UlLength();
	for (ULONG ul = 0; ul < ulOrderSpec; ul++)
	{
		COrderSpec *pos = ((*m_pdrgpos)[ul])->PosCopyWithRemappedColumns(pmp, phmulcr, fMustExist);
		pdrgpos->Append(pos);
	}

	DrgPwf *pdrgpwf = GPOS_NEW(pmp) DrgPwf(pmp);
	const ULONG ulWindowFrames = m_pdrgpwf->UlLength();
	for (ULONG ul = 0; ul < ulWindowFrames; ul++)
	{
		CWindowFrame *pwf = ((*m_pdrgpwf)[ul])->PwfCopyWithRemappedColumns(pmp, phmulcr, fMustExist);
		pdrgpwf->Append(pwf);
	}

	return GPOS_NEW(pmp) CLogicalSequenceProject(pmp, pds, pdrgpos, pdrgpwf);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::SetHasOrderSpecs
//
//	@doc:
//		Set the flag indicating that SeqPrj has specified order specs
//
//---------------------------------------------------------------------------
void
CLogicalSequenceProject::SetHasOrderSpecs
	(
	IMemoryPool *pmp
	)
{
	GPOS_ASSERT(NULL != m_pdrgpos);

	const ULONG ulOrderSpecs = m_pdrgpos->UlLength();
	if (0 == ulOrderSpecs)
	{
		// if no order specs are given, we add one empty order spec
		m_pdrgpos->Append(GPOS_NEW(pmp) COrderSpec(pmp));
	}
	BOOL fHasOrderSpecs = false;
	for (ULONG ul = 0; !fHasOrderSpecs && ul < ulOrderSpecs; ul++)
	{
		fHasOrderSpecs = !(*m_pdrgpos)[ul]->FEmpty();
	}
	m_fHasOrderSpecs = fHasOrderSpecs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::SetHasFrameSpecs
//
//	@doc:
//		Set the flag indicating that SeqPrj has specified frame specs
//
//---------------------------------------------------------------------------
void
CLogicalSequenceProject::SetHasFrameSpecs
	(
	IMemoryPool * // pmp
	)
{
	GPOS_ASSERT(NULL != m_pdrgpwf);

	const ULONG ulFrameSpecs = m_pdrgpwf->UlLength();
	if (0 == ulFrameSpecs)
	{
		// if no frame specs are given, we add one empty frame
		CWindowFrame *pwf = const_cast<CWindowFrame *>(CWindowFrame::PwfEmpty());
		pwf->AddRef();
		m_pdrgpwf->Append(pwf);
	}
	BOOL fHasFrameSpecs = false;
	for (ULONG ul = 0; !fHasFrameSpecs && ul < ulFrameSpecs; ul++)
	{
		fHasFrameSpecs = !CWindowFrame::FEmpty((*m_pdrgpwf)[ul]);
	}
	m_fHasFrameSpecs = fHasFrameSpecs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalSequenceProject::PcrsDeriveOutput
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
{
	GPOS_ASSERT(2 == exprhdl.UlArity());

	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);

	pcrs->Union(exprhdl.Pdprel(0)->PcrsOutput());

	// the scalar child defines additional columns
	pcrs->Union(exprhdl.Pdpscalar(1)->PcrsDefined());

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::PcrsDeriveOuter
//
//	@doc:
//		Derive outer references
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalSequenceProject::PcrsDeriveOuter
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
{
	CColRefSet *pcrsOuter = CLogical::PcrsDeriveOuter(pmp, exprhdl, m_pcrsLocalUsed);

	return pcrsOuter;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::FHasLocalOuterRefs
//
//	@doc:
//		Return true if outer references are included in Partition/Order,
//		or window frame edges
//
//---------------------------------------------------------------------------
BOOL
CLogicalSequenceProject::FHasLocalOuterRefs
	(
	CExpressionHandle &exprhdl
	)
	const
{
	GPOS_ASSERT(this == exprhdl.Pop());

	CColRefSet *pcrsOuter = CDrvdPropRelational::Pdprel(exprhdl.Pdp())->PcrsOuter();

	return !(pcrsOuter->FDisjoint(m_pcrsLocalUsed));
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalSequenceProject::PkcDeriveKeys
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	return PkcDeriveKeysPassThru(exprhdl, 0 /* ulChild */);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalSequenceProject::Maxcard
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	// pass on max card of first child
	return exprhdl.Pdprel(0)->Maxcard();
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::FMatch
//
//	@doc:
//		Matching function
//
//---------------------------------------------------------------------------
BOOL
CLogicalSequenceProject::FMatch
	(
	COperator *pop
	)
	const
{
	GPOS_ASSERT(NULL != pop);
	if (Eopid() == pop->Eopid())
	{
		CLogicalSequenceProject *popLogicalSequenceProject = CLogicalSequenceProject::PopConvert(pop);
		return
			m_pds->FMatch(popLogicalSequenceProject->Pds()) &&
			CWindowFrame::FEqual(m_pdrgpwf, popLogicalSequenceProject->Pdrgpwf()) &&
			COrderSpec::FEqual(m_pdrgpos, popLogicalSequenceProject->Pdrgpos());
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::UlHash
//
//	@doc:
//		Hashing function
//
//---------------------------------------------------------------------------
ULONG
CLogicalSequenceProject::UlHash() const
{
	ULONG ulHash = 0;
	ulHash = gpos::UlCombineHashes(ulHash, m_pds->UlHash());
	ulHash = gpos::UlCombineHashes(ulHash, CWindowFrame::UlHash(m_pdrgpwf, 3 /*ulMaxSize*/));
	ulHash = gpos::UlCombineHashes(ulHash, COrderSpec::UlHash(m_pdrgpos, 3 /*ulMaxSize*/));

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalSequenceProject::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfSequenceProject2Apply);
	(void) pxfs->FExchangeSet(CXform::ExfImplementSequenceProject);

	return pxfs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::PstatsDerive
//
//	@doc:
//		Derive statistics based on filter predicates
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalSequenceProject::PstatsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	DrgPstat * // pdrgpstatCtxt
	)
	const
{
	return PstatsDeriveProject(pmp, exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalSequenceProject::OsPrint
	(
	IOstream &os
	)
	const
{
	os	<< SzId() << " (";
	os << "Partition By Keys:";
	(void) m_pds->OsPrint(os);
	os	<< ", ";
	os << "Order Spec:";
	(void) COrderSpec::OsPrint(os, m_pdrgpos);
	os	<< ", ";
	os << "WindowFrame Spec:";
	(void) CWindowFrame::OsPrint(os, m_pdrgpwf);

	return os << ")";
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequenceProject::PopRemoveLocalOuterRefs
//
//	@doc:
//		Filter out outer references from Order By/ Partition By
//		clauses, and return a new operator
//
//---------------------------------------------------------------------------
CLogicalSequenceProject *
CLogicalSequenceProject::PopRemoveLocalOuterRefs
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
{
	GPOS_ASSERT(this == exprhdl.Pop());

	CColRefSet *pcrsOuter = exprhdl.Pdprel()->PcrsOuter();
	CDistributionSpec *pds = m_pds;
	if (CDistributionSpec::EdtHashed == m_pds->Edt())
	{
		pds = CDistributionSpecHashed::PdsConvert(m_pds)->PdshashedExcludeColumns(pmp, pcrsOuter);
		if (NULL == pds)
		{
			// eliminate Partition clause
			pds = GPOS_NEW(pmp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
		}
	}
	else
	{
		pds->AddRef();
	}

	DrgPos *pdrgpos = COrderSpec::PdrgposExclude(pmp, m_pdrgpos, pcrsOuter);

	// for window frame edges, outer references cannot be removed since this can change
	// the semantics of frame edge from delayed-bounding to unbounded,
	// we re-use the frame edges without changing here
	m_pdrgpwf->AddRef();

	return GPOS_NEW(pmp) CLogicalSequenceProject(pmp, pds, pdrgpos, m_pdrgpwf);
}

// EOF

