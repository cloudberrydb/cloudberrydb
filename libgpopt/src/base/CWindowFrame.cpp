//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CWindowFrame.cpp
//
//	@doc:
//		Implementation of window frame
//---------------------------------------------------------------------------

#include "gpopt/base/CWindowFrame.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CScalarIdent.h"


using namespace gpopt;

// string encoding of frame specification
const CHAR rgszFrameSpec[][10] = {"Rows", "Range"};
GPOS_CPL_ASSERT(CWindowFrame::EfsSentinel == GPOS_ARRAY_SIZE(rgszFrameSpec));

// string encoding of frame boundary
const CHAR rgszFrameBoundary[][40] =
	{
	"Unbounded Preceding",
	"Bounded Preceding",
	"Current",
	"Unbounded Following",
	"Bounded Following",
	"Delayed Bounded Preceding",
	"Delayed Bounded Following"
	};
GPOS_CPL_ASSERT(CWindowFrame::EfbSentinel == GPOS_ARRAY_SIZE(rgszFrameBoundary));

// string encoding of frame exclusion strategy
const CHAR rgszFrameExclusionStrategy[][20] = {"None", "Nulls", "Current", "MatchingOthers", "Ties"};
GPOS_CPL_ASSERT(CWindowFrame::EfesSentinel == GPOS_ARRAY_SIZE(rgszFrameExclusionStrategy));

// empty window frame
const CWindowFrame CWindowFrame::m_wfEmpty;

//---------------------------------------------------------------------------
//	@function:
//		CWindowFrame::CWindowFrame
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CWindowFrame::CWindowFrame
	(
	IMemoryPool *pmp,
	EFrameSpec efs,
	EFrameBoundary efbLeading,
	EFrameBoundary efbTrailing,
	CExpression *pexprLeading,
	CExpression *pexprTrailing,
	EFrameExclusionStrategy efes
	)
	:
	m_efs(efs),
	m_efbLeading(efbLeading),
	m_efbTrailing(efbTrailing),
	m_pexprLeading(pexprLeading),
	m_pexprTrailing(pexprTrailing),
	m_efes(efes),
	m_pcrsUsed(NULL)
{
	GPOS_ASSERT_IMP(EfbBoundedPreceding == m_efbLeading || EfbBoundedFollowing == m_efbLeading, NULL != pexprLeading);
	GPOS_ASSERT_IMP(EfbBoundedPreceding == m_efbTrailing || EfbBoundedFollowing == m_efbTrailing, NULL != pexprTrailing);

	// include used columns by frame edges
	m_pcrsUsed = GPOS_NEW(pmp) CColRefSet(pmp);
	if (NULL != pexprLeading)
	{
		m_pcrsUsed->Include(CDrvdPropScalar::Pdpscalar(pexprLeading->PdpDerive())->PcrsUsed());
	}

	if (NULL != pexprTrailing)
	{
		m_pcrsUsed->Include(CDrvdPropScalar::Pdpscalar(pexprTrailing->PdpDerive())->PcrsUsed());
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CWindowFrame::CWindowFrame
//
//	@doc:
//		Private dummy ctor used for creating empty frame
//
//---------------------------------------------------------------------------
CWindowFrame::CWindowFrame()
	:
	m_efs(EfsRange),
	m_efbLeading(EfbUnboundedPreceding),
	m_efbTrailing(EfbCurrentRow),
	m_pexprLeading(NULL),
	m_pexprTrailing(NULL),
	m_efes(EfesNone),
	m_pcrsUsed(NULL)
{}


//---------------------------------------------------------------------------
//	@function:
//		CWindowFrame::~CWindowFrame
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CWindowFrame::~CWindowFrame()
{
	CRefCount::SafeRelease(m_pexprLeading);
	CRefCount::SafeRelease(m_pexprTrailing);
	CRefCount::SafeRelease(m_pcrsUsed);
}


//---------------------------------------------------------------------------
//	@function:
//		CWindowFrame::FMatch
//
//	@doc:
//		Check for equality between window frames
//
//---------------------------------------------------------------------------
BOOL
CWindowFrame::FMatch
	(
	const CWindowFrame *pwf
	)
	const
{
	return
		m_efs == pwf->Efs() &&
		m_efbLeading == pwf->EfbLeading() &&
		m_efbTrailing == pwf->EfbTrailing() &&
		m_efes == pwf->Efes() &&
		CUtils::FEqual(m_pexprLeading , pwf->PexprLeading()) &&
		CUtils::FEqual(m_pexprTrailing , pwf->PexprTrailing());
}


//---------------------------------------------------------------------------
//	@function:
//		CWindowFrame::UlHash
//
//	@doc:
//		Hash of components
//
//---------------------------------------------------------------------------
ULONG
CWindowFrame::UlHash() const
{
	ULONG ulHash = 0;
	ulHash = gpos::UlCombineHashes(ulHash, m_efs);
	ulHash = gpos::UlCombineHashes(ulHash, m_efbLeading);
	ulHash = gpos::UlCombineHashes(ulHash, m_efbTrailing);
	ulHash = gpos::UlCombineHashes(ulHash, m_efes);
	if (NULL != m_pexprLeading)
	{
		ulHash = gpos::UlCombineHashes(ulHash, CExpression::UlHash(m_pexprLeading));
	}

	if (NULL != m_pexprTrailing)
	{
		ulHash = gpos::UlCombineHashes(ulHash, CExpression::UlHash(m_pexprTrailing));
	}

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CWindowFrame::PwfCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the window frame with remapped columns
//
//---------------------------------------------------------------------------
CWindowFrame *
CWindowFrame::PwfCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	if (this == &m_wfEmpty)
	{
		this->AddRef();
		return this;
	}

	CExpression *pexprLeading = NULL;
	if (NULL != m_pexprLeading)
	{
		pexprLeading = m_pexprLeading->PexprCopyWithRemappedColumns(pmp, phmulcr, fMustExist);
	}

	CExpression *pexprTrailing = NULL;
	if (NULL != m_pexprTrailing)
	{
		pexprTrailing = m_pexprTrailing->PexprCopyWithRemappedColumns(pmp, phmulcr, fMustExist);
	}

	return GPOS_NEW(pmp) CWindowFrame(pmp, m_efs, m_efbLeading, m_efbTrailing, pexprLeading, pexprTrailing, m_efes);
}

//---------------------------------------------------------------------------
//	@function:
//		CWindowFrame::OsPrint
//
//	@doc:
//		Print window frame
//
//---------------------------------------------------------------------------
IOstream &
CWindowFrame::OsPrint
	(
	IOstream &os
	)
	const
{
	if (this == &m_wfEmpty)
	{
		os << "EMPTY FRAME";
		return os;
	}

	os << "[" << rgszFrameSpec[m_efs] << ", ";

	os << "Trail: " << rgszFrameBoundary[m_efbTrailing];
	if (NULL != m_pexprTrailing)
	{
		os << " " << *m_pexprTrailing;
	}

	os << ", Lead: " << rgszFrameBoundary[m_efbLeading];
	if (NULL != m_pexprLeading)
	{
		os << " " << *m_pexprLeading;
	}

	os << ", " << rgszFrameExclusionStrategy[m_efes];

	os << "]";

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CWindowFrame::FEqual
//
//	@doc:
//		 Matching function over frame arrays
//
//---------------------------------------------------------------------------
BOOL
CWindowFrame::FEqual
	(
	const DrgPwf *pdrgpwfFirst,
	const DrgPwf *pdrgpwfSecond
	)
{
	if (NULL == pdrgpwfFirst || NULL == pdrgpwfSecond)
	{
		return (NULL == pdrgpwfFirst && NULL ==pdrgpwfSecond);
	}

	if (pdrgpwfFirst->UlLength() != pdrgpwfSecond->UlLength())
	{
		return false;
	}

	const ULONG ulSize = pdrgpwfFirst->UlLength();
	BOOL fMatch = true;
	for (ULONG ul = 0; fMatch && ul < ulSize; ul++)
	{
		fMatch = (*pdrgpwfFirst)[ul]->FMatch((*pdrgpwfSecond)[ul]);
	}

	return fMatch;
}


//---------------------------------------------------------------------------
//	@function:
//		CWindowFrame::UlHash
//
//	@doc:
//		 Combine hash values of a maximum number of entries
//
//---------------------------------------------------------------------------
ULONG
CWindowFrame::UlHash
	(
	const DrgPwf *pdrgpwf,
	ULONG ulMaxSize
	)
{
	GPOS_ASSERT(NULL != pdrgpwf);
	const ULONG ulSize = std::min(ulMaxSize, pdrgpwf->UlLength());

	ULONG ulHash = 0;
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		ulHash = gpos::UlCombineHashes(ulHash, (*pdrgpwf)[ul]->UlHash());
	}

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CWindowFrame::OsPrint
//
//	@doc:
//		 Print array of window frame objects
//
//---------------------------------------------------------------------------
IOstream &
CWindowFrame::OsPrint
	(
	IOstream &os,
	const DrgPwf *pdrgpwf
	)
{
	os	<< "[";
	const ULONG ulSize = pdrgpwf->UlLength();
	if (0 < ulSize)
	{
		for (ULONG ul = 0; ul < ulSize - 1; ul++)
		{
			(void) (*pdrgpwf)[ul]->OsPrint(os);
			os <<	", ";
		}

		(void) (*pdrgpwf)[ulSize - 1]->OsPrint(os);
	}

	return os << "]";
}


// EOF

