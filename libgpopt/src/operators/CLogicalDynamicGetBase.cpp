//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CLogicalDynamicGetBase.cpp
//
//	@doc:
//		Implementation of dynamic table access base class
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CConstraintInterval.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CPartIndexMap.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/COptCtxt.h"

#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalDynamicGetBase.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/metadata/CName.h"

#include "naucrates/statistics/CStatsPredUtils.h"
#include "naucrates/statistics/CFilterStatsProcessor.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::CLogicalDynamicGetBase
//
//	@doc:
//		ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalDynamicGetBase::CLogicalDynamicGetBase
	(
	IMemoryPool *pmp
	)
	:
	CLogical(pmp),
	m_pnameAlias(NULL),
	m_ptabdesc(NULL),
	m_ulScanId(0),
	m_pdrgpcrOutput(NULL),
	m_pdrgpdrgpcrPart(NULL),
	m_ulSecondaryScanId(0),
	m_fPartial(false),
	m_ppartcnstr(NULL),
	m_ppartcnstrRel(NULL),
	m_pcrsDist(NULL)
{
	m_fPattern = true;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::CLogicalDynamicGetBase
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalDynamicGetBase::CLogicalDynamicGetBase
	(
	IMemoryPool *pmp,
	const CName *pnameAlias,
	CTableDescriptor *ptabdesc,
	ULONG ulScanId,
	DrgPcr *pdrgpcrOutput, 
	DrgDrgPcr *pdrgpdrgpcrPart,
	ULONG ulSecondaryScanId,
	BOOL fPartial,
	CPartConstraint *ppartcnstr,
	CPartConstraint *ppartcnstrRel
	)
	:
	CLogical(pmp),
	m_pnameAlias(pnameAlias),
	m_ptabdesc(ptabdesc),
	m_ulScanId(ulScanId),
	m_pdrgpcrOutput(pdrgpcrOutput),
	m_pdrgpdrgpcrPart(pdrgpdrgpcrPart),
	m_ulSecondaryScanId(ulSecondaryScanId),
	m_fPartial(fPartial),
	m_ppartcnstr(ppartcnstr),
	m_ppartcnstrRel(ppartcnstrRel),
	m_pcrsDist(NULL)
{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pnameAlias);
	GPOS_ASSERT(NULL != pdrgpcrOutput);
	GPOS_ASSERT(NULL != pdrgpdrgpcrPart);
	GPOS_ASSERT(NULL != ppartcnstr);
	GPOS_ASSERT(NULL != ppartcnstrRel);

	GPOS_ASSERT_IMP(ulScanId != ulSecondaryScanId, NULL != ppartcnstr);
	GPOS_ASSERT_IMP(fPartial, NULL != m_ppartcnstr->PcnstrCombined() && "Partial scan with unsupported constraint type");

	m_pcrsDist = CLogical::PcrsDist(pmp, m_ptabdesc, m_pdrgpcrOutput);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::CLogicalDynamicGetBase
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalDynamicGetBase::CLogicalDynamicGetBase
	(
	IMemoryPool *pmp,
	const CName *pnameAlias,
	CTableDescriptor *ptabdesc,
	ULONG ulScanId
	)
	:
	CLogical(pmp),
	m_pnameAlias(pnameAlias),
	m_ptabdesc(ptabdesc),
	m_ulScanId(ulScanId),
	m_pdrgpcrOutput(NULL),
	m_ulSecondaryScanId(ulScanId),
	m_fPartial(false),
	m_ppartcnstr(NULL),
	m_ppartcnstrRel(NULL),
	m_pcrsDist(NULL)
{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pnameAlias);
	
	// generate a default column set for the table descriptor
	m_pdrgpcrOutput = PdrgpcrCreateMapping(pmp, m_ptabdesc->Pdrgpcoldesc(), UlOpId());
	m_pdrgpdrgpcrPart = PdrgpdrgpcrCreatePartCols(pmp, m_pdrgpcrOutput, m_ptabdesc->PdrgpulPart());
	
	// generate a constraint "true"
	HMUlCnstr *phmulcnstr = CUtils::PhmulcnstrBoolConstOnPartKeys(pmp, m_pdrgpdrgpcrPart, true /*fVal*/);
	CBitSet *pbsDefaultParts = CUtils::PbsAllSet(pmp, m_pdrgpdrgpcrPart->UlLength());
	m_pdrgpdrgpcrPart->AddRef();
	m_ppartcnstr = GPOS_NEW(pmp) CPartConstraint(pmp, phmulcnstr, pbsDefaultParts, true /*fUnbounded*/, m_pdrgpdrgpcrPart);
	m_ppartcnstr->AddRef();
	m_ppartcnstrRel = m_ppartcnstr;
        
	m_pcrsDist = CLogical::PcrsDist(pmp, m_ptabdesc, m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::~CLogicalDynamicGetBase
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CLogicalDynamicGetBase::~CLogicalDynamicGetBase()
{
	CRefCount::SafeRelease(m_ptabdesc);
	CRefCount::SafeRelease(m_pdrgpcrOutput);
	CRefCount::SafeRelease(m_pdrgpdrgpcrPart);
	CRefCount::SafeRelease(m_ppartcnstr);
	CRefCount::SafeRelease(m_ppartcnstrRel);
	CRefCount::SafeRelease(m_pcrsDist);

	GPOS_DELETE(m_pnameAlias);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalDynamicGetBase::PcrsDeriveOutput
	(
	IMemoryPool *pmp,
	CExpressionHandle & // exprhdl
	)
{
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrs->Include(m_pdrgpcrOutput);

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalDynamicGetBase::PkcDeriveKeys
	(
	IMemoryPool *pmp,
	CExpressionHandle & // exprhdl
	)
	const
{
	const DrgPbs *pdrgpbs = m_ptabdesc->PdrgpbsKeys();

	return CLogical::PkcKeysBaseTable(pmp, pdrgpbs, m_pdrgpcrOutput);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::PpcDeriveConstraint
//
//	@doc:
//		Derive constraint property
//
//---------------------------------------------------------------------------
CPropConstraint *
CLogicalDynamicGetBase::PpcDeriveConstraint
	(
	IMemoryPool *pmp,
	CExpressionHandle & // exprhdl
	)
	const
{
	return PpcDeriveConstraintFromTable(pmp, m_ptabdesc, m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::PpartinfoDerive
//
//	@doc:
//		Derive partition consumer info
//
//---------------------------------------------------------------------------
CPartInfo *
CLogicalDynamicGetBase::PpartinfoDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle & // exprhdl
	)
	const
{
	IMDId *pmdid = m_ptabdesc->Pmdid();
	pmdid->AddRef();
	m_pdrgpdrgpcrPart->AddRef();
	m_ppartcnstrRel->AddRef(); 
	
	CPartInfo *ppartinfo = GPOS_NEW(pmp) CPartInfo(pmp);
	ppartinfo->AddPartConsumer(pmp, m_ulScanId, pmdid, m_pdrgpdrgpcrPart, m_ppartcnstrRel);
	
	return ppartinfo;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::SetPartConstraint
//
//	@doc:
//		Set part constraint
//
//---------------------------------------------------------------------------
void
CLogicalDynamicGetBase::SetPartConstraint
	(
	CPartConstraint *ppartcnstr
	) 
{
	GPOS_ASSERT(NULL != ppartcnstr);
	GPOS_ASSERT(NULL != m_ppartcnstr);

	m_ppartcnstr->Release();
	m_ppartcnstr = ppartcnstr;
	
	m_ppartcnstrRel->Release();
	ppartcnstr->AddRef();
	m_ppartcnstrRel = ppartcnstr;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::SetSecondaryScanId
//
//	@doc:
//		Set secondary scan id
//
//---------------------------------------------------------------------------
void
CLogicalDynamicGetBase::SetSecondaryScanId
	(
	ULONG ulScanId
	)
{
	m_ulSecondaryScanId = ulScanId;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::SetPartial
//
//	@doc:
//		Set partial to true
//
//---------------------------------------------------------------------------
void
CLogicalDynamicGetBase::SetPartial()
{
	GPOS_ASSERT(!FPartial());
	GPOS_ASSERT(NULL != m_ppartcnstr->PcnstrCombined() && "Partial scan with unsupported constraint type");
	m_fPartial = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::PstatsDeriveFilter
//
//	@doc:
//		Derive stats from base table using filters on partition and/or index columns
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalDynamicGetBase::PstatsDeriveFilter
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CExpression *pexprFilter
	)
	const
{
	CExpression *pexprFilterNew = NULL;
	CConstraint *pcnstr = m_ppartcnstr->PcnstrCombined();
	if (m_fPartial && NULL != pcnstr && !pcnstr->FUnbounded())
	{
		if (NULL == pexprFilter)
		{
			pexprFilterNew = pcnstr->PexprScalar(pmp);
			pexprFilterNew->AddRef();
		}
		else
		{
			pexprFilterNew = CPredicateUtils::PexprConjunction(pmp, pexprFilter, pcnstr->PexprScalar(pmp));
		}
	}
	else if (NULL != pexprFilter)
	{
		pexprFilterNew = pexprFilter;
		pexprFilterNew->AddRef();
	}

	CColRefSet *pcrsStat = GPOS_NEW(pmp) CColRefSet(pmp);
	CDrvdPropScalar *pdpscalar = NULL;

	if (NULL != pexprFilterNew)
	{
		pdpscalar = CDrvdPropScalar::Pdpscalar(pexprFilterNew->PdpDerive());
		pcrsStat->Include(pdpscalar->PcrsUsed());
	}

	// requesting statistics on distribution columns to estimate data skew
	if (NULL != m_pcrsDist)
	{
		pcrsStat->Include(m_pcrsDist);
	}


	CStatistics *pstatsFullTable = dynamic_cast<CStatistics *>(PstatsBaseTable(pmp, exprhdl, m_ptabdesc, pcrsStat));
	
	pcrsStat->Release();
	
	if (NULL == pexprFilterNew || pdpscalar->FHasSubquery())
	{
		return pstatsFullTable;
	}

	CStatsPred *pstatspred =  CStatsPredUtils::PstatspredExtract
												(
												pmp, 
												pexprFilterNew, 
												NULL /*pcrsOuterRefs*/
												);
	pexprFilterNew->Release();

	IStatistics *pstatsResult = CFilterStatsProcessor::PstatsFilter(pmp, pstatsFullTable, pstatspred, true /* fCapNdvs */);
	pstatspred->Release();
	pstatsFullTable->Release();

	return pstatsResult;
}

// EOF

