//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp
//
//	@filename:
//		CLogicalTVF.cpp
//
//	@doc:
//		Implementation of table functions
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CUtils.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalTVF.h"
#include "gpopt/metadata/CName.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/COptCtxt.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::CLogicalTVF
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalTVF::CLogicalTVF
	(
	IMemoryPool *pmp
	)
	:
	CLogical(pmp),
	m_pmdidFunc(NULL),
	m_pmdidRetType(NULL),
	m_pstr(NULL),
	m_pdrgpcoldesc(NULL),
	m_pdrgpcrOutput(NULL),
	m_efs(IMDFunction::EfsImmutable),
	m_efda(IMDFunction::EfdaNoSQL),
	m_fReturnsSet(true)
{
	m_fPattern = true;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::CLogicalTVF
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalTVF::CLogicalTVF
	(
	IMemoryPool *pmp,
	IMDId *pmdidFunc,
	IMDId *pmdidRetType,
	CWStringConst *pstr,
	DrgPcoldesc *pdrgpcoldesc
	)
	:
	CLogical(pmp),
	m_pmdidFunc(pmdidFunc),
	m_pmdidRetType(pmdidRetType),
	m_pstr(pstr),
	m_pdrgpcoldesc(pdrgpcoldesc),
	m_pdrgpcrOutput(NULL)
{
	GPOS_ASSERT(pmdidFunc->FValid());
	GPOS_ASSERT(pmdidRetType->FValid());
	GPOS_ASSERT(NULL != pstr);
	GPOS_ASSERT(NULL != pdrgpcoldesc);

	// generate a default column set for the list of column descriptors
	m_pdrgpcrOutput = PdrgpcrCreateMapping(pmp, pdrgpcoldesc, UlOpId());

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDFunction *pmdfunc = pmda->Pmdfunc(m_pmdidFunc);

	m_efs = pmdfunc->EfsStability();
	m_efda = pmdfunc->EfdaDataAccess();
	m_fReturnsSet = pmdfunc->FReturnsSet();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::CLogicalTVF
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalTVF::CLogicalTVF
	(
	IMemoryPool *pmp,
	IMDId *pmdidFunc,
	IMDId *pmdidRetType,
	CWStringConst *pstr,
	DrgPcoldesc *pdrgpcoldesc,
	DrgPcr *pdrgpcrOutput
	)
	:
	CLogical(pmp),
	m_pmdidFunc(pmdidFunc),
	m_pmdidRetType(pmdidRetType),
	m_pstr(pstr),
	m_pdrgpcoldesc(pdrgpcoldesc),
	m_pdrgpcrOutput(pdrgpcrOutput)
{
	GPOS_ASSERT(pmdidFunc->FValid());
	GPOS_ASSERT(pmdidRetType->FValid());
	GPOS_ASSERT(NULL != pstr);
	GPOS_ASSERT(NULL != pdrgpcoldesc);
	GPOS_ASSERT(NULL != pdrgpcrOutput);

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDFunction *pmdfunc = pmda->Pmdfunc(m_pmdidFunc);

	m_efs = pmdfunc->EfsStability();
	m_efda = pmdfunc->EfdaDataAccess();
	m_fReturnsSet = pmdfunc->FReturnsSet();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::~CLogicalTVF
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalTVF::~CLogicalTVF()
{
	CRefCount::SafeRelease(m_pmdidFunc);
	CRefCount::SafeRelease(m_pmdidRetType);
	CRefCount::SafeRelease(m_pdrgpcoldesc);
	CRefCount::SafeRelease(m_pdrgpcrOutput);
	GPOS_DELETE(m_pstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::UlHash
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalTVF::UlHash() const
{
	ULONG ulHash = gpos::UlCombineHashes(
								COperator::UlHash(),
								gpos::UlCombineHashes(
										m_pmdidFunc->UlHash(),
										gpos::UlCombineHashes(
												m_pmdidRetType->UlHash(),
												gpos::UlHashPtr<DrgPcoldesc>(m_pdrgpcoldesc))));
	ulHash = gpos::UlCombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrOutput));
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalTVF::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalTVF *popTVF = CLogicalTVF::PopConvert(pop);
		
	return m_pmdidFunc->FEquals(popTVF->PmdidFunc()) &&
			m_pmdidRetType->FEquals(popTVF->PmdidRetType()) &&
			m_pdrgpcoldesc->FEqual(popTVF->Pdrgpcoldesc()) &&
			m_pdrgpcrOutput->FEqual(popTVF->PdrgpcrOutput());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalTVF::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcrOutput = NULL;
	if (fMustExist)
	{
		pdrgpcrOutput = CUtils::PdrgpcrRemapAndCreate(pmp, m_pdrgpcrOutput, phmulcr);
	}
	else
	{
		pdrgpcrOutput = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrOutput, phmulcr, fMustExist);
	}

	CWStringConst *pstr = GPOS_NEW(pmp) CWStringConst(m_pstr->Wsz());
	m_pmdidFunc->AddRef();
	m_pmdidRetType->AddRef();
	m_pdrgpcoldesc->AddRef();

	return GPOS_NEW(pmp) CLogicalTVF(pmp, m_pmdidFunc, m_pmdidRetType, pstr, m_pdrgpcoldesc, pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalTVF::PcrsDeriveOutput
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
//		CLogicalTVF::PfpDerive
//
//	@doc:
//		Derive function properties
//
//---------------------------------------------------------------------------
CFunctionProp *
CLogicalTVF::PfpDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	BOOL fVolatileScan = (IMDFunction::EfsVolatile == m_efs);
	return PfpDeriveFromChildren(pmp, exprhdl, m_efs, m_efda, fVolatileScan, true /*fScan*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::FInputOrderSensitive
//
//	@doc:
//		Sensitivity to input order
//
//---------------------------------------------------------------------------
BOOL
CLogicalTVF::FInputOrderSensitive() const
{
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalTVF::PxfsCandidates
	(
	IMemoryPool *pmp
	) 
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);

	(void) pxfs->FExchangeSet(CXform::ExfUnnestTVF);
	(void) pxfs->FExchangeSet(CXform::ExfImplementTVF);
	(void) pxfs->FExchangeSet(CXform::ExfImplementTVFNoArgs);
	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalTVF::Maxcard
	(
	IMemoryPool *, // pmp
	CExpressionHandle & // exprhdl
	)
	const
{
	if (m_fReturnsSet)
	{
		// unbounded by default
		return CMaxCard();
	}

	return CMaxCard(1);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------

IStatistics *
CLogicalTVF::PstatsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	DrgPstat * // pdrgpstatCtxt
	)
	const
{
	CDouble dRows(1.0);
	if (m_fReturnsSet)
	{
		dRows = CStatistics::DDefaultRelationRows;
	}

	return PstatsDeriveDummy(pmp, exprhdl, dRows);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalTVF::OsPrint
	(
	IOstream &os
	)
	const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}
	os << SzId() << " (" << Pstr()->Wsz() << ") ";
	os << "Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
	os << "] ";
		
	return os;
}

// EOF

