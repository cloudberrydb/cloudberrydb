//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalRowTrigger.cpp
//
//	@doc:
//		Implementation of logical row-level trigger operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalRowTrigger.h"
#include "naucrates/md/CMDTriggerGPDB.h"
#include "gpopt/base/COptCtxt.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::CLogicalRowTrigger
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalRowTrigger::CLogicalRowTrigger
	(
	IMemoryPool *pmp
	)
	:
	CLogical(pmp),
	m_pmdidRel(NULL),
	m_iType(0),
	m_pdrgpcrOld(NULL),
	m_pdrgpcrNew(NULL),
	m_efs(IMDFunction::EfsImmutable),
	m_efda(IMDFunction::EfdaNoSQL)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::CLogicalRowTrigger
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalRowTrigger::CLogicalRowTrigger
	(
	IMemoryPool *pmp,
	IMDId *pmdidRel,
	INT iType,
	DrgPcr *pdrgpcrOld,
	DrgPcr *pdrgpcrNew
	)
	:
	CLogical(pmp),
	m_pmdidRel(pmdidRel),
	m_iType(iType),
	m_pdrgpcrOld(pdrgpcrOld),
	m_pdrgpcrNew(pdrgpcrNew),
	m_efs(IMDFunction::EfsImmutable),
	m_efda(IMDFunction::EfdaNoSQL)
{
	GPOS_ASSERT(pmdidRel->FValid());
	GPOS_ASSERT(0 != iType);
	GPOS_ASSERT(NULL != pdrgpcrNew || NULL != pdrgpcrOld);
	GPOS_ASSERT_IMP(NULL != pdrgpcrNew && NULL != pdrgpcrOld,
			pdrgpcrNew->UlLength() == pdrgpcrOld->UlLength());
	InitFunctionProperties();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::~CLogicalRowTrigger
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalRowTrigger::~CLogicalRowTrigger()
{
	CRefCount::SafeRelease(m_pmdidRel);
	CRefCount::SafeRelease(m_pdrgpcrOld);
	CRefCount::SafeRelease(m_pdrgpcrNew);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::InitFunctionProperties
//
//	@doc:
//		Initialize function properties
//
//---------------------------------------------------------------------------
void
CLogicalRowTrigger::InitFunctionProperties()
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = pmda->Pmdrel(m_pmdidRel);
	const ULONG ulTriggers = pmdrel->UlTriggers();

	for (ULONG ul = 0; ul < ulTriggers; ul++)
	{
		const IMDTrigger *pmdtrigger = pmda->Pmdtrigger(pmdrel->PmdidTrigger(ul));
		if (!pmdtrigger->FEnabled() ||
			!pmdtrigger->FRow() ||
			(ITriggerType(pmdtrigger) & m_iType) != m_iType)
		{
			continue;
		}

		const IMDFunction *pmdfunc = pmda->Pmdfunc(pmdtrigger->PmdidFunc());
		IMDFunction::EFuncStbl efs = pmdfunc->EfsStability();
		IMDFunction::EFuncDataAcc efda = pmdfunc->EfdaDataAccess();

		if (efs > m_efs)
		{
			m_efs = efs;
		}

		if (efda > m_efda)
		{
			m_efda = efda;
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::ITriggerType
//
//	@doc:
//		Return the type of a given trigger as an integer
//
//---------------------------------------------------------------------------
INT
CLogicalRowTrigger::ITriggerType
	(
	const IMDTrigger *pmdtrigger
	)
	const
{
	INT iType = GPMD_TRIGGER_ROW;
	if (pmdtrigger->FBefore())
	{
		iType |= GPMD_TRIGGER_BEFORE;
	}

	if (pmdtrigger->FInsert())
	{
		iType |= GPMD_TRIGGER_INSERT;
	}

	if (pmdtrigger->FDelete())
	{
		iType |= GPMD_TRIGGER_DELETE;
	}

	if (pmdtrigger->FUpdate())
	{
		iType |= GPMD_TRIGGER_UPDATE;
	}

	return iType;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::FMatch
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalRowTrigger::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalRowTrigger *popRowTrigger = CLogicalRowTrigger::PopConvert(pop);

	return m_pmdidRel->FEquals(popRowTrigger->PmdidRel()) &&
			m_iType == popRowTrigger->IType() &&
			m_pdrgpcrOld->FEqual(popRowTrigger->PdrgpcrOld()) &&
			m_pdrgpcrNew->FEqual(popRowTrigger->PdrgpcrNew());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalRowTrigger::UlHash() const
{
	ULONG ulHash = gpos::UlCombineHashes(COperator::UlHash(), m_pmdidRel->UlHash());
	ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHash<INT>(&m_iType));

	if (NULL != m_pdrgpcrOld)
	{
		ulHash = gpos::UlCombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrOld));
	}

	if (NULL != m_pdrgpcrNew)
	{
		ulHash = gpos::UlCombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrNew));
	}

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalRowTrigger::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcrOld = NULL;
	if (NULL != m_pdrgpcrOld)
	{
		pdrgpcrOld = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrOld, phmulcr, fMustExist);
	}

	DrgPcr *pdrgpcrNew = NULL;
	if (NULL != m_pdrgpcrNew)
	{
		pdrgpcrNew = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrNew, phmulcr, fMustExist);
	}

	m_pmdidRel->AddRef();

	return GPOS_NEW(pmp) CLogicalRowTrigger(pmp, m_pmdidRel, m_iType, pdrgpcrOld, pdrgpcrNew);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalRowTrigger::PcrsDeriveOutput
	(
	IMemoryPool *, //pmp,
	CExpressionHandle &exprhdl
	)
{
	return PcrsDeriveOutputPassThru(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalRowTrigger::PkcDeriveKeys
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
//		CLogicalRowTrigger::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalRowTrigger::Maxcard
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
//		CLogicalRowTrigger::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalRowTrigger::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfImplementRowTrigger);
	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalRowTrigger::PstatsDerive
	(
	IMemoryPool *, // pmp,
	CExpressionHandle &exprhdl,
	DrgPstat * // not used
	)
	const
{
	return PstatsPassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::PfpDerive
//
//	@doc:
//		Derive function properties
//
//---------------------------------------------------------------------------
CFunctionProp *
CLogicalRowTrigger::PfpDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	return PfpDeriveFromChildren(pmp, exprhdl, m_efs, m_efda, false /*fHasVolatileFunctionScan*/, false /*fScan*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalRowTrigger::OsPrint
	(
	IOstream &os
	)
	const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	os << SzId() << " (Type: " << m_iType << ")";

	if (NULL != m_pdrgpcrOld)
	{
		os << ", Old Columns: [";
		CUtils::OsPrintDrgPcr(os, m_pdrgpcrOld);
		os << "]";
	}

	if (NULL != m_pdrgpcrNew)
	{
		os << ", New Columns: [";
		CUtils::OsPrintDrgPcr(os, m_pdrgpcrNew);
		os << "]";
	}

	return os;
}

// EOF

