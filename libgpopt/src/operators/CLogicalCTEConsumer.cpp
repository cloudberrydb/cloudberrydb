//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalCTEConsumer.cpp
//
//	@doc:
//		Implementation of CTE consumer operator
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalCTEConsumer.h"
#include "gpopt/operators/CLogicalCTEProducer.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/CMaxCard.h"
#include "gpopt/base/COptCtxt.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::CLogicalCTEConsumer
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalCTEConsumer::CLogicalCTEConsumer
	(
	IMemoryPool *pmp
	)
	:
	CLogical(pmp),
	m_ulId(0),
	m_pdrgpcr(NULL),
	m_pexprInlined(NULL),
	m_phmulcr(NULL),
	m_pcrsOutput(NULL)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::CLogicalCTEConsumer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalCTEConsumer::CLogicalCTEConsumer
	(
	IMemoryPool *pmp,
	ULONG ulId,
	DrgPcr *pdrgpcr
	)
	:
	CLogical(pmp),
	m_ulId(ulId),
	m_pdrgpcr(pdrgpcr),
	m_pexprInlined(NULL),
	m_phmulcr(NULL)
{
	GPOS_ASSERT(NULL != pdrgpcr);
	m_pcrsOutput = GPOS_NEW(pmp) CColRefSet(pmp, m_pdrgpcr);
	CreateInlinedExpr(pmp);
	m_pcrsLocalUsed->Include(m_pdrgpcr);

	// map consumer columns to their positions in consumer output
	COptCtxt::PoctxtFromTLS()->Pcteinfo()->AddConsumerCols(ulId, m_pdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::~CLogicalCTEConsumer
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalCTEConsumer::~CLogicalCTEConsumer()
{
	CRefCount::SafeRelease(m_pdrgpcr);
	CRefCount::SafeRelease(m_pexprInlined);
	CRefCount::SafeRelease(m_phmulcr);
	CRefCount::SafeRelease(m_pcrsOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::CreateInlinedExpr
//
//	@doc:
//		Create the inlined version of this consumer as well as the column mapping
//
//---------------------------------------------------------------------------
void
CLogicalCTEConsumer::CreateInlinedExpr
	(
	IMemoryPool *pmp
	)
{
	CExpression *pexprProducer = COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_ulId);
	GPOS_ASSERT(NULL != pexprProducer);
	// the actual definition of the CTE is the first child of the producer
	CExpression *pexprCTEDef = (*pexprProducer)[0];

	CLogicalCTEProducer *popProducer = CLogicalCTEProducer::PopConvert(pexprProducer->Pop());

	m_phmulcr = CUtils::PhmulcrMapping(pmp, popProducer->Pdrgpcr(), m_pdrgpcr);
	m_pexprInlined = pexprCTEDef->PexprCopyWithRemappedColumns(pmp, m_phmulcr, true /*fMustExist*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalCTEConsumer::PcrsDeriveOutput
	(
	IMemoryPool *, //pmp,
	CExpressionHandle & //exprhdl
	)
{
	m_pcrsOutput->AddRef();
	return m_pcrsOutput;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::PcrsDeriveNotNull
//
//	@doc:
//		Derive not nullable output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalCTEConsumer::PcrsDeriveNotNull
	(
	IMemoryPool *pmp,
	CExpressionHandle & // exprhdl
	)
	const
{
	CExpression *pexprProducer = COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_ulId);
	GPOS_ASSERT(NULL != pexprProducer);

	// find producer's not null columns
	CColRefSet *pcrsProducerNotNull = CDrvdPropRelational::Pdprel(pexprProducer->PdpDerive())->PcrsNotNull();

	// map producer's not null columns to consumer's output columns
	CColRefSet *pcrsConsumerNotNull = CUtils::PcrsRemap(pmp, pcrsProducerNotNull, m_phmulcr, true /*fMustExist*/);
	GPOS_ASSERT(pcrsConsumerNotNull->CElements() == pcrsProducerNotNull->CElements());

	return pcrsConsumerNotNull;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalCTEConsumer::PkcDeriveKeys
	(
	IMemoryPool *, //pmp,
	CExpressionHandle & //exprhdl
	)
	const
{
	CExpression *pexpr = COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_ulId);
	GPOS_ASSERT(NULL != pexpr);
	CKeyCollection *pkc = CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->Pkc();
	if (NULL != pkc)
	{
		pkc->AddRef();
	}

	return pkc;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::PpartinfoDerive
//
//	@doc:
//		Derive partition consumers
//
//---------------------------------------------------------------------------
CPartInfo *
CLogicalCTEConsumer::PpartinfoDerive
	(
	IMemoryPool *, //pmp,
	CExpressionHandle & //exprhdl
	)
	const
{
	CPartInfo *ppartInfo = CDrvdPropRelational::Pdprel(m_pexprInlined->PdpDerive())->Ppartinfo();
	ppartInfo->AddRef();

	return ppartInfo;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalCTEConsumer::Maxcard
	(
	IMemoryPool *, //pmp,
	CExpressionHandle & //exprhdl
	)
	const
{
	CExpression *pexpr = COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_ulId);
	GPOS_ASSERT(NULL != pexpr);
	return CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->Maxcard();
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::UlJoinDepth
//
//	@doc:
//		Derive join depth
//
//---------------------------------------------------------------------------
ULONG
CLogicalCTEConsumer::UlJoinDepth
	(
	IMemoryPool *, //pmp,
	CExpressionHandle & //exprhdl
	)
	const
{
	CExpression *pexpr = COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_ulId);
	GPOS_ASSERT(NULL != pexpr);
	return CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->UlJoinDepth();
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::FMatch
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalCTEConsumer::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalCTEConsumer *popCTEConsumer = CLogicalCTEConsumer::PopConvert(pop);

	return m_ulId == popCTEConsumer->UlCTEId() &&
			m_pdrgpcr->FEqual(popCTEConsumer->Pdrgpcr());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalCTEConsumer::UlHash() const
{
	ULONG ulHash = gpos::UlCombineHashes(COperator::UlHash(), m_ulId);
	ulHash = gpos::UlCombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcr));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::FInputOrderSensitive
//
//	@doc:
//		Not called for leaf operators
//
//---------------------------------------------------------------------------
BOOL
CLogicalCTEConsumer::FInputOrderSensitive() const
{
	GPOS_ASSERT(!"Unexpected function call of FInputOrderSensitive");
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalCTEConsumer::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcr = NULL;
	if (fMustExist)
	{
		pdrgpcr = CUtils::PdrgpcrRemapAndCreate(pmp, m_pdrgpcr, phmulcr);
	}
	else
	{
		pdrgpcr = CUtils::PdrgpcrRemap(pmp, m_pdrgpcr, phmulcr, fMustExist);
	}
	return GPOS_NEW(pmp) CLogicalCTEConsumer(pmp, m_ulId, pdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalCTEConsumer::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfInlineCTEConsumer);
	(void) pxfs->FExchangeSet(CXform::ExfImplementCTEConsumer);
	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::PpcDeriveConstraint
//
//	@doc:
//		Derive constraint property
//
//---------------------------------------------------------------------------
CPropConstraint *
CLogicalCTEConsumer::PpcDeriveConstraint
	(
	IMemoryPool *pmp,
	CExpressionHandle & //exprhdl
	)
	const
{
	CExpression *pexprProducer = COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_ulId);
	GPOS_ASSERT(NULL != pexprProducer);
	CPropConstraint *ppc = CDrvdPropRelational::Pdprel(pexprProducer->PdpDerive())->Ppc();
	DrgPcrs *pdrgpcrs = ppc->PdrgpcrsEquivClasses();
	CConstraint *pcnstr = ppc->Pcnstr();

	// remap producer columns to consumer columns
	DrgPcrs *pdrgpcrsMapped = GPOS_NEW(pmp) DrgPcrs(pmp);
	const ULONG ulLen = pdrgpcrs->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CColRefSet *pcrs = (*pdrgpcrs)[ul];
		CColRefSet *pcrsMapped = CUtils::PcrsRemap(pmp, pcrs, m_phmulcr, true /*fMustExist*/);
		pdrgpcrsMapped->Append(pcrsMapped);
	}

	CConstraint *pcnstrMapped = NULL;
	if (NULL != pcnstr)
	{
		pcnstrMapped = pcnstr->PcnstrCopyWithRemappedColumns(pmp, m_phmulcr, true /*fMustExist*/);
	}

	return GPOS_NEW(pmp) CPropConstraint(pmp, pdrgpcrsMapped, pcnstrMapped);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::PstatsDerive
//
//	@doc:
//		Derive statistics based on cte producer
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalCTEConsumer::PstatsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &, //exprhdl,
	DrgPstat * // pdrgpstat
	)
	const
{
	CExpression *pexprProducer = COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_ulId);
	GPOS_ASSERT(NULL != pexprProducer);
	const IStatistics *pstats = pexprProducer->Pstats();
	GPOS_ASSERT(NULL != pstats);

	// copy the stats with the remaped colids
	IStatistics *pstatsNew = pstats->PstatsCopyWithRemap(pmp, m_phmulcr);

	return pstatsNew;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalCTEConsumer::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " (";
	os << m_ulId;
	os << "), Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcr);
	os	<< "]";

	return os;
}

// EOF
