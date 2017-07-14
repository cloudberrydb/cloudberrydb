//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalDML.cpp
//
//	@doc:
//		Implementation of logical DML operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/CPartIndexMap.h"

#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalDML.h"


using namespace gpopt;

const WCHAR CLogicalDML::m_rgwszDml[EdmlSentinel][10] =
{
	GPOS_WSZ_LIT("Insert"),
	GPOS_WSZ_LIT("Delete"),
	GPOS_WSZ_LIT("Update")
};

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDML::CLogicalDML
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalDML::CLogicalDML
	(
	IMemoryPool *pmp
	)
	:
	CLogical(pmp),
	m_ptabdesc(NULL),
	m_pdrgpcrSource(NULL),
	m_pbsModified(NULL),
	m_pcrAction(NULL),
	m_pcrTableOid(NULL),
	m_pcrCtid(NULL),
	m_pcrSegmentId(NULL),
	m_pcrTupleOid(NULL)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDML::CLogicalDML
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalDML::CLogicalDML
	(
	IMemoryPool *pmp,
	EDMLOperator edmlop,
	CTableDescriptor *ptabdesc,
	DrgPcr *pdrgpcrSource,
	CBitSet *pbsModified,
	CColRef *pcrAction,
	CColRef *pcrTableOid,
	CColRef *pcrCtid,
	CColRef *pcrSegmentId,
	CColRef *pcrTupleOid
	)
	:
	CLogical(pmp),
	m_edmlop(edmlop),
	m_ptabdesc(ptabdesc),
	m_pdrgpcrSource(pdrgpcrSource),
	m_pbsModified(pbsModified),
	m_pcrAction(pcrAction),
	m_pcrTableOid(pcrTableOid),
	m_pcrCtid(pcrCtid),
	m_pcrSegmentId(pcrSegmentId),
	m_pcrTupleOid(pcrTupleOid)
{
	GPOS_ASSERT(EdmlSentinel != edmlop);
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pdrgpcrSource);
	GPOS_ASSERT(NULL != pbsModified);
	GPOS_ASSERT(NULL != pcrAction);
	GPOS_ASSERT_IMP(EdmlDelete == edmlop || EdmlUpdate == edmlop,
					NULL != pcrCtid && NULL != pcrSegmentId);

	m_pcrsLocalUsed->Include(m_pdrgpcrSource);
	m_pcrsLocalUsed->Include(m_pcrAction);
	if (NULL != m_pcrTableOid)
	{
		m_pcrsLocalUsed->Include(m_pcrTableOid);

	}
	if (NULL != m_pcrCtid)
	{
		m_pcrsLocalUsed->Include(m_pcrCtid);
	}

	if (NULL != m_pcrSegmentId)
	{
		m_pcrsLocalUsed->Include(m_pcrSegmentId);
	}

	if (NULL != m_pcrTupleOid)
	{
		m_pcrsLocalUsed->Include(m_pcrTupleOid);
	}
}

	
//---------------------------------------------------------------------------
//	@function:
//		CLogicalDML::~CLogicalDML
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalDML::~CLogicalDML()
{
	CRefCount::SafeRelease(m_ptabdesc);
	CRefCount::SafeRelease(m_pdrgpcrSource);
	CRefCount::SafeRelease(m_pbsModified);
}
			
//---------------------------------------------------------------------------
//	@function:
//		CLogicalDML::FMatch
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalDML::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}
	
	CLogicalDML *popDML = CLogicalDML::PopConvert(pop);

	return m_pcrAction == popDML->PcrAction() &&
			m_pcrTableOid == popDML->PcrTableOid() &&
			m_pcrCtid == popDML->PcrCtid() &&
			m_pcrSegmentId == popDML->PcrSegmentId() &&
			m_pcrTupleOid == popDML->PcrTupleOid() &&
			m_ptabdesc->Pmdid()->FEquals(popDML->Ptabdesc()->Pmdid()) &&
			m_pdrgpcrSource->FEqual(popDML->PdrgpcrSource());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDML::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalDML::UlHash() const
{
	ULONG ulHash = gpos::UlCombineHashes(COperator::UlHash(), m_ptabdesc->Pmdid()->UlHash());
	ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHashPtr<CColRef>(m_pcrAction));
	ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHashPtr<CColRef>(m_pcrTableOid));
	ulHash = gpos::UlCombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrSource));

	if (EdmlDelete == m_edmlop || EdmlUpdate == m_edmlop)
	{
		ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHashPtr<CColRef>(m_pcrCtid));
		ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHashPtr<CColRef>(m_pcrSegmentId));
	}

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDML::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalDML::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcr = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrSource, phmulcr, fMustExist);
	CColRef *pcrAction = CUtils::PcrRemap(m_pcrAction, phmulcr, fMustExist);
	CColRef *pcrTableOid = CUtils::PcrRemap(m_pcrTableOid, phmulcr, fMustExist);
	
	// no need to remap modified columns bitset as it represent column indexes
	// and not actual columns
	m_pbsModified->AddRef();
	
	CColRef *pcrCtid = NULL;
	if (NULL != m_pcrCtid)
	{
		pcrCtid = CUtils::PcrRemap(m_pcrCtid, phmulcr, fMustExist);
	}

	CColRef *pcrSegmentId = NULL;
	if (NULL != m_pcrSegmentId)
	{
		pcrSegmentId = CUtils::PcrRemap(m_pcrSegmentId, phmulcr, fMustExist);
	}

	CColRef *pcrTupleOid = NULL;
	if (NULL != m_pcrTupleOid)
	{
		pcrTupleOid = CUtils::PcrRemap(m_pcrTupleOid, phmulcr, fMustExist);
	}
	
	m_ptabdesc->AddRef();

	return GPOS_NEW(pmp) CLogicalDML(pmp, m_edmlop, m_ptabdesc, pdrgpcr, m_pbsModified, pcrAction, pcrTableOid, pcrCtid, pcrSegmentId, pcrTupleOid);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDML::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalDML::PcrsDeriveOutput
	(
	IMemoryPool *pmp,
	CExpressionHandle & //exprhdl
	)
{
	CColRefSet *pcrsOutput = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsOutput->Include(m_pdrgpcrSource);
	if (NULL != m_pcrCtid)
	{
		GPOS_ASSERT(NULL != m_pcrSegmentId);
		pcrsOutput->Include(m_pcrCtid);
		pcrsOutput->Include(m_pcrSegmentId);
	}
	
	pcrsOutput->Include(m_pcrAction);
	
	if (NULL != m_pcrTupleOid)
	{
		pcrsOutput->Include(m_pcrTupleOid);
	}
	
	return pcrsOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDML::PpcDeriveConstraint
//
//	@doc:
//		Derive constraint property
//
//---------------------------------------------------------------------------
CPropConstraint *
CLogicalDML::PpcDeriveConstraint
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	CColRefSet *pcrsOutput = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsOutput->Include(m_pdrgpcrSource);
	CPropConstraint *ppc = PpcDeriveConstraintRestrict(pmp, exprhdl, pcrsOutput);
	pcrsOutput->Release();

	return ppc;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDML::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalDML::PkcDeriveKeys
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
//		CLogicalDML::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalDML::Maxcard
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
//		CLogicalDML::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalDML::PxfsCandidates
	(
	IMemoryPool *pmp
	) 
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfImplementDML);
	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDML::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalDML::PstatsDerive
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
//		CLogicalDML::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalDML::OsPrint
	(
	IOstream &os
	)
	const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	os	<< SzId()
		<< " (";
	os << m_rgwszDml[m_edmlop] << ", ";
	m_ptabdesc->Name().OsPrint(os);
	os << "), Affected Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrSource);
	os	<< "], Action: (";
	m_pcrAction->OsPrint(os);
	os << ")";

	if (m_pcrTableOid != NULL)
	{
		os << ", Oid: (";
		m_pcrTableOid->OsPrint(os);
		os << ")";
	}

	if (EdmlDelete == m_edmlop || EdmlUpdate == m_edmlop)
	{
		os << ", ";
		m_pcrCtid->OsPrint(os);
		os	<< ", ";
		m_pcrSegmentId->OsPrint(os);
	}
	
	return os;
}

// EOF

