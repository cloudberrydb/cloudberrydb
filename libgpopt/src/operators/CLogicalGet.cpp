//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogicalGet.cpp
//
//	@doc:
//		Implementation of basic table access
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CAutoP.h"
#include "gpos/common/CDynamicPtrArray.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/COptCtxt.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalGet.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/metadata/CName.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::CLogicalGet
//
//	@doc:
//		ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalGet::CLogicalGet
	(
	IMemoryPool *pmp
	)
	:
	CLogical(pmp),
	m_pnameAlias(NULL),
	m_ptabdesc(NULL),
	m_pdrgpcrOutput(NULL),
	m_pdrgpdrgpcrPart(NULL),
	m_pcrsDist(NULL)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::CLogicalGet
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalGet::CLogicalGet
	(
	IMemoryPool *pmp,
	const CName *pnameAlias,
	CTableDescriptor *ptabdesc
	)
	:
	CLogical(pmp),
	m_pnameAlias(pnameAlias),
	m_ptabdesc(ptabdesc),
	m_pdrgpcrOutput(NULL),
	m_pdrgpdrgpcrPart(NULL),
	m_pcrsDist(NULL)
{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pnameAlias);

	// generate a default column set for the table descriptor
	m_pdrgpcrOutput = PdrgpcrCreateMapping(pmp, m_ptabdesc->Pdrgpcoldesc(), UlOpId());
	
	if (m_ptabdesc->FPartitioned())
	{
		m_pdrgpdrgpcrPart = PdrgpdrgpcrCreatePartCols(pmp, m_pdrgpcrOutput, m_ptabdesc->PdrgpulPart());
	}

	m_pcrsDist = CLogical::PcrsDist(pmp, m_ptabdesc, m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::CLogicalGet
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalGet::CLogicalGet
	(
	IMemoryPool *pmp,
	const CName *pnameAlias,
	CTableDescriptor *ptabdesc,
	DrgPcr *pdrgpcrOutput
	)
	:
	CLogical(pmp),
	m_pnameAlias(pnameAlias),
	m_ptabdesc(ptabdesc),
	m_pdrgpcrOutput(pdrgpcrOutput),
	m_pdrgpdrgpcrPart(NULL)
{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pnameAlias);

	if (m_ptabdesc->FPartitioned())
	{
		m_pdrgpdrgpcrPart = PdrgpdrgpcrCreatePartCols(pmp, m_pdrgpcrOutput, m_ptabdesc->PdrgpulPart());
	}

	m_pcrsDist = CLogical::PcrsDist(pmp, m_ptabdesc, m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::~CLogicalGet
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CLogicalGet::~CLogicalGet()
{
	CRefCount::SafeRelease(m_ptabdesc);
	CRefCount::SafeRelease(m_pdrgpcrOutput);
	CRefCount::SafeRelease(m_pdrgpdrgpcrPart);
	CRefCount::SafeRelease(m_pcrsDist);
	
	GPOS_DELETE(m_pnameAlias);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::UlHash
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalGet::UlHash() const
{
	ULONG ulHash = gpos::UlCombineHashes(COperator::UlHash(), m_ptabdesc->Pmdid()->UlHash());
	ulHash = gpos::UlCombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrOutput));

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalGet::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}
	CLogicalGet *popGet = CLogicalGet::PopConvert(pop);
	
	return m_ptabdesc->Pmdid()->FEquals(popGet->m_ptabdesc->Pmdid()) &&
			m_pdrgpcrOutput->FEqual(popGet->PdrgpcrOutput());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalGet::PopCopyWithRemappedColumns
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
	CName *pnameAlias = GPOS_NEW(pmp) CName(pmp, *m_pnameAlias);
	m_ptabdesc->AddRef();

	return GPOS_NEW(pmp) CLogicalGet(pmp, pnameAlias, m_ptabdesc, pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalGet::PcrsDeriveOutput
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
//		CLogicalGet::PcrsDeriveNotNull
//
//	@doc:
//		Derive not null output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalGet::PcrsDeriveNotNull
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	// get all output columns
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrs->Include(CDrvdPropRelational::Pdprel(exprhdl.Pdp())->PcrsOutput());

	// filters out nullable columns
	CColRefSetIter crsi(*CDrvdPropRelational::Pdprel(exprhdl.Pdp())->PcrsOutput());
	while (crsi.FAdvance())
	{
		CColRefTable *pcrtable = CColRefTable::PcrConvert(const_cast<CColRef*>(crsi.Pcr()));
		if (pcrtable->FNullable())
		{
			pcrs->Exclude(pcrtable);
		}
	}

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::FInputOrderSensitive
//
//	@doc:
//		Not called for leaf operators
//
//---------------------------------------------------------------------------
BOOL
CLogicalGet::FInputOrderSensitive() const
{
	GPOS_ASSERT(!"Unexpected function call of FInputOrderSensitive");
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalGet::PkcDeriveKeys
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
//		CLogicalGet::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalGet::PxfsCandidates
	(
	IMemoryPool *pmp
	) 
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	
	(void) pxfs->FExchangeSet(CXform::ExfGet2TableScan);
	
	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::PstatsDerive
//
//	@doc:
//		Load up statistics from metadata
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalGet::PstatsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	DrgPstat * // not used
	)
	const
{
	// requesting stats on distribution columns to estimate data skew
	IStatistics *pstatsTable = PstatsBaseTable(pmp, exprhdl, m_ptabdesc, m_pcrsDist);
	
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp, m_pdrgpcrOutput);
	CUpperBoundNDVs *pubndv = GPOS_NEW(pmp) CUpperBoundNDVs(pcrs, pstatsTable->DRows());
	CStatistics::PstatsConvert(pstatsTable)->AddCardUpperBound(pubndv);

	return pstatsTable;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalGet::OsPrint
	(
	IOstream &os
	)
	const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}
	else
	{
		os << SzId() << " ";
		// alias of table as referenced in the query
		m_pnameAlias->OsPrint(os);

		// actual name of table in catalog and columns
		os << " (";
		m_ptabdesc->Name().OsPrint(os);
		os << "), Columns: [";
		CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
		os << "] Key sets: {";
		
		const ULONG ulColumns = m_pdrgpcrOutput->UlLength();
		const DrgPbs *pdrgpbsKeys = m_ptabdesc->PdrgpbsKeys();
		for (ULONG ul = 0; ul < pdrgpbsKeys->UlLength(); ul++)
		{
			CBitSet *pbs = (*pdrgpbsKeys)[ul];
			if (0 < ul)
			{
				os << ", ";
			}
			os << "[";
			ULONG ulPrintedKeys = 0;
			for (ULONG ulKey = 0; ulKey < ulColumns; ulKey++)
			{
				if (pbs->FBit(ulKey))
				{
					if (0 < ulPrintedKeys)
					{
						os << ",";
					}
					os << ulKey;
					ulPrintedKeys++;
				}
			}
			os << "]";
		}
		os << "}";
		
	}
		
	return os;
}



// EOF

