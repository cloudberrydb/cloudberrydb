//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008, 2009 Greenplum, Inc.
//
//	@filename:
//		CColumnFactory.cpp
//
//	@doc:
//		Implementation of column reference management
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/common/CSyncHashtableAccessByKey.h"
#include "gpos/string/CWStringDynamic.h"
#include "gpos/common/CAutoP.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/CColRefComputed.h"
#include "gpopt/base/CColumnFactory.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CScalarProjectElement.h"

#include "naucrates/md/CMDIdGPDB.h"

using namespace gpopt;
using namespace gpmd;

#define GPOPT_COLFACTORY_HT_BUCKETS	10000

//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::CColumnFactory
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CColumnFactory::CColumnFactory()
	:
	m_pmp(NULL),
	m_phmcrcrs(NULL)
{
	CAutoMemoryPool amp;
	m_pmp = amp.Pmp();
	
	// initialize hash table
	m_sht.Init
		(
		m_pmp,
		GPOPT_COLFACTORY_HT_BUCKETS,
		GPOS_OFFSET(CColRef, m_link),
		GPOS_OFFSET(CColRef, m_ulId),
		&(CColRef::m_ulInvalid),
		CColRef::UlHash,
		CColRef::FEqual
		);

	// now it's safe to detach the auto pool
	(void) amp.PmpDetach();
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::~CColumnFactory
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CColumnFactory::~CColumnFactory()
{
	CRefCount::SafeRelease(m_phmcrcrs);

	// dealloc hash table
	m_sht.Cleanup();

	// destroy mem pool
	CMemoryPoolManager *pmpm = CMemoryPoolManager::Pmpm();
	pmpm->Destroy(m_pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::Initialize
//
//	@doc:
//		Initialize the hash map between computed column and used columns
//
//---------------------------------------------------------------------------
void
CColumnFactory::Initialize()
{
	m_phmcrcrs = GPOS_NEW(m_pmp) HMCrCrs(m_pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrCreate
//
//	@doc:
//		Variant without name for computed columns
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrCreate
	(
	const IMDType *pmdtype,
	INT iTypeModifier,
	OID oidCollation
	)
{
	// increment atomic counter
	ULONG ulId = m_aul.TIncr();
	
	WCHAR wszFmt[] = GPOS_WSZ_LIT("ColRef_%04d");
	CWStringDynamic *pstrTempName = GPOS_NEW(m_pmp) CWStringDynamic(m_pmp);
	CAutoP<CWStringDynamic> a_pstrTempName(pstrTempName);
	pstrTempName->AppendFormat(wszFmt, ulId);
	CWStringConst strName(pstrTempName->Wsz());
	return PcrCreate(pmdtype, iTypeModifier, oidCollation, ulId, CName(&strName));
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrCreate
//
//	@doc:
//		Variant without name for computed columns
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrCreate
	(
	const IMDType *pmdtype,
	INT iTypeModifier,
	OID oidCollation,
	const CName &name
	)
{
	ULONG ulId = m_aul.TIncr();

	return PcrCreate(pmdtype, iTypeModifier, oidCollation, ulId, name);
}
	
	
//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrCreate
//
//	@doc:
//		Basic implementation of all factory methods;
//		Name and id have already determined, we just create the ColRef and
//		insert it into the hashtable
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrCreate
	(
	const IMDType *pmdtype,
	INT iTypeModifier,
	OID oidCollation,
	ULONG ulId,
	const CName &name
	)
{
	CName *pnameCopy = GPOS_NEW(m_pmp) CName(m_pmp, name); 
	CAutoP<CName> a_pnameCopy(pnameCopy);

	CColRef *pcr = GPOS_NEW(m_pmp) CColRefComputed(pmdtype, iTypeModifier, oidCollation, ulId, pnameCopy);
	(void) a_pnameCopy.PtReset();
	CAutoP<CColRef> a_pcr(pcr);
	
	// ensure uniqueness
	GPOS_ASSERT(NULL == PcrLookup(ulId));
	m_sht.Insert(pcr);
	
	return a_pcr.PtReset();
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrCreate
//
//	@doc:
//		Basic implementation of all factory methods;
//		Name and id have already determined, we just create the ColRef and
//		insert it into the hashtable
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrCreate
	(
	const CColumnDescriptor *pcoldesc,
	ULONG ulId,
	const CName &name,
	ULONG ulOpSource
	)
{
	CName *pnameCopy = GPOS_NEW(m_pmp) CName(m_pmp, name);
	CAutoP<CName> a_pnameCopy(pnameCopy);

	CColRef *pcr = GPOS_NEW(m_pmp) CColRefTable(pcoldesc, ulId, pnameCopy, ulOpSource);
	(void) a_pnameCopy.PtReset();
	CAutoP<CColRef> a_pcr(pcr);

	// ensure uniqueness
	GPOS_ASSERT(NULL == PcrLookup(ulId));
	m_sht.Insert(pcr);
	
	return a_pcr.PtReset();
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrCreate
//
//	@doc:
//		Basic implementation of all factory methods;
//		Name and id have already determined, we just create the ColRef and
//		insert it into the hashtable
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrCreate
	(
	const IMDType *pmdtype,
	INT iTypeModifier,
	OID oidCollation,
	INT iAttno,
	BOOL fNullable,
	ULONG ulId,
	const CName &name,
	ULONG ulOpSource,
	ULONG ulWidth
	)
{
	CName *pnameCopy = GPOS_NEW(m_pmp) CName(m_pmp, name);
	CAutoP<CName> a_pnameCopy(pnameCopy);

	CColRef *pcr =
			GPOS_NEW(m_pmp) CColRefTable(pmdtype, iTypeModifier, oidCollation, iAttno, fNullable, ulId, pnameCopy, ulOpSource, ulWidth);
	(void) a_pnameCopy.PtReset();
	CAutoP<CColRef> a_pcr(pcr);

	// ensure uniqueness
	GPOS_ASSERT(NULL == PcrLookup(ulId));
	m_sht.Insert(pcr);

	return a_pcr.PtReset();
}

//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrCreate
//
//	@doc:
//		Variant with alias/name for base table columns
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrCreate
	(
	const CColumnDescriptor *pcoldesc,
	const CName &name,
	ULONG ulOpSource
	)
{
	ULONG ulId = m_aul.TIncr();
	
	return PcrCreate(pcoldesc, ulId, name, ulOpSource);
}

//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrCopy
//
//	@doc:
//		Create a copy of the given colref
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrCopy
	(
	const CColRef* pcr
	)
{
	CName name(pcr->Name());
	if (CColRef::EcrtComputed == pcr->Ecrt())
	{
		return PcrCreate(pcr->Pmdtype(), pcr->ITypeModifier(), pcr->OidCollation(), name);
	}

	GPOS_ASSERT(CColRef::EcrtTable == pcr->Ecrt());
	ULONG ulId = m_aul.TIncr();
	CColRefTable *pcrTable = CColRefTable::PcrConvert(const_cast<CColRef*>(pcr));

	return PcrCreate
			(
			pcr->Pmdtype(),
			pcr->ITypeModifier(),
			pcr->OidCollation(),
			pcrTable->IAttno(),
			pcrTable->FNullable(),
			ulId,
			name,
			pcrTable->UlSourceOpId(),
			pcrTable->UlWidth()
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrLookup
//
//	@doc:
//		Lookup by id
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrLookup
	(
	ULONG ulId
	)
{
	CSyncHashtableAccessByKey<CColRef, ULONG,
		CSpinlockColumnFactory> shtacc(m_sht, ulId);
	
	CColRef *pcr = shtacc.PtLookup();
	
	return pcr;
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::Destroy
//
//	@doc:
//		unlink and destruct
//
//---------------------------------------------------------------------------
void
CColumnFactory::Destroy
	(
	CColRef *pcr
	)
{
	GPOS_ASSERT(NULL != pcr);

	ULONG ulId = pcr->m_ulId;
	
	{
		// scope for the hash table accessor
		CSyncHashtableAccessByKey<CColRef, ULONG, CSpinlockColumnFactory>
			shtacc(m_sht, ulId);
		
		CColRef *pcrFound = shtacc.PtLookup();
		GPOS_ASSERT(pcr == pcrFound);
		
		// unlink from hashtable
		shtacc.Remove(pcrFound);
	}
	
	GPOS_DELETE(pcr);
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrsUsedInComputedCol
//
//	@doc:
//		Lookup the set of used column references (if any) based on id of
//		computed column
//---------------------------------------------------------------------------
const CColRefSet *
CColumnFactory::PcrsUsedInComputedCol
	(
	const CColRef *pcr
	)
{
	GPOS_ASSERT(NULL != pcr);
	GPOS_ASSERT(NULL != m_phmcrcrs);

	// get its column reference set from the hash map
	const CColRefSet *pcrs = m_phmcrcrs->PtLookup(pcr);

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::AddComputedToUsedColsMap
//
//	@doc:
//		Add the map between computed column and its used columns
//
//---------------------------------------------------------------------------
void
CColumnFactory::AddComputedToUsedColsMap
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != m_phmcrcrs);

	const CScalarProjectElement *popScPrEl = CScalarProjectElement::PopConvert(pexpr->Pop());
	CColRef *pcrComputedCol = popScPrEl->Pcr();

	CDrvdPropScalar *pdpscalar = CDrvdPropScalar::Pdpscalar(pexpr->PdpDerive());
	CColRefSet *pcrsUsed = pdpscalar->PcrsUsed();
	if (NULL != pcrsUsed && 0 < pcrsUsed->CElements())
	{
#ifdef GPOS_DEBUG
		BOOL fres =
#endif // GPOS_DEBUG
			m_phmcrcrs->FInsert(pcrComputedCol, GPOS_NEW(m_pmp) CColRefSet(m_pmp, *pcrsUsed));
		GPOS_ASSERT(fres);
	}
}


// EOF

