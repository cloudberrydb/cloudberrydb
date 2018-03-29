//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPartIndexMap.cpp
//
//	@doc:
//		Implementation of partition index map
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CPartIndexMap.h"
#include "gpopt/base/CPartitionPropagationSpec.h"

#ifdef GPOS_DEBUG
#include "gpopt/base/COptCtxt.h"
#include "gpos/error/CAutoTrace.h"
#endif // GPOS_DEBUG

using namespace gpopt;

// initialization of static variables
const CHAR *CPartIndexMap::CPartTableInfo::m_szManipulator[EpimSentinel] =
{
	"propagator",
	"consumer",
	"resolver"
};

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::CPartTableInfo::CPartTableInfo
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartIndexMap::CPartTableInfo::CPartTableInfo
	(
	ULONG ulScanId,
	PartCnstrMap *ppartcnstrmap,
	EPartIndexManipulator epim,
	IMDId *pmdid,
	DrgPpartkeys *pdrgppartkeys,
	CPartConstraint *ppartcnstrRel,
	ULONG ulPropagators
	)
	:
	m_ulScanId(ulScanId),
	m_ppartcnstrmap(ppartcnstrmap),
	m_epim(epim),
	m_pmdid(pmdid),
	m_pdrgppartkeys(pdrgppartkeys),
	m_ppartcnstrRel(ppartcnstrRel),
	m_ulPropagators(ulPropagators)
{
	GPOS_ASSERT(EpimSentinel > epim &&
				"Invalid manipulator type");

	GPOS_ASSERT(pmdid->FValid());
	GPOS_ASSERT(pdrgppartkeys != NULL);
	GPOS_ASSERT(0 < pdrgppartkeys->UlLength());
	GPOS_ASSERT(NULL != ppartcnstrRel);
	GPOS_ASSERT_IMP(CPartIndexMap::EpimConsumer != epim, 0 == ulPropagators);

	m_fPartialScans = FDefinesPartialScans(ppartcnstrmap, ppartcnstrRel);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::CPartTableInfo::~CPartTableInfo
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPartIndexMap::CPartTableInfo::~CPartTableInfo()
{
	CRefCount::SafeRelease(m_ppartcnstrmap);
	m_pmdid->Release();
	m_pdrgppartkeys->Release();
	m_ppartcnstrRel->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::CPartTableInfo::AddPartConstraint
//
//	@doc:
//		Add a part constraint
//
//---------------------------------------------------------------------------
void
CPartIndexMap::CPartTableInfo::AddPartConstraint
	(
	IMemoryPool *pmp,
	ULONG ulScanId,
	CPartConstraint *ppartcnstr
	)
{
	GPOS_ASSERT(NULL != m_ppartcnstrmap);
#ifdef GPOS_DEBUG
	BOOL fResult =
#endif // GPOS_DEBUG
	m_ppartcnstrmap->FInsert(GPOS_NEW(pmp) ULONG(ulScanId), ppartcnstr);

	GPOS_ASSERT(fResult && "Part constraint already exists in map");
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::CPartTableInfo::AddPartConstraint
//
//	@doc:
//		Add a part constraint
//
//---------------------------------------------------------------------------
void
CPartIndexMap::CPartTableInfo::AddPartConstraints
	(
	IMemoryPool *pmp,
	PartCnstrMap *ppartcnstrmap
	)
{
	GPOS_ASSERT(NULL != ppartcnstrmap);

	PartCnstrMapIter partcnstriter(ppartcnstrmap);
	while (partcnstriter.FAdvance())
	{
		ULONG ulScanId = *(partcnstriter.Pk());
		CPartConstraint *ppartcnstr = const_cast<CPartConstraint*>(partcnstriter.Pt());

		ppartcnstr->AddRef();
		AddPartConstraint(pmp, ulScanId, ppartcnstr);
	}

	m_fPartialScans = m_fPartialScans || FDefinesPartialScans(ppartcnstrmap, m_ppartcnstrRel);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::CPartTableInfo::SzManipulatorType
//
//	@doc:
//		Description of mainpulator type
//
//---------------------------------------------------------------------------
const CHAR *
CPartIndexMap::CPartTableInfo::SzManipulatorType
	(
	EPartIndexManipulator epim
	)
{
	GPOS_ASSERT(EpimSentinel > epim);
	return m_szManipulator[epim];
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::CPartTableInfo::FDefinesPartialScans
//
//	@doc:
//		Does given part constraint map define partial scans
//
//---------------------------------------------------------------------------
BOOL
CPartIndexMap::CPartTableInfo::FDefinesPartialScans
	(
	PartCnstrMap *ppartcnstrmap,
	CPartConstraint *ppartcnstrRel
	)
{
	if (NULL == ppartcnstrmap)
	{
		return false;
	}

	PartCnstrMapIter partcnstriter(ppartcnstrmap);
	while (partcnstriter.FAdvance())
	{
		const CPartConstraint *ppartcnstr = partcnstriter.Pt();
		if (!ppartcnstr->FUninterpreted() && !ppartcnstr->FUnbounded() &&
				!ppartcnstrRel->FEquivalent(ppartcnstr))
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::CPartTableInfo::OsPrint
//
//	@doc:
//		Print functions
//
//---------------------------------------------------------------------------
IOstream &
CPartIndexMap::CPartTableInfo::OsPrint
	(
	IOstream &os
	)
	const
{
	os << CPartTableInfo::SzManipulatorType(Epim());
	os << "<Scan Id: " << m_ulScanId << ">";
	os << ", <Propagators: " << m_ulPropagators << ">";
	return os;
}

#ifdef GPOS_DEBUG
void
CPartIndexMap::CPartTableInfo::DbgPrint() const
{

	IMemoryPool *pmp = COptCtxt::PoctxtFromTLS()->Pmp();
	CAutoTrace at(pmp);
	(void) this->OsPrint(at.Os());
}
#endif

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::CPartIndexMap
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartIndexMap::CPartIndexMap
	(
	IMemoryPool *pmp
	)
	:
	m_pmp(pmp),
	m_pim(NULL),
	m_ulUnresolved(0),
	m_ulUnresolvedZeroPropagators(0)
{
	GPOS_ASSERT(NULL != pmp);

	m_pim = GPOS_NEW(m_pmp) PartIndexMap(m_pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::~CPartIndexMap
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPartIndexMap::~CPartIndexMap()
{
	m_pim->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::Insert
//
//	@doc:
//		Insert a new map entry;
//		if entry is already found, increase its number of manipulators
//
//---------------------------------------------------------------------------
void
CPartIndexMap::Insert
	(
	ULONG ulScanId,
	PartCnstrMap *ppartcnstrmap,
	EPartIndexManipulator epim,
	ULONG ulExpectedPropagators,
	IMDId *pmdid,
	DrgPpartkeys *pdrgppartkeys,
	CPartConstraint *ppartcnstrRel
	)
{
	CPartTableInfo *ppti = m_pim->PtLookup(&ulScanId);
	if (NULL == ppti)
	{
		// no entry is found, create a new entry
		ppti = GPOS_NEW(m_pmp) CPartTableInfo(ulScanId, ppartcnstrmap, epim, pmdid, pdrgppartkeys, ppartcnstrRel, ulExpectedPropagators);
#ifdef GPOS_DEBUG
		BOOL fSuccess =
#endif // GPOS_DEBUG
		m_pim->FInsert(GPOS_NEW(m_pmp) ULONG(ulScanId), ppti);
		GPOS_ASSERT(fSuccess && "failed to insert partition index map entry");
		
		// increase number of unresolved consumers
		if (EpimConsumer == epim)
		{
			m_ulUnresolved++;
			if (0 == ulExpectedPropagators)
			{
				m_ulUnresolvedZeroPropagators++;
			}
		}
	}
	else 
	{
		BOOL fEmpty = (ppartcnstrmap->UlEntries() == 0);
		
		if (!fEmpty)
		{
			// add part constraints to part info
			ppti->AddPartConstraints(m_pmp, ppartcnstrmap);
		}
		
		pmdid->Release();
		pdrgppartkeys->Release();
		ppartcnstrmap->Release();
		ppartcnstrRel->Release();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::PptiLookup
//
//	@doc:
//		Lookup partition index map entry given scan id
//
//---------------------------------------------------------------------------
CPartIndexMap::CPartTableInfo *
CPartIndexMap::PptiLookup
	(
	ULONG ulScanId
	)
	const
{
	return m_pim->PtLookup(&ulScanId);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::Pdrgppartkeys
//
//	@doc:
//		Part keys of the entry with the given scan id
//
//---------------------------------------------------------------------------
DrgPpartkeys *
CPartIndexMap::Pdrgppartkeys
	(
	ULONG ulScanId
	)
	const
{
	CPartTableInfo* ppti = m_pim->PtLookup(&ulScanId);
	GPOS_ASSERT(NULL != ppti);

	return ppti->Pdrgppartkeys();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::PmdidRel
//
//	@doc:
//		Relation mdid of the entry with the given scan id
//
//---------------------------------------------------------------------------
IMDId *
CPartIndexMap::PmdidRel
	(
	ULONG ulScanId
	)
	const
{
	CPartTableInfo* ppti = m_pim->PtLookup(&ulScanId);
	GPOS_ASSERT(NULL != ppti);

	return ppti->Pmdid();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::Ppartcnstrmap
//
//	@doc:
//		Part constraint map of the entry with the given scan id
//
//---------------------------------------------------------------------------
PartCnstrMap *CPartIndexMap::Ppartcnstrmap
	(
	ULONG ulScanId
	)
	const
{
	CPartTableInfo* ppti = m_pim->PtLookup(&ulScanId);
	GPOS_ASSERT(NULL != ppti);

	return ppti->Ppartcnstrmap();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::PpartcnstrRel
//
//	@doc:
//		Relation part constraint of the entry with the given scan id
//
//---------------------------------------------------------------------------
CPartConstraint *CPartIndexMap::PpartcnstrRel
	(
	ULONG ulScanId
	)
	const
{
	CPartTableInfo* ppti = m_pim->PtLookup(&ulScanId);
	GPOS_ASSERT(NULL != ppti);

	return ppti->PpartcnstrRel();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::Epim
//
//	@doc:
//		Manipulator type of the entry with the given scan id
//
//---------------------------------------------------------------------------
CPartIndexMap::EPartIndexManipulator
CPartIndexMap::Epim
	(
	ULONG ulScanId
	)
	const
{
	CPartTableInfo* ppti = m_pim->PtLookup(&ulScanId);
	GPOS_ASSERT(NULL != ppti);

	return ppti->Epim();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::UlExpectedPropagators
//
//	@doc:
//		Number of expected propagators of the entry with the given scan id.
//		Returns gpos::ulong_max if no entry with the given scan id is found
//
//---------------------------------------------------------------------------
ULONG
CPartIndexMap::UlExpectedPropagators
	(
	ULONG ulScanId
	)
	const
{
	CPartTableInfo* ppti = m_pim->PtLookup(&ulScanId);
	if (NULL == ppti)
	{
		return gpos::ulong_max;
	}

	return ppti->UlExpectedPropagators();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::SetExpectedPropagators
//
//	@doc:
//		Set the number of expected propagators for the entry with the given
//		scan id
//
//---------------------------------------------------------------------------
void
CPartIndexMap::SetExpectedPropagators
	(
	ULONG ulScanId,
	ULONG ulPropagators
	)
{
	CPartTableInfo* ppti = m_pim->PtLookup(&ulScanId);
	GPOS_ASSERT(NULL != ppti);
	ppti->SetExpectedPropagators(ulPropagators);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::FPartialScans
//
//	@doc:
//		Check whether the entry with the given scan id has partial scans
//
//---------------------------------------------------------------------------
BOOL CPartIndexMap::FPartialScans
	(
	ULONG ulScanId
	)
	const
{
	CPartTableInfo* ppti = m_pim->PtLookup(&ulScanId);
	GPOS_ASSERT(NULL != ppti);

	return ppti->FPartialScans();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::AddUnresolved
//
//	@doc:
//		Helper to add part-index id's found in the first map and are
//		unresolved based on the second map
//
//		For example, if the first and second map contain the following entries:
//		pimFst:
//			(partindexid: 1, consumer, part cnstr: 5->[1,3)),
//			(partindexid: 2, consumer, part cnstr: <>),
//		pimSnd:
//			(partindexid: 1, consumer, part cnstr: 6->(4,5))),
//			(partindexid: 2, producer, part cnstr: <>),
//			(partindexid: 3, producer, part cnstr: <>)
//		the result will be:
//			(partindexid: 1, consumer, part cnstr: 5->[1,3), 6->(4,5)),		// part constraint get combined
//			(partindexid: 2, resolver, part cnstr: <>),						// consumer+producer=resolver
//			(partindexid: 3, producer, part cnstr: <>)						// no match for part index id 3: copy out
//
//---------------------------------------------------------------------------
void
CPartIndexMap::AddUnresolved
	(
	IMemoryPool *pmp,
	const CPartIndexMap &pimFst,
	const CPartIndexMap &pimSnd,
	CPartIndexMap* ppimResult
	)
{
	// iterate on first map and lookup entries in second map
	PartIndexMapIter pimiFst(pimFst.m_pim);
	while (pimiFst.FAdvance())
	{
		const CPartTableInfo *pptiFst = pimiFst.Pt();
		ULONG ulScanId = pptiFst->UlScanId();
		EPartIndexManipulator epimFst = pptiFst->Epim();
		ULONG ulPropagatorsFst = pptiFst->UlExpectedPropagators();

		if (NULL != ppimResult->PptiLookup(ulScanId))
		{
			// skip entries already in the result map
			continue;
		}

		// check if entry exists in second map
		CPartTableInfo *pptiSnd = pimSnd.PptiLookup(ulScanId);
		
		EPartIndexManipulator epimResult = epimFst;
		ULONG ulPropagatorsResult = ulPropagatorsFst;
		PartCnstrMap *ppartcnstrmapSnd = NULL;
		if (NULL != pptiSnd)
		{		
			EPartIndexManipulator epimSnd = pptiSnd->Epim();
			ULONG ulPropagatorsSnd = pptiSnd->UlExpectedPropagators();

			GPOS_ASSERT_IMP(epimFst == EpimConsumer && epimSnd == EpimConsumer, ulPropagatorsFst == ulPropagatorsSnd);
			ResolvePropagator(epimFst, ulPropagatorsFst, epimSnd, ulPropagatorsSnd, &epimResult, &ulPropagatorsResult);
			ppartcnstrmapSnd = pptiSnd->Ppartcnstrmap();
		}
		
		// copy mdid and partition columns from part index map entry
		IMDId *pmdid = pptiFst->Pmdid();
		DrgPpartkeys *pdrgppartkeys = pptiFst->Pdrgppartkeys();
		CPartConstraint *ppartcnstrRel = pptiFst->PpartcnstrRel();
		
		PartCnstrMap *ppartcnstrmap = CPartConstraint::PpartcnstrmapCombine(pmp, pptiFst->Ppartcnstrmap(), ppartcnstrmapSnd);

		pmdid->AddRef();
		pdrgppartkeys->AddRef();
		ppartcnstrRel->AddRef();
		
		ppimResult->Insert(ulScanId, ppartcnstrmap, epimResult, ulPropagatorsResult, pmdid, pdrgppartkeys, ppartcnstrRel);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::ResolvePropagator
//
//	@doc:
//		Handle the cases where one of the given manipulators is a propagator and the other is a consumer
//
//---------------------------------------------------------------------------
void
CPartIndexMap::ResolvePropagator
	(
	EPartIndexManipulator epimFst,
	ULONG ulExpectedPropagatorsFst,
	EPartIndexManipulator epimSnd,
	ULONG ulExpectedPropagatorsSnd,
	EPartIndexManipulator *pepimResult,		// output
	ULONG *pulExpectedPropagatorsResult		// output
	)
{
	GPOS_ASSERT(NULL != pepimResult);
	GPOS_ASSERT(NULL != pulExpectedPropagatorsResult);

	if (EpimPropagator == epimFst && EpimConsumer == epimSnd)
	{
		if (0 < ulExpectedPropagatorsSnd)
		{
			*pulExpectedPropagatorsResult = ulExpectedPropagatorsSnd - 1;
			*pepimResult = (1 == ulExpectedPropagatorsSnd)? EpimResolver: EpimConsumer;
		}
		else
		{
			*pulExpectedPropagatorsResult = gpos::ulong_max;
			*pepimResult = EpimConsumer;
		}
	}
	else if (EpimConsumer == epimFst && EpimPropagator == epimSnd)
	{
		if (0 < ulExpectedPropagatorsFst)
		{
			*pulExpectedPropagatorsResult = ulExpectedPropagatorsFst - 1;
			*pepimResult = (1 == ulExpectedPropagatorsFst)? EpimResolver: EpimConsumer;
		}
		else
		{
			*pulExpectedPropagatorsResult = gpos::ulong_max;
			*pepimResult = EpimConsumer;
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::PpimCombine
//
//	@doc:
//		Combine the two given maps and return the result
//
//---------------------------------------------------------------------------
CPartIndexMap *
CPartIndexMap::PpimCombine
	(
	IMemoryPool *pmp,
	const CPartIndexMap &pimFst,
	const CPartIndexMap &pimSnd
	)
{
	CPartIndexMap *ppim = GPOS_NEW(pmp) CPartIndexMap(pmp);

	// add entries from first map that are not resolvable based on second map
	AddUnresolved(pmp, pimFst, pimSnd, ppim);

	// add entries from second map that are not resolvable based on first map
	AddUnresolved(pmp, pimSnd, pimFst, ppim);

	return ppim;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::FContainsUnresolved
//
//	@doc:
//		Does map contain unresolved entries
//
//---------------------------------------------------------------------------
BOOL
CPartIndexMap::FContainsUnresolved() const
{
	return (0 != m_ulUnresolved);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::FContainsUnresolvedZeroPropagators
//
//	@doc:
//		Does map contain unresolved entries with zero propagators
//
//---------------------------------------------------------------------------
BOOL
CPartIndexMap::FContainsUnresolvedZeroPropagators() const
{
	return (0 != m_ulUnresolvedZeroPropagators);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::PdrgpulScanIds
//
//	@doc:
//		 Extract scan ids
//
//---------------------------------------------------------------------------
DrgPul *
CPartIndexMap::PdrgpulScanIds
	(
	IMemoryPool *pmp,
	BOOL fConsumersOnly
	)
	const
{
	DrgPul *pdrgpul = GPOS_NEW(pmp) DrgPul(pmp);
	PartIndexMapIter pimi(m_pim);
	while (pimi.FAdvance())
	{
		const CPartTableInfo *ppti = pimi.Pt();
		if (fConsumersOnly && EpimConsumer != ppti->Epim())
		{
			continue;
		}

		pdrgpul->Append(GPOS_NEW(pmp) ULONG(ppti->UlScanId()));
	}

	return pdrgpul;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::FSubset
//
//	@doc:
//		Check if the current part index map is a subset of the given one
//
//---------------------------------------------------------------------------
BOOL
CPartIndexMap::FSubset
	(
	const CPartIndexMap *ppim
	)
	const
{
	GPOS_ASSERT(NULL != ppim);

	if (m_pim->UlEntries() > ppim->m_pim->UlEntries())
	{
		return false;
	}

	// iterate on first map and lookup entries in second map
	PartIndexMapIter pimi(m_pim);
	while (pimi.FAdvance())
	{
		const CPartTableInfo *pptiFst = pimi.Pt();
		ULONG ulScanId = pptiFst->UlScanId();

		// lookup entry in the given part index map
		CPartTableInfo *pptiSnd = ppim->PptiLookup(ulScanId);
		
		if (NULL == pptiSnd)			
		{
			// entry does not exist in second map
			return false;
		}
		
		if (pptiFst->Epim() != pptiSnd->Epim() ||
			pptiFst->UlExpectedPropagators() != pptiSnd->UlExpectedPropagators())
		{
			return false;
		}		
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::UlHash
//
//	@doc:
//		Hash of components
//
//---------------------------------------------------------------------------
ULONG
CPartIndexMap::UlHash() const
{
	ULONG ulHash = 0;
	
	// how many scan ids to use for hash computation
	ULONG ulMaxScanIds = 5;
	ULONG ul = 0;
	
	// hash elements in partition map
	PartIndexMapIter pimi(m_pim);
	while (pimi.FAdvance() && ul < ulMaxScanIds)
	{
		ULONG ulScanId = (pimi.Pt())->UlScanId();
		ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHash<ULONG>(&ulScanId));
		ul++;
	}
	
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::FContainsRedundantPartitionSelectors
//
//	@doc:
//		Check if the given expression derives unneccessary partition selectors
//
//---------------------------------------------------------------------------
BOOL
CPartIndexMap::FContainsRedundantPartitionSelectors
	(
	CPartIndexMap *ppimReqd
	)
	const
{
	// check that there are no unneeded propagators
	PartIndexMapIter pimiDrvd(m_pim);
	while (pimiDrvd.FAdvance())
	{
		// check if there is a derived propagator that does not appear in the requirements
		if (EpimPropagator == (pimiDrvd.Pt())->Epim() &&
			(NULL == ppimReqd || !ppimReqd->FContains(pimiDrvd.Pt()->UlScanId())))
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::FSatisfies
//
//	@doc:
//		Check if part index map satisfies required partition propagation spec
//
//---------------------------------------------------------------------------
BOOL
CPartIndexMap::FSatisfies
	(
	const CPartitionPropagationSpec *ppps
	) 
	const
{
	GPOS_ASSERT(NULL != ppps);
	
	CPartIndexMap *ppimReqd = ppps->Ppim();
	if (NULL == ppimReqd || !ppimReqd->FContainsUnresolved())
	{
		// no reqiurements specified
		return true;
	}
		
	// check if all required entries are satisfied
	PartIndexMapIter pimiReqd(ppimReqd->m_pim);
	while (pimiReqd.FAdvance())
	{
		const CPartTableInfo *pptiReqd = pimiReqd.Pt();
		CPartTableInfo *ppti = PptiLookup(pptiReqd->UlScanId());

		if (NULL != ppti && !FSatisfiesEntry(pptiReqd, ppti))
		{
			return false;
		}
	}
	
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::FSatisfiesEntry
//
//	@doc:
//		Check if part index map entry satisfies the corresponding required
//		partition propagation spec entry
//
//---------------------------------------------------------------------------
BOOL
CPartIndexMap::FSatisfiesEntry
	(
	const CPartTableInfo *pptiReqd,
	CPartTableInfo *pptiDrvd
	)
	const
{
	GPOS_ASSERT(NULL != pptiReqd);
	GPOS_ASSERT(NULL != pptiDrvd);
	GPOS_ASSERT(EpimConsumer == pptiReqd->Epim());

	if (EpimResolver == pptiDrvd->Epim() || EpimPropagator == pptiDrvd->Epim())
	{
		return true;
	}

	ULONG ulExpectedPropagators = pptiDrvd->UlExpectedPropagators();
	return gpos::ulong_max == ulExpectedPropagators ||
		   (0 != ulExpectedPropagators &&
			ulExpectedPropagators == pptiReqd->UlExpectedPropagators());
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::AddRequiredPartPropagation
//
//	@doc:
//		Get part consumer with given scanId from the given map, and add it to the
//		current map with the given array of keys
//
//---------------------------------------------------------------------------
void
CPartIndexMap::AddRequiredPartPropagation
	(
	CPartIndexMap *ppimSource,
	ULONG ulScanId,
	EPartPropagationRequestAction eppra,
	DrgPpartkeys *pdrgppartkeys
	)
{
	GPOS_ASSERT(NULL != ppimSource);
	GPOS_ASSERT(EppraSentinel > eppra);

	CPartTableInfo *ppti = ppimSource->PptiLookup(ulScanId);
	GPOS_ASSERT(NULL != ppti);

	ppti->Pmdid()->AddRef();
	ppti->Ppartcnstrmap()->AddRef();
	ppti->PpartcnstrRel()->AddRef();
	if (NULL == pdrgppartkeys)
	{
		// use the keys from the given part table info entry
		pdrgppartkeys = ppti->Pdrgppartkeys();
		pdrgppartkeys->AddRef();
	}

	ULONG ulExpectedPropagators = ppti->UlExpectedPropagators();
	if (EppraIncrementPropagators == eppra)
	{
		ulExpectedPropagators++;
	}
	else if (EppraZeroPropagators == eppra)
	{
		ulExpectedPropagators = 0;
	}

	this->Insert(ppti->UlScanId(), ppti->Ppartcnstrmap(), ppti->Epim(), ulExpectedPropagators, ppti->Pmdid(), pdrgppartkeys, ppti->PpartcnstrRel());
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::PpimPartitionSelector
//
//	@doc:
//		Return a new part index map for a partition selector with the given
//		scan id, and the given number of expected selectors above it
//
//---------------------------------------------------------------------------
CPartIndexMap *
CPartIndexMap::PpimPartitionSelector
	(
	IMemoryPool *pmp,
	ULONG ulScanId,
	ULONG ulExpectedFromReq
	)
	const
{
	CPartIndexMap *ppimResult = GPOS_NEW(pmp) CPartIndexMap(pmp);

	PartIndexMapIter pimi(m_pim);
	while (pimi.FAdvance())
	{
		const CPartTableInfo *ppti = pimi.Pt();
		PartCnstrMap *ppartcnstrmap = ppti->Ppartcnstrmap();
		IMDId *pmdid = ppti->Pmdid();
		DrgPpartkeys *pdrgppartkeys = ppti->Pdrgppartkeys();
		CPartConstraint *ppartcnstrRel = ppti->PpartcnstrRel();
		ppartcnstrmap->AddRef();
		pmdid->AddRef();
		pdrgppartkeys->AddRef();
		ppartcnstrRel->AddRef();

		EPartIndexManipulator epim = ppti->Epim();
		ULONG ulExpectedPropagators = ppti->UlExpectedPropagators();
		if (ppti->UlScanId() == ulScanId)
		{
			if (0 == ulExpectedFromReq)
			{
				// this are no other expected partition selectors
				// so this scan id is resolved
				epim = EpimResolver;
				ulExpectedPropagators = 0;
			}
			else
			{
				// this is not resolved yet
				epim = EpimConsumer;
				ulExpectedPropagators = ulExpectedFromReq;
			}
		}

		ppimResult->Insert(ppti->UlScanId(), ppartcnstrmap, epim, ulExpectedPropagators, pmdid, pdrgppartkeys, ppartcnstrRel);
	}

	return ppimResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CPartIndexMap::OsPrint
	(
	IOstream &os
	)
	const
{
	PartIndexMapIter pimi(m_pim);
	while (pimi.FAdvance())
	{
		const CPartTableInfo *ppti = pimi.Pt();
		os << "("
			<< CPartTableInfo::SzManipulatorType(ppti->Epim())
			<<"<" << ppti->UlScanId() << ">("
			<< ppti->UlExpectedPropagators( ) << "), partial scans: <";
		
		CPartIndexMap::OsPrintPartCnstrMap(ppti->UlScanId(), ppti->Ppartcnstrmap(), os);

		os << ">), unresolved: (" << m_ulUnresolved << ", " << m_ulUnresolvedZeroPropagators << "), ";
	}

	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartIndexMap::OsPrintPartCnstrMap
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CPartIndexMap::OsPrintPartCnstrMap
	(
	ULONG ulScanId,
	PartCnstrMap *ppartcnstrmap,
	IOstream &os
	)
{

	if (NULL == ppartcnstrmap)
	{
		return os;
	}
	
	PartCnstrMapIter pcmi(ppartcnstrmap);
	BOOL fFirstElem = true;
	
	while (pcmi.FAdvance())
	{
		if (!fFirstElem)
		{
			os << ", ";
		}
		else
		{
			fFirstElem = false;
		}
		
		ULONG ulKey = *(pcmi.Pk());
		
		os << ulScanId << "." << ulKey << " -> ";
		pcmi.Pt()->OsPrint(os);
	}

	return os;
}

#ifdef GPOS_DEBUG
void
CPartIndexMap::DbgPrint() const
{
	CAutoTrace at(m_pmp);
	(void) this->OsPrint(at.Os());
}
#endif // GPOS_DEBUG

namespace gpopt {

  IOstream &operator << (IOstream &os, CPartIndexMap &pim)
  {
    return pim.OsPrint(os);
  }

}

// EOF

