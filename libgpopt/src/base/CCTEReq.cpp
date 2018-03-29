//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CCTEReq.cpp
//
//	@doc:
//		Implementation of CTE requirements
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CCTEReq.h"
#include "gpopt/base/COptCtxt.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CCTEReq::CCTEReqEntry::CCTEReqEntry
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CCTEReq::CCTEReqEntry::CCTEReqEntry
	(
	ULONG ulId,
	CCTEMap::ECteType ect,
	BOOL fRequired,
	CDrvdPropPlan *pdpplan
	)
	:
	m_ulId(ulId),
	m_ect(ect),
	m_fRequired(fRequired),
	m_pdpplan(pdpplan)
{
	GPOS_ASSERT(CCTEMap::EctSentinel > ect);
	GPOS_ASSERT_IMP(NULL == pdpplan, CCTEMap::EctProducer == ect);
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEReq::CCTEReqEntry::~CCTEReqEntry
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CCTEReq::CCTEReqEntry::~CCTEReqEntry()
{
	CRefCount::SafeRelease(m_pdpplan);
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEReq::CCTEReqEntry::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CCTEReq::CCTEReqEntry::UlHash() const
{
	ULONG ulHash = gpos::UlCombineHashes(gpos::UlHash<ULONG>(&m_ulId), gpos::UlHash<CCTEMap::ECteType>(&m_ect));
	ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHash<BOOL>(&m_fRequired));

	if (NULL != m_pdpplan)
	{
		ulHash = gpos::UlCombineHashes(ulHash, m_pdpplan->UlHash());
	}

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEReq::CCTEReqEntry::FEqual
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CCTEReq::CCTEReqEntry::FEqual
	(
	CCTEReqEntry *pcre
	)
	const
{
	GPOS_ASSERT(NULL != pcre);
	if (m_ulId != pcre->UlId() ||
		m_ect != pcre->Ect() ||
		m_fRequired != pcre->FRequired())
	{
		return false;
	}

	CDrvdPropPlan *pdpplan = pcre->PdpplanProducer();
	if (NULL == m_pdpplan && NULL == pdpplan)
	{
		return true;
	}

	if (NULL != m_pdpplan && NULL != pdpplan)
	{
		return m_pdpplan->FEqual(pdpplan);
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEReq::CCTEReqEntry::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CCTEReq::CCTEReqEntry::OsPrint
	(
	IOstream &os
	)
	const
{
	os << m_ulId
		<< (CCTEMap::EctProducer == m_ect ? "p" : "c")
		<< (m_fRequired ? "" : "(opt)");

	if (NULL != m_pdpplan)
	{
		os << "Plan Props: " << *m_pdpplan;
	}
	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEReq::CCTEReq
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CCTEReq::CCTEReq
	(
	IMemoryPool *pmp
	)
	:
	m_pmp(pmp),
	m_phmcter(NULL),
	m_pdrgpulRequired(NULL)
{
	GPOS_ASSERT(NULL != pmp);

	m_phmcter = GPOS_NEW(m_pmp) HMCteReq(m_pmp);
	m_pdrgpulRequired = GPOS_NEW(m_pmp) DrgPul(m_pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEReq::~CCTEReq
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CCTEReq::~CCTEReq()
{
	m_phmcter->Release();
	m_pdrgpulRequired->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEReq::Insert
//
//	@doc:
//		Insert a new map entry. No entry with the same id can already exist
//
//---------------------------------------------------------------------------
void
CCTEReq::Insert
	(
	ULONG ulCteId,
	CCTEMap::ECteType ect,
	BOOL fRequired,
	CDrvdPropPlan *pdpplan
	)
{
	GPOS_ASSERT(CCTEMap::EctSentinel > ect);
	CCTEReqEntry *pcre = GPOS_NEW(m_pmp) CCTEReqEntry(ulCteId, ect, fRequired, pdpplan);
#ifdef GPOS_DEBUG
	BOOL fSuccess =
#endif // GPOS_DEBUG
	m_phmcter->FInsert(GPOS_NEW(m_pmp) ULONG(ulCteId), pcre);
	GPOS_ASSERT(fSuccess);
	if (fRequired)
	{
		m_pdrgpulRequired->Append(GPOS_NEW(m_pmp) ULONG(ulCteId));
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEReq::InsertConsumer
//
//	@doc:
//		Insert a new consumer entry. with the given id. The plan properties are
//		taken from the given context
//
//---------------------------------------------------------------------------
void
CCTEReq::InsertConsumer
	(
	ULONG ulId,
	DrgPdp *pdrgpdpCtxt
	)
{
	ULONG ulProducerId = gpos::ulong_max;
	CDrvdPropPlan *pdpplan = CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pcm()->PdpplanProducer(&ulProducerId);
	GPOS_ASSERT(NULL != pdpplan);
	GPOS_ASSERT(ulProducerId == ulId && "unexpected CTE producer plan properties");

	pdpplan->AddRef();
	Insert(ulId, CCTEMap::EctConsumer, true /*fRequired*/, pdpplan);
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEReq::PcreLookup
//
//	@doc:
//		Lookup info for given cte id
//
//---------------------------------------------------------------------------
CCTEReq::CCTEReqEntry *
CCTEReq::PcreLookup
	(
	ULONG ulCteId
	)
	const
{
	return m_phmcter->PtLookup(&ulCteId);
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEReq::FSubset
//
//	@doc:
//		Check if the current requirement is a subset of the given one
//
//---------------------------------------------------------------------------
BOOL
CCTEReq::FSubset
	(
	const CCTEReq *pcter
	)
	const
{
	GPOS_ASSERT(NULL != pcter);

	// compare number of entries first
	if (m_phmcter->UlEntries() > pcter->m_phmcter->UlEntries())
	{
		return false;
	}

	if (0 == m_phmcter->UlEntries())
	{
		// empty subset
		return true;
	}

	// iterate over map entries
	HMCteReqIter hmcri(m_phmcter);
	while (hmcri.FAdvance())
	{
		const CCTEReqEntry *pcre = hmcri.Pt();
		CCTEReqEntry *pcreOther = pcter->PcreLookup(pcre->UlId());
		if (NULL == pcreOther || !pcre->FEqual(pcreOther))
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEReq::FContainsRequirement
//
//	@doc:
//		Check if the given CTE is in the requirements
//
//---------------------------------------------------------------------------
BOOL
CCTEReq::FContainsRequirement
	(
	const ULONG ulId,
	const CCTEMap::ECteType ect
	)
	const
{
	CCTEReqEntry *pcre = PcreLookup(ulId);
	return (NULL != pcre && pcre->Ect() == ect);
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEReq::Ect
//
//	@doc:
//		Return the CTE type associated with the given ID in the requirements
//
//---------------------------------------------------------------------------
CCTEMap::ECteType
CCTEReq::Ect
	(
	const ULONG ulId
	)
	const
{
	CCTEReqEntry *pcre = PcreLookup(ulId);
	GPOS_ASSERT(NULL != pcre);
	return pcre->Ect();
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEReq::UlHash
//
//	@doc:
//		Hash of components
//
//---------------------------------------------------------------------------
ULONG
CCTEReq::UlHash() const
{
	ULONG ulHash = 0;

	// how many map entries to use for hash computation
	const ULONG ulMaxEntries = 5;
	ULONG ul = 0;

	HMCteReqIter hmcri(m_phmcter);
	while (hmcri.FAdvance() && ul < ulMaxEntries)
	{
		const CCTEReqEntry *pcre = hmcri.Pt();
		ulHash = gpos::UlCombineHashes(ulHash, pcre->UlHash());
		ul++;
	}

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEReq::PcterUnresolved
//
//	@doc:
//		Unresolved CTE requirements given a derived CTE map
//
//---------------------------------------------------------------------------
CCTEReq *
CCTEReq::PcterUnresolved
	(
	IMemoryPool *pmp,
	CCTEMap *pcm
	)
{
	GPOS_ASSERT(NULL != pcm);
	CCTEReq *pcterUnresolved = GPOS_NEW(pmp) CCTEReq(pmp);

	HMCteReqIter hmcri(m_phmcter);
	while (hmcri.FAdvance())
	{
		// if a cte is marked as required and it is not found in the given map
		// then keep it as required, else make it optional
		const CCTEReqEntry *pcre = hmcri.Pt();
		ULONG ulId = pcre->UlId();
		BOOL fRequired = pcre->FRequired() && CCTEMap::EctSentinel == pcm->Ect(ulId);
		CDrvdPropPlan *pdpplan = pcre->PdpplanProducer();
		if (NULL != pdpplan)
		{
			pdpplan->AddRef();
		}

		pcterUnresolved->Insert(ulId, pcre->Ect(), fRequired, pdpplan);
	}

	return pcterUnresolved;
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEReq::PcterUnresolvedSequence
//
//	@doc:
//		Unresolved CTE requirements given a derived CTE map for a sequence
//		operator
//
//---------------------------------------------------------------------------
CCTEReq *
CCTEReq::PcterUnresolvedSequence
	(
	IMemoryPool *pmp,
	CCTEMap *pcm,
	DrgPdp *pdrgpdpCtxt // context contains derived plan properties of producer tree
	)
{
	GPOS_ASSERT(NULL != pcm);
	CCTEReq *pcterUnresolved = GPOS_NEW(pmp) CCTEReq(pmp);

	HMCteReqIter hmcri(m_phmcter);
	while (hmcri.FAdvance())
	{
		const CCTEReqEntry *pcre = hmcri.Pt();

		ULONG ulId = pcre->UlId();
		CCTEMap::ECteType ect = pcre->Ect();
		BOOL fRequired = pcre->FRequired();

		CCTEMap::ECteType ectDrvd = pcm->Ect(ulId);
		if (fRequired && CCTEMap::EctSentinel != ectDrvd)
		{
			GPOS_ASSERT(CCTEMap::EctConsumer == ect);
			GPOS_ASSERT(CCTEMap::EctConsumer == ectDrvd);
			// already found, so mark it as optional
			CDrvdPropPlan *pdpplan = pcre->PdpplanProducer();
			GPOS_ASSERT(NULL != pdpplan);
			pdpplan->AddRef();
			pcterUnresolved->Insert(ulId, ect, false /*fReqiored*/, pdpplan);
		}
		else if (!fRequired && CCTEMap::EctProducer == ect && CCTEMap::EctSentinel != ectDrvd)
		{
			GPOS_ASSERT(CCTEMap::EctProducer == ectDrvd);

			// found a producer. require the corresponding consumer and
			// extract producer plan properties from passed context
			pcterUnresolved->InsertConsumer(ulId, pdrgpdpCtxt);
		}
		else
		{
			// either required and not found yet, or optional
			// in both cases, pass it down as is
			CDrvdPropPlan *pdpplan = pcre->PdpplanProducer();
			GPOS_ASSERT_IMP(NULL == pdpplan, CCTEMap::EctProducer == ect);
			if (NULL != pdpplan)
			{
				pdpplan->AddRef();
			}
			pcterUnresolved->Insert(ulId, ect, fRequired, pdpplan);
		}
	}

	// if something is in pcm and not in the requirments, it has to be a producer
	// in which case, add the corresponding consumer as unresolved
	DrgPul *pdrgpulProducers = pcm->PdrgpulAdditionalProducers(pmp, this);
	const ULONG ulLen = pdrgpulProducers->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		ULONG *pulId = (*pdrgpulProducers)[ul];
		pcterUnresolved->InsertConsumer(*pulId, pdrgpdpCtxt);
	}
	pdrgpulProducers->Release();

	return pcterUnresolved;
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEReq::PcterAllOptional
//
//	@doc:
//		Create a copy of the current requirement where all the entries are marked optional
//
//---------------------------------------------------------------------------
CCTEReq *
CCTEReq::PcterAllOptional
	(
	IMemoryPool *pmp
	)
{
	CCTEReq *pcter = GPOS_NEW(pmp) CCTEReq(pmp);

	HMCteReqIter hmcri(m_phmcter);
	while (hmcri.FAdvance())
	{
		const CCTEReqEntry *pcre = hmcri.Pt();
		CDrvdPropPlan *pdpplan = pcre->PdpplanProducer();
		if (NULL != pdpplan)
		{
			pdpplan->AddRef();
		}
		pcter->Insert(pcre->UlId(), pcre->Ect(), false /*fRequired*/, pdpplan);
	}

	return pcter;
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEReq::Pdpplan
//
//	@doc:
//		Lookup plan properties for given cte id
//
//---------------------------------------------------------------------------
CDrvdPropPlan *
CCTEReq::Pdpplan
	(
	ULONG ulCteId
	)
	const
{
	const CCTEReqEntry *pcre = PcreLookup(ulCteId);
	if (NULL != pcre)
	{
		return pcre->PdpplanProducer();
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CCTEReq::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CCTEReq::OsPrint
	(
	IOstream &os
	)
	const
{
	HMCteReqIter hmcri(m_phmcter);
	while (hmcri.FAdvance())
	{
		CCTEReqEntry *pcre = const_cast<CCTEReqEntry *>(hmcri.Pt());
		pcre->OsPrint(os);
		os << " ";
	}

	return os;
}

namespace gpopt {

  IOstream &operator << (IOstream &os, CCTEReq &cter)
  {
    return cter.OsPrint(os);
  }

}

// EOF
