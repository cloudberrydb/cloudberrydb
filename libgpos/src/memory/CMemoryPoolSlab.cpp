//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMemoryPoolSlab.cpp
//
//	@doc:
//		Implementation of slab allocator.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CAutoP.h"
#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/memory/CMemoryPoolSlab.h"

#define GPOS_MEM_SLAB_XL_HT_SIZE	(128)

#define GPOS_MEM_SLAB_SLOT_SIZE_S	(1024)
#define GPOS_MEM_SLAB_SLOT_SIZE_M	(16 * 1024)
#define GPOS_MEM_SLAB_SLOT_SIZE_L	(128 * 1024)

#define GPOS_MEM_SLAB_SLOTS_S		(128)
#define GPOS_MEM_SLAB_SLOTS_M		(16)
#define GPOS_MEM_SLAB_SLOTS_L		(8)
#define GPOS_MEM_SLAB_SLOTS_XL		(4)

#define GPOS_MEM_SLAB_THRES_HIGH_S	(16)
#define GPOS_MEM_SLAB_THRES_HIGH_M	(8)
#define GPOS_MEM_SLAB_THRES_HIGH_L	(4)
#define GPOS_MEM_SLAB_THRES_HIGH_XL	(4)

#define GPOS_MEM_SLAB_THRES_LOW_S	(8)
#define GPOS_MEM_SLAB_THRES_LOW_M	(4)
#define GPOS_MEM_SLAB_THRES_LOW_L	(2)
#define GPOS_MEM_SLAB_THRES_LOW_XL	(2)

using namespace gpos;

// initialization of static members
ULONG_PTR CMemoryPoolSlab::SAllocXLHeader::m_ulpIdInvalid(0);


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolSlab::CMemoryPoolSlab
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMemoryPoolSlab::CMemoryPoolSlab
	(
	IMemoryPool *pmp,
	ULLONG ullCapacity,
	BOOL fThreadSafe,
	BOOL fOwnsUnderlying
	)
	:
	CMemoryPool(pmp, fOwnsUnderlying, fThreadSafe),
	m_pshtAllocXL(NULL),
	m_ulpReserved(0),
	m_ullCapacity(ullCapacity)
{
	GPOS_ASSERT(NULL != pmp);

	// hash table of XL allocations
	CAutoP<AllocXLHashTable> a_pshtChunksXL;
	a_pshtChunksXL = GPOS_NEW(PmpUnderlying()) AllocXLHashTable();
	a_pshtChunksXL.Pt()->Init
		(
		PmpUnderlying(),
		GPOS_MEM_SLAB_XL_HT_SIZE,
		GPOS_OFFSET(SAllocXLHeader, m_link),
		GPOS_OFFSET(SAllocXLHeader, m_ulpId),
		&SAllocXLHeader::m_ulpIdInvalid,
		UlHashUlp,
		FEqualUlp
		);

	InitCaches();

	m_pshtAllocXL = a_pshtChunksXL.PtReset();
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolSlab::InitCaches
//
//	@doc:
//		Initialize slab caches
//
//---------------------------------------------------------------------------
void
CMemoryPoolSlab::InitCaches()
{
	// guard against OOM during initialization
	CAutoP<CSlabCache> a_rgsc[GPOS_ARRAY_SIZE(m_rgrgpsc)][GPOS_ARRAY_SIZE(m_rgrgpsc[0])];
	for (ULONG ulGroup = 0; ulGroup < GPOS_ARRAY_SIZE(a_rgsc); ulGroup++)
	{
		ULONG ulChunkSize = ULONG(1) << (ulGroup + 5);
		ULONG ulChunks = GPOS_MEM_SLAB_SLOTS_XL;
		ULONG ulThresholdHigh = GPOS_MEM_SLAB_THRES_HIGH_XL;
		ULONG ulThresholdLow = GPOS_MEM_SLAB_THRES_LOW_XL;

		if (GPOS_MEM_SLAB_SLOT_SIZE_S > ulChunkSize)
		{
			ulChunks = GPOS_MEM_SLAB_SLOTS_S;
			ulThresholdHigh = GPOS_MEM_SLAB_THRES_HIGH_S;
			ulThresholdLow = GPOS_MEM_SLAB_THRES_LOW_S;
		}
		else if (GPOS_MEM_SLAB_SLOT_SIZE_M > ulChunkSize)
		{
			ulChunks = GPOS_MEM_SLAB_SLOTS_M;
			ulThresholdHigh = GPOS_MEM_SLAB_THRES_HIGH_M;
			ulThresholdLow = GPOS_MEM_SLAB_THRES_LOW_M;
		}
		else if (GPOS_MEM_SLAB_SLOT_SIZE_L > ulChunkSize)
		{
			ulChunks = GPOS_MEM_SLAB_SLOTS_L;
			ulThresholdHigh = GPOS_MEM_SLAB_THRES_HIGH_L;
			ulThresholdLow = GPOS_MEM_SLAB_THRES_LOW_L;
		}

		for (ULONG ulIdx = 0; ulIdx < GPOS_ARRAY_SIZE(a_rgsc[ulGroup]); ulIdx++)
		{
			a_rgsc[ulGroup][ulIdx] = GPOS_NEW(PmpUnderlying()) CSlabCache
				(
				PmpUnderlying(),
				ulChunkSize,
				ulChunks,
				ulThresholdHigh,
				ulThresholdLow,
				&m_ulpReserved,
				m_ullCapacity,
				FThreadSafe()
				);
		}
	}

	for (ULONG ulGroup = 0; ulGroup < GPOS_ARRAY_SIZE(a_rgsc); ulGroup++)
	{
		for (ULONG ulIdx = 0; ulIdx < GPOS_ARRAY_SIZE(a_rgsc[ulGroup]); ulIdx++)
		{
			m_rgrgpsc[ulGroup][ulIdx] = a_rgsc[ulGroup][ulIdx].PtReset();
		}

		m_rgulpCacheIdx[ulGroup] = 0;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolSlab::~CMemoryPoolSlab
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMemoryPoolSlab::~CMemoryPoolSlab()
{}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolSlab::PvAllocate
//
//	@doc:
//		Allocate memory
//
//---------------------------------------------------------------------------
void *
CMemoryPoolSlab::PvAllocate
	(
	ULONG ulBytes,
	const CHAR *szFile,
	const ULONG ulLine
	)
{
	CSlabCache *psl = Psc(ulBytes);

	if (NULL != psl)
	{
		// slab allocation
		GPOS_ASSERT(ulBytes <= psl->UlSlotSize());

		return psl->PvAllocate();
	}

	// XL allocation
	return PvAllocateXL(ulBytes, szFile, ulLine);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolSlab::Free
//
//	@doc:
//		Free memory.
//
//---------------------------------------------------------------------------
void
CMemoryPoolSlab::Free
	(
	void *pv
	)
{
#ifdef GPOS_DEBUG
	CheckAllocation(pv);
#endif // GPOS_DEBUG

	if (FSlabAlloc(pv))
	{
		// slab allocation
		CSlab *pslab = CSlab::PslabConvert(pv);
		pslab->Free(pv);
	}
	else
	{
		// XL allocation
		FreeXL(SAllocXLHeader::Pallocxl(pv));
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolSlab::TearDown
//
//	@doc:
//		Return all used memory to the underlying pool and tear it down
//
//---------------------------------------------------------------------------
void
CMemoryPoolSlab::TearDown()
{
	// release slabs in caches
	for (ULONG ulGroup = 0; ulGroup < GPOS_ARRAY_SIZE(m_rgrgpsc); ulGroup++)
	{
		for (ULONG ulIdx = 0; ulIdx < GPOS_ARRAY_SIZE(m_rgrgpsc[ulGroup]); ulIdx++)
		{
			m_rgrgpsc[ulGroup][ulIdx]->Teardown();
			GPOS_DELETE(m_rgrgpsc[ulGroup][ulIdx]);
		}
	}

	// release XL allocations
	SAllocXLHeader *pallocxl = NULL;
	AllocXLIter shtit(*m_pshtAllocXL);
	while (NULL != pallocxl || shtit.FAdvance())
	{
		if (NULL != pallocxl)
		{
			PmpUnderlying()->Free(pallocxl);
		}

		AllocXLIterAccessor shtitacc(shtit);
		pallocxl = shtitacc.Pt();
		if (NULL != pallocxl)
		{
			shtitacc.Remove(pallocxl);
		}
	}

	// release hash table
	GPOS_DELETE(m_pshtAllocXL);

	CMemoryPool::TearDown();
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolSlab::Psc
//
//	@doc:
//		Find cache corresponding to allocation size
//
//---------------------------------------------------------------------------
CSlabCache *
CMemoryPoolSlab::Psc
	(
	ULONG ulAlloc
	)
{
	if (ulAlloc > m_rgrgpsc[GPOS_ARRAY_SIZE(m_rgrgpsc) - 1][0]->UlSlotSize())
	{
		// XL allocation, don't use slabs
		return NULL;
	}

	ULONG ulLow = 0;
	ULONG ulHigh = GPOS_ARRAY_SIZE(m_rgrgpsc) - 1;
	ULONG ulGroup = ULONG_MAX;

	// binary search on slot size per cache
	while (ulLow + 1 < ulHigh)
	{
		const ULONG ulMid = (ulLow + ulHigh) / 2;
		const ULONG ulMidChunkSize = m_rgrgpsc[ulMid][0]->UlSlotSize();

		if (ulAlloc == ulMidChunkSize)
		{
			ulGroup = ulMid;
			break;
		}
		if (ulAlloc > ulMidChunkSize)
		{
			ulLow = ulMid;
		}
		else
		{
			ulHigh = ulMid;
		}
	}

	if (ULONG_MAX == ulGroup)
	{
		if (ulAlloc <= m_rgrgpsc[ulLow][0]->UlSlotSize())
		{
			ulGroup = ulLow;
		}
		else
		{
			GPOS_ASSERT(ulAlloc <= m_rgrgpsc[ulHigh][0]->UlSlotSize());
			ulGroup = ulHigh;
		}
	}

	ULONG ulIdx =
			UlpExchangeAdd(&m_rgulpCacheIdx[ulGroup], 1) &
			(GPOS_MEM_SLAB_CACHES_PER_GROUP - 1);

	return m_rgrgpsc[ulGroup][ulIdx];
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolSlab::PvAllocateXL
//
//	@doc:
//		Allocate XL object
//
//---------------------------------------------------------------------------
void *
CMemoryPoolSlab::PvAllocateXL
	(
	const ULONG ulBytes,
	const CHAR *szFile,
	const ULONG ulLine
	)
{
	const ULONG ulAllocSize =
		GPOS_MEM_ALIGNED_SIZE(GPOS_SIZEOF(SAllocXLHeader)) +
		GPOS_MEM_ALIGNED_SIZE(ulBytes);

	if (FSetCapacity() && !FReserveMem(ulAllocSize))
	{
		return NULL;
	}

	void *pvAlloc = PmpUnderlying()->PvAllocate(ulAllocSize, szFile, ulLine);
	if (NULL == pvAlloc)
	{
		if (FSetCapacity())
		{
			UnreserveMem(ulAllocSize);
		}
		return NULL;
	}

	SAllocXLHeader *pallocxl = static_cast<SAllocXLHeader*>(pvAlloc);
	pallocxl->m_ulpId = (ULONG_PTR) pallocxl;
	pallocxl->m_ulAllocSize = ulAllocSize;
	pallocxl->ulpSlabAlloc = CSlab::EatXLAlloc;

	// scope for accessor
	{
		AllocXLKeyAccessor shtacc(*m_pshtAllocXL, pallocxl->m_ulpId);
		shtacc.Insert(pallocxl);
	}

	// return pointer after header (allocation body)
	return pallocxl + 1;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolSlab::FreeXL
//
//	@doc:
//		Release XL object
//
//---------------------------------------------------------------------------
void
CMemoryPoolSlab::FreeXL
	(
	SAllocXLHeader *pallocxl
	)
{
	if (FSetCapacity())
	{
		UnreserveMem(pallocxl->m_ulAllocSize);
	}

	// scope for accessor
	{
		AllocXLKeyAccessor shtacc(*m_pshtAllocXL, pallocxl->m_ulpId);
		shtacc.Remove(pallocxl);
	}

	PmpUnderlying()->Free(pallocxl);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolSlab::FReserveMem
//
//	@doc:
//		Update reserved memory with allocation request
//
//---------------------------------------------------------------------------
BOOL
CMemoryPoolSlab::FReserveMem
	(
	ULONG ulAlloc
	)
{
	if (m_ulpReserved + ulAlloc <= m_ullCapacity)
	{
		ULONG_PTR ulpOld = UlpExchangeAdd(&m_ulpReserved, ulAlloc);
		if (ulpOld + ulAlloc <= m_ullCapacity)
		{
			return true;
		}

		UnreserveMem(ulAlloc);
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolSlab::UnreserveMem
//
//	@doc:
//		Update reserved memory with release request
//
//---------------------------------------------------------------------------
void
CMemoryPoolSlab::UnreserveMem
	(
	ULONG ulAlloc
	)
{
	(void) UlpExchangeAdd(&m_ulpReserved, -ulAlloc);
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolSlab::CheckAllocation
//
//	@doc:
//		Verify that an allocation is correct and came from this pool
//
//---------------------------------------------------------------------------
void
CMemoryPoolSlab::CheckAllocation
	(
	void *pv
	)
{
	if (FSlabAlloc(pv))
	{
		CSlab *pslab = CSlab::PslabConvert(pv);

		BOOL fFound = false;
		for (ULONG ulGroup = 0; ulGroup < GPOS_ARRAY_SIZE(m_rgrgpsc); ulGroup++)
		{
			for (ULONG ulIdx = 0; ulIdx < GPOS_ARRAY_SIZE(m_rgrgpsc[ulIdx]); ulIdx++)
			{
				if (pslab->Psc() == m_rgrgpsc[ulGroup][ulIdx])
				{
					GPOS_ASSERT_IMP(m_rgrgpsc[ulGroup][ulIdx]->FOwns(pslab), !fFound);
					fFound = true;
				}
			}
		}

		GPOS_ASSERT(fFound && "Allocation does not belong to any cache");
	}
	else
	{
		SAllocXLHeader *pallocxl = static_cast<SAllocXLHeader*>(pv) - 1;
		AllocXLKeyAccessor shtacc(*m_pshtAllocXL, pallocxl->m_ulpId);

		GPOS_ASSERT(NULL != shtacc.PtLookup());
	}
}

#endif // GPOS_DEBUG

// EOF
