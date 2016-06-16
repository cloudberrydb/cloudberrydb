//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CSyncHashtable.inl
//
//	@doc:
//		Inlined function implementation of hashtable
//---------------------------------------------------------------------------
#ifndef GPOS_CSyncHashtable_INL
#define GPOS_CSyncHashtable_INL

#include "gpos/base.h"

#include "gpos/common/CAutoRg.h"
#include "gpos/common/CList.h"
#include "gpos/sync/CAutoSpinlock.h"
#include "gpos/task/CAutoSuspendAbort.h"

namespace gpos
{

	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtable::Init
	//
	//	@doc:
	//		Initialization of hashtable structures
	//
	//---------------------------------------------------------------------------
	template <class T, class K, class S>
	void 
	CSyncHashtable<T, K, S>::Init
		(
		IMemoryPool *pmp,
		ULONG cSize,
		ULONG cLinkOffset,
		ULONG cKeyOffset,
		const K *pkeyInvalid,
		ULONG (*pfuncHash)(const K&),
		BOOL (*pfuncEqual)(const K&, const K&)
		)
	{
		GPOS_ASSERT(NULL == m_rgbucket);
		GPOS_ASSERT(0 == m_cSize);
		GPOS_ASSERT(NULL != pkeyInvalid);
		GPOS_ASSERT(NULL != pfuncHash);
		GPOS_ASSERT(NULL != pfuncEqual);

		m_cSize = cSize;
		m_cKeyOffset = cKeyOffset;
		m_pkeyInvalid = pkeyInvalid;
		m_pfuncHash = pfuncHash;
		m_pfuncEqual = pfuncEqual;
					
		m_rgbucket = GPOS_NEW_ARRAY(pmp, SBucket, m_cSize);

		// NOTE: 03/25/2008; since it's the only allocation in the
		//		constructor the protection is not needed strictly speaking;
		//		Using auto range here just for cleanliness;
		CAutoRg<SBucket> argbucket;
		argbucket = m_rgbucket;
		
		for(ULONG i = 0; i < m_cSize; i ++)
		{
			m_rgbucket[i].m_list.Init(cLinkOffset);
#ifdef GPOS_DEBUG
			// add serial number
			m_rgbucket[i].m_ulBucket = i;
#endif // GPOS_DEBUG
		}

		// unhook from protector
		argbucket.RgtReset();
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtable::~CSyncHashtable
	//
	//	@doc:
	//		Dtor
	//
	//---------------------------------------------------------------------------
	template <class T, class K, class S>
	CSyncHashtable<T, K, S>::~CSyncHashtable()
	{
		Cleanup();
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtable::Cleanup
	//
	//	@doc:
	//		Cleanup hashtable structures; can be re-init'd afterwards
	//
	//---------------------------------------------------------------------------
	template <class T, class K, class S>
	void 
	CSyncHashtable<T, K, S>::Cleanup()
	{
		GPOS_DELETE_ARRAY(m_rgbucket);
		m_rgbucket = NULL;
		
		m_cSize = 0;
	}

	
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtable::DestroyEntries
	//
	//	@doc:
	//		Iterate over all entries and call destroy function on each of them
	//
	//---------------------------------------------------------------------------
	template <class T, class K, class S>
	void
	CSyncHashtable<T, K, S>::DestroyEntries
		(
		DestroyEntryFuncPtr pfuncDestroy
		)
	{
		// need to suspend cancellation while cleaning up
		CAutoSuspendAbort asa;

		T *pt = NULL;
		CSyncHashtableIter<T, K, S> shtit(*this);

		// since removing an entry will automatically advance iter's
		// position, we need to make sure that advance iter is called
		// only when we do not have an entry to delete
		while (NULL != pt || shtit.FAdvance())
		{
			if (NULL != pt)
			{
				pfuncDestroy(pt);
			}

			{
				CSyncHashtableAccessByIter<T, K, S> shtitacc(shtit);
				if (NULL != (pt = shtitacc.Pt()))
				{
					shtitacc.Remove(pt);
				}
			}
		}

#ifdef GPOS_DEBUG		
		CSyncHashtableIter<T, K, S> shtitSnd(*this);
		GPOS_ASSERT(!shtitSnd.FAdvance());
#endif // GPOS_DEBUG		
	}
	
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtable::Insert
	//
	//	@doc:		
	//		insert function;
	//		does not prevent insertion of duplicates
	//
	//		CONSIDER: 02/03/2008; might want to add a remove for symmetry's
	//		sake: currently, remove functionality is only provided
	//		through accessor
	//
	//---------------------------------------------------------------------------
	template <class T, class K, class S>
	void 
	CSyncHashtable<T, K, S>::Insert
		(
		T *pt
		)
	{
		K &key = Key(pt);
		
		GPOS_ASSERT(FValid(key));
			
		// determine target bucket
		SBucket &bucket = Bucket(UlBucketIndex(key));
		
		// acquire auto spinlock
		CAutoSpinlock alock(bucket.m_slock);
		alock.Lock();
		
		// inserting at bucket's head is required by hashtable iteration
		bucket.m_list.Prepend(pt);
		
		// increase number of entries
		(void) UlpExchangeAdd(&m_ulpEntries, 1);
	}

}
#endif // !GPOS_CSyncHashtable_INL


// EOF
