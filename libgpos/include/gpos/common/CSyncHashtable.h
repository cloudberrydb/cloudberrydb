//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CSyncHashtable.h
//
//	@doc:
//		Allocation-less static hashtable; 
//		Manages client objects without additional allocations; this is a
//		requirement for system programming tasks to ensure the hashtable
//		works in exception situations, e.g. OOM;
//
//		1)	Hashtable is static and cannot resize during operations;
//		2)	expects target type to have SLink (see CList.h) and Key
//			members with appopriate accessors;
//		3)	clients must provide their own hash function;
//		4)	hashtable synchronizes through spinlocks on each bucket;
//---------------------------------------------------------------------------
#ifndef GPOS_CSyncHashtable_H
#define GPOS_CSyncHashtable_H

#include "gpos/base.h"

#include "gpos/common/CAutoRg.h"
#include "gpos/common/CList.h"
#include "gpos/sync/CAutoSpinlock.h"
#include "gpos/task/CAutoSuspendAbort.h"

namespace gpos
{

	// prototypes
	template <class T, class K, class S>
	class CSyncHashtableAccessorBase;

	template <class T, class K, class S>
	class CSyncHashtableAccessByKey;

	template <class T, class K, class S>
	class CSyncHashtableIter;

	template <class T, class K, class S>
	class CSyncHashtableAccessByIter;

	//---------------------------------------------------------------------------
	//	@class:
	//		CSyncHashtable<T, K, S>
	//
	//	@doc:
	//		Allocation-less static hash table;
	//
	//		Ideally the offset of the key would be a template parameter too in order
	//		to avoid accidental tampering with this value -- not all compiler allow 
	//		the use of the offset macro in the template definition, however.
	//
	//---------------------------------------------------------------------------
	template <class T, class K, class S>
	class CSyncHashtable
	{
		// accessor and iterator classes are friends
		friend class CSyncHashtableAccessorBase<T, K, S>;
		friend class CSyncHashtableAccessByKey<T, K, S>;
		friend class CSyncHashtableAccessByIter<T, K, S>;
		friend class CSyncHashtableIter<T, K, S>;

		private:
	
			// hash bucket is a pair of list, spinlock
			struct SBucket
			{
				private:
			
					// no copy ctor
					SBucket(const SBucket &);

				public:
			
					// ctor
					SBucket() {};
				
					// spinlock to protect bucket
					S m_slock;
				
					// hash chain
					CList<T> m_list;

#ifdef GPOS_DEBUG
					// bucket number
					ULONG m_ulBucket;
#endif // GPOS_DEBUG

			};

			// range of buckets
			SBucket *m_rgbucket;
		
			// number of ht buckets
			ULONG m_cSize;

			// number of ht entries
			volatile ULONG_PTR m_ulpEntries;
		
			// offset of key
			ULONG m_cKeyOffset;

			// invalid key - needed for iteration
			const K *m_pkeyInvalid;

			// pointer to hashing function
			ULONG (*m_pfuncHash)(const K&);

			// pointer to key equality function
			BOOL (*m_pfuncEqual)(const K&, const K&);
				
			// function to compute bucket index for key
			ULONG UlBucketIndex
				(
				const K &key
				)
				const
			{
				GPOS_ASSERT(FValid(key) && "Invalid key is inaccessible");

				return m_pfuncHash(key) % m_cSize;
			}

			// function to get bucket by index
			SBucket &Bucket
				(
				const ULONG ulIndex
				)
				const
			{
				GPOS_ASSERT(ulIndex < m_cSize && "Invalid bucket index");

				return m_rgbucket[ulIndex];
			}

			// extract key out of type
			K &Key
				(
				T *pt
				)
				const
			{
				GPOS_ASSERT(ULONG_MAX != m_cKeyOffset && "Key offset not initialized.");
			
				K &k = *(K*)((BYTE*)pt + m_cKeyOffset);

				return k;
			}

			// key validity check
			BOOL FValid
				(
				const K &key
				)
				const
			{
				return !m_pfuncEqual(key, *m_pkeyInvalid);
			}

		public:
	
			// type definition of function used to cleanup element
			typedef void (*DestroyEntryFuncPtr)(T *);

			// ctor
			CSyncHashtable<T, K, S>()
				:
				m_rgbucket(NULL),
				m_cSize(0),
				m_ulpEntries(0),
				m_cKeyOffset(ULONG_MAX),
				m_pkeyInvalid(NULL)
			{}
		
			// dtor
			// deallocates hashtable internals, does not destroy
			// client objects
			~CSyncHashtable<T, K, S>()
            {
                Cleanup();
            }
		
			// Initialization of hashtable
			void Init
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

			// dealloc bucket range and reset members
			void Cleanup()
            {
                GPOS_DELETE_ARRAY(m_rgbucket);
                m_rgbucket = NULL;

                m_cSize = 0;
            }

			// iterate over all entries and call destroy function on each entry
			void DestroyEntries(DestroyEntryFuncPtr pfuncDestroy)
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

			// insert function;
			void Insert(T *pt)
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

			// return number of entries
			ULONG_PTR UlpEntries() const
			{
				return m_ulpEntries;
			}

	}; // class CSyncHashtable

}

#endif // !GPOS_CSyncHashtable_H

// EOF

