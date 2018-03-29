//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC CORP.
//
//	@filename:
//		CCacheAccessor.h
//
//	@doc:
//		Definition of cache accessor base class.
//.
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CCACHEACCESSOR_H_
#define GPOS_CCACHEACCESSOR_H_

#include "gpos/base.h"

#include "gpos/memory/CCache.h"

using namespace gpos;

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CCacheAccessor
	//
	//	@doc:
	//		Definition of CCacheAccessor;
	//
	//		The accessor holds exactly one cached object at a time.
	//
	//		The accessor's API includes four main functions:
	//			(1) Inserting a new object in the cache
	//			(2) Looking up a cached object by key
	//			(3) Deleting a cached object
	//			(4) Iterating over cached objects with the same key
	//---------------------------------------------------------------------------
	template <class T, class K>
	class CCacheAccessor
	{
		private:

			// the underlying cache
			CCache<T, K> *m_pcache;

			// memory pool of a cached object inserted by the accessor
			IMemoryPool *m_pmp;

			// cached object currently held by the accessor
			typename CCache<T, K>::CCacheHashTableEntry *m_pce;

			// true if insertion of a new object into the cache was successful
			BOOL m_fInserted;

		public:

			// ctor; protected to disable instantiation unless from child class
			CCacheAccessor(CCache<T, K> *pcache)
			:
			m_pcache(pcache),
			m_pmp(NULL),
			m_pce(NULL),
			m_fInserted(false)

			{
				GPOS_ASSERT(NULL != pcache);
			}

			// dtor
			~CCacheAccessor()
			{
				// check if a memory pool was created but insertion failed
				if (NULL != m_pmp && !m_fInserted)
				{
					CMemoryPoolManager::Pmpm()->Destroy(m_pmp);
				}

				// release entry if one was created
				if (NULL != m_pce)
				{
					m_pcache->ReleaseEntry(m_pce);
				}
			}

			// the following functions are hidden since they involve
			// (void *) key/value data types; the actual types are defined
			// as template parameters in the child class CCacheAccessor

			// inserts a new object into the cache
			T PtInsert
				(
				K pKey,
				T pVal
				)
			{
				GPOS_ASSERT(NULL != m_pmp);

				GPOS_ASSERT(!m_fInserted &&
						    "Accessor was already used for insertion");

				GPOS_ASSERT(NULL == m_pce &&
						    "Accessor already holds an entry");

				CCacheEntry<T, K> *pce =
						GPOS_NEW(m_pcache->m_pmp) CCacheEntry<T, K>(m_pmp, pKey, pVal, m_pcache->m_ulGClockInitCounter);

				CCacheEntry<T, K> *pceReturn = m_pcache->PceInsert(pce);

				// check if insertion completed successfully
				if (pce == pceReturn)
				{
					m_fInserted = true;
				}
				else
				{
					GPOS_DELETE(pce);
				}

				// accessor holds the returned entry in all cases
				m_pce = pceReturn;

				return pceReturn->PVal();

			}

			// returns the object currently held by the accessor
			T PtVal() const
			{
				T pReturn = NULL;
				if (NULL != m_pce)
				{
					pReturn = m_pce->PVal();
				}

				return pReturn;
			}

			// gets the next undeleted object with the same key
			T PtNext()
			{
				GPOS_ASSERT(NULL != m_pce);

				typename CCache<T, K>::CCacheHashTableEntry *pce = m_pce;
				m_pce = m_pcache->PceNext(m_pce);

				// release previous entry
				m_pcache->ReleaseEntry(pce);

				return PtVal();
			}

		public:

			// creates a new memory pool for allocating a new object
			IMemoryPool *Pmp()
			{
				GPOS_ASSERT(NULL == m_pmp);

				// construct a memory pool for cache entry
                m_pmp = CMemoryPoolManager::Pmpm()->PmpCreate(
                    CMemoryPoolManager::EatTracker, true /*fThreadSafe*/,
                    gpos::ullong_max);

				return m_pmp;
			}

			// finds the first object matching the given key
			void Lookup(K pKey)
			{
				GPOS_ASSERT(NULL == m_pce && "Accessor already holds an entry");

				m_pce = m_pcache->PceLookup(pKey);

				if(NULL != m_pce)
				{
					// increase ref count before return the object to the customer
					// customer is responsible for decreasing the ref count after use
					m_pce->IncRefCount();
				}
			}

			// marks currently held object as deleted
			void MarkForDeletion()
			{
				GPOS_ASSERT(NULL != m_pce);

				m_pce->MarkForDeletion();
			}

	};  // CCacheAccessor

} //namespace gpos

#endif // GPOS_CCACHEACCESSOR_H_

// EOF
