//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC CORP.
//
//	@filename:
//		CCache.h
//
//	@doc:
//		Definition of cache factory class.
//.
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef CCACHEFACTORY_H_
#define CCACHEFACTORY_H_

#include "gpos/base.h"

#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/memory/CCache.h"

// default initial value of the gclock counter during insertion of an entry
#define CCACHE_GCLOCK_INIT_COUNTER 3

using namespace gpos;

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CCacheFactory
	//
	//	@doc:
	//		Definition of cache factory;
	//
	//		This class is responsible of creating cache objects and allocating the
	//		memory pools they need
	//
	//---------------------------------------------------------------------------
	class CCacheFactory
	{
		private:

			// global instance
			static CCacheFactory *m_pcf;

			// memory pool allocated to caches
			IMemoryPool *m_pmp;

			// private ctor
			CCacheFactory(IMemoryPool *pmp);

			// no copy ctor
			CCacheFactory(const CCacheFactory&);



		public:

			// private dtor
			~CCacheFactory()
			{
				GPOS_ASSERT(NULL == m_pcf &&
							"Cache factory has not been shut down");
			}

			// initialize global memory pool
			static
			GPOS_RESULT EresInit();

			// destroy global instance
			void Shutdown();

			// global accessor
			inline
			static CCacheFactory *Pcf()
			{
				return m_pcf;
			}

			// create a cache instance
			template <class T, class K>
			static
			CCache<T, K> *PCacheCreate
				(
				BOOL fUnique,
				ULLONG ullCacheQuota,
				typename CCache<T, K>::HashFuncPtr pfuncHash,
				typename CCache<T, K>::EqualFuncPtr pfuncEqual
				)
			{
				GPOS_ASSERT(NULL != Pcf() &&
						    "Cache factory has not been initialized");

				IMemoryPool *pmp = Pcf()->Pmp();
				CCache<T, K> *pcache = GPOS_NEW(pmp) CCache<T, K>
							(
							pmp,
							fUnique,
							ullCacheQuota,
							CCACHE_GCLOCK_INIT_COUNTER,
							pfuncHash,
							pfuncEqual
							);

				return pcache;

			}

			IMemoryPool *Pmp() const;

	}; // CCacheFactory
} // namespace gpos


#endif // CCACHEFACTORY_H_

// EOF

