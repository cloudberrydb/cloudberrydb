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

namespace gpos
{

	//prototypes
	class CCache;

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
			static CCacheFactory* m_pcf;

			// memory pool allocated to caches
			IMemoryPool *m_pmp;

			// private ctor
			CCacheFactory(IMemoryPool *pmp);

			// no copy ctor
			CCacheFactory(const CCacheFactory&);

			// private dtor
			~CCacheFactory()
			{
				GPOS_ASSERT(NULL == m_pcf &&
							"Cache factory has not been shut down");
			}


		public:

			// type definition of key hashing and equality functions
			typedef ULONG (*HashFuncPtr)(const VOID_PTR &);
			typedef BOOL (*EqualFuncPtr)(const VOID_PTR&, const VOID_PTR&);

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
			static
			CCache *PCacheCreate
				(
				BOOL fUnique,
				ULLONG ullCacheQuota,
				HashFuncPtr pfuncHash,
				EqualFuncPtr pfuncEqual
				);

			IMemoryPool *Pmp() const;

	}; // CCacheFactory


} // namespace gpos


#endif // CCACHEFACTORY_H_

// EOF

