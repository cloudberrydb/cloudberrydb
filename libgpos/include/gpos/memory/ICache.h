//---------------------------------------------------------------------------
//	Greenplum Database 
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		ICache.h
//
//	@doc:
//		Interface for cache implementations.
//
//	@owner: 
//		cwhipkey
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_ICache_H
#define GPOS_ICache_H

#include "gpos/base.h"

namespace gpos
{
    typedef ULONG (*PFnHashCacheKey)(void *pvKey);
    typedef BOOL (*PFnEqualsCacheKey)(void *pvLeftKey, void *pvRightKey);

    class ICacheEntry
    {
        protected:
            ICacheEntry() {}

        public:
            virtual
            ~ICacheEntry() {}

            /**
             * Get the assigned value
             */
            virtual
            void *PvValue() = 0;

            /**
             * Get the memory pool for this cache value.
             */
            virtual
            IMemoryPool *Pmp() = 0;
    };

	class ICache
	{
        protected:
            ICache() {}

		public:
			virtual
			~ICache() {}

            //
            // Get a handle to the value for the given key, incrementing the pin count on the value.
            //
            // Returns NULL if the key is not in the cache
            //
	        virtual
	        ICacheEntry *PceLookup(void *pvKey) = 0;

            //
            // Insert the given cache value.
            //
            // pvKey and pvValue should be allocated in the memory pool of the pcv object
            //
            // Return true if the object was successfully inserted, false otherwise.
            //
            virtual
            BOOL FInsert( ICacheEntry *pcv, void *pvKey, void *pvValue) = 0;

            //
            // Create a new cache value that can be later passed to FInsert
            //
            // Note that even if FInsert fails, or FInsert is never called, the returned value
            //  must be released with a call to Release.  Calling PceCreateEntry returns a value
            //  with a read lock of 1.
            //
            // Returns NULL if the cache value cannot be created because the cache is full or
            //   no memory is available
            //
            virtual
            ICacheEntry *PceCreateEntry() = 0;

            //
            // Decrement the pin counter, potentially triggering the delete of this cache value
            //
            virtual
            void Release(ICacheEntry *pcv) = 0;

            //
            // Remove the given key from the cache.
            //
            // Callers who still hold valid ICacheEntry handles will not be affected.
            //
            virtual
            void Delete(void *pvKey) = 0;
	};
}

#endif // !GPOS_ICache_H

// EOF

