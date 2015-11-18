//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC CORP.
//
//	@filename:
//		CCacheAccessorBase.h
//
//	@doc:
//		Definition of cache accessor base class.
//.
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CCACHEACCESSORBASE_H_
#define GPOS_CCACHEACCESSORBASE_H_

#include "gpos/base.h"

#include "gpos/memory/CCache.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CCacheAccessorBase
	//
	//	@doc:
	//		Definition of CCacheAccessorBase;
	//
	//		The accessor holds exactly one cached object at a time.
	//
	//		The accessor's API includes four main functions:
	//			(1) Inserting a new object in the cache
	//			(2) Looking up a cached object by key
	//			(3) Deleting a cached object
	//			(4) Iterating over cached objects with the same key
	//---------------------------------------------------------------------------
	class CCacheAccessorBase
	{
		private:

			// the underlying cache
			CCache *m_pcache;

			// memory pool of a cached object inserted by the accessor
			IMemoryPool *m_pmp;

			// cached object currently held by the accessor
			CCache::CCacheEntry *m_pce;

			// true if insertion of a new object into the cache was successful
			BOOL m_fInserted;

		protected:

			// ctor; protected to disable instantiation unless from child class
			CCacheAccessorBase(CCache *pcache);

			// dtor
			~CCacheAccessorBase();

			// the following functions are hidden since they involve
			// (void *) key/value data types; the actual types are defined
			// as template parameters in the child class CCacheAccessor

			// inserts a new object into the cache
			VOID_PTR PvInsert
				(
				VOID_PTR pvKey,
				VOID_PTR pvVal
				);

			// returns the object currently held by the accessor
			VOID_PTR PvVal() const;

			// gets the next undeleted object with the same key
			VOID_PTR PvNext();

		public:

			// creates a new memory pool for allocating a new object
			IMemoryPool *Pmp();

			// finds the first object matching the given key
			void Lookup(VOID_PTR pvKey);

			// marks currently held object as deleted
			void MarkForDeletion();

	};  // CCacheAccessorBase

} //namespace gpos

#endif // GPOS_CCACHEACCESSORBASE_H_

// EOF
