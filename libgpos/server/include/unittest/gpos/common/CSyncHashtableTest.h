//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CSyncHashtableTest.h
//
//	@doc:
//      Test for CSyncHashtable
//---------------------------------------------------------------------------
#ifndef GPOS_CSyncHashtableTest_H
#define GPOS_CSyncHashtableTest_H

#include "gpos/base.h"

#include "gpos/sync/CAutoSpinlock.h"
#include "gpos/sync/CEvent.h"

#include "gpos/common/CList.h"

#include "gpos/common/CSyncHashtable.h"
#include "gpos/common/CSyncHashtableAccessByKey.h"
#include "gpos/common/CSyncHashtableIter.h"



namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CSyncHashtableTest
	//
	//	@doc:
	//		Wrapper class to avoid compiler confusion about template parameters
	//		Contains also unittests for accessor class
	//
	//---------------------------------------------------------------------------
	class CSyncHashtableTest
	{

		// prototypes
		struct SElem;

		private:

			// types used by testing functions
			typedef CSyncHashtable<SElem, ULONG, CSpinlockDummy>
				SElemHashtable;

			typedef CSyncHashtableAccessByKey<SElem, ULONG, CSpinlockDummy>
				SElemHashtableAccessor;

			typedef CSyncHashtableIter<SElem, ULONG, CSpinlockDummy>
				SElemHashtableIter;

			typedef CSyncHashtableAccessByIter<SElem, ULONG, CSpinlockDummy>
				SElemHashtableIterAccessor;

			// function pointer to a hash table task
			typedef void * (*pfuncHashtableTask)(void *);

			//---------------------------------------------------------------------------
			//	@class:
			//		SElem
			//
			//	@doc:
			//		Local class for hashtable tests;
			//
			//---------------------------------------------------------------------------
			struct SElem
			{

				private:

					// element's unique id
					ULONG m_id;

				public:

					// generic link
					SLink m_link;

					// hash key
					ULONG m_ulKey;

					// invalid key
					static
					const ULONG m_ulInvalid;

					// invalid element
					static
					const SElem m_elemInvalid;

					// simple hash function
					static ULONG HashValue
						(
						const ULONG &ul
						)
					{
						return ul;
					}

					static ULONG HashValue
						(
						const SElem &elem
						)
					{
						return elem.m_id;
					}

					// equality for object-based comparison
					BOOL operator ==
						(
						const SElem &elem
						)
						const
					{
						return elem.m_id == m_id;
					}

					// key equality function for hashtable
					static
					BOOL FEqualKeys
						(
						const ULONG &ulkey,
						const ULONG &ulkeyOther
						)
					{
						return ulkey == ulkeyOther;
					}

					// element equality function for hashtable
					static
					BOOL Equals
						(
						const SElem &elem,
						const SElem &elemOther
						)
					{
						return elem == elemOther;
					}

					// ctor
					SElem
						(
						ULONG id,
						ULONG ulKey
						)
						:
						m_id(id),
						m_ulKey(ulKey)
					{
					}
					
					// copy ctor
					SElem
						(
						const SElem &elem
						)
					{
						m_id = elem.m_id;
						m_ulKey = elem.m_ulKey;
					}

#ifdef GPOS_DEBUG
					static
					BOOL IsValid
						(
						const ULONG &ulKey
						)
					{
						return !FEqualKeys(m_ulInvalid, ulKey);
					}
#endif // GPOS_DEBUG

					// dummy ctor
					SElem()
					{
					}

					// Id accessor
					ULONG Id() const
					{
						return m_id;
					}

			}; // struct SElem


			//---------------------------------------------------------------------------
			//	@class:
			//		SIterTest
			//
			//	@doc:
			//		Local class used for passing arguments to concurrent tasks;
			//
			//---------------------------------------------------------------------------
			struct SElemTest
			{

				private:

					// hash table to operate on
					SElemHashtable &m_sht;

					// array of elements maintained by hash table
					SElem *m_rgelem;

					// event used for waiting all tasks to start
					CEvent *m_pevent;

					// number of started tasks
					ULONG m_ulStarted;


				public:

					// ctor
					SElemTest
						(
						SElemHashtable &sht,
						SElem *rgelem,
						CEvent *pevent = NULL
						)
						:
						m_sht(sht),
						m_rgelem(rgelem),
						m_pevent(pevent),
						m_ulStarted(0)
					{
					}

					// hash table accessor
					SElemHashtable &GetHashTable() const
					{
						return m_sht;
					}

					// elements array accessor
					SElem *Rgelem() const
					{
						return m_rgelem;
					}

					// event accessor
					CEvent *Pevent() const
					{
						return m_pevent;
					}

					// started tasks accessor
					ULONG UlStarted() const
					{
						return m_ulStarted;
					}

					void IncStarted()
					{
						m_ulStarted++;
					}


			}; // struct SElemTest


			// internal tasks used for testing concurrent access

			// wait for all non-started tasks to start
			static void Unittest_WaitTasks(SElemTest *);

			// inserts a subset of elements
			static void *PvUnittest_Inserter(void *);

			// looks up a subset of elements
			static void *PvUnittest_Reader(void *);

			// removes a subset of elements
			static void *PvUnittest_Remover(void *);

			// checks the number of visited elements
			static void *PvUnittest_Iterator(void *);

			// checks the number and contents of visited elements
			static void *PvUnittest_IteratorCheck(void *);

			// spawns a number of iterator check tasks
			static void *PvUnittest_IteratorsRun
				(
				IMemoryPool *,
				SElemHashtable &,
				SElem *,
				ULONG
				);

		public:

			// actual unittests
			static GPOS_RESULT EresUnittest();
			static GPOS_RESULT EresUnittest_Basics();
			static GPOS_RESULT EresUnittest_Accessor();
			static GPOS_RESULT EresUnittest_ComplexEquality();
			static GPOS_RESULT EresUnittest_SameKeyIteration();
			static GPOS_RESULT EresUnittest_NonConcurrentIteration();
			static GPOS_RESULT EresUnittest_ConcurrentIteration();
			static GPOS_RESULT EresUnittest_Concurrency();


#ifdef GPOS_DEBUG
			static GPOS_RESULT EresUnittest_AccessorDeadlock();
#endif // GPOS_DEBUG
	};
}

#endif // !GPOS_CSyncHashtableTest_H

// EOF

