//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CFSimulator.h
//
//	@doc:
//		Failpoint simulator framework; computes a hash value for current
//		call stack; if stack has not been seen before, stack repository 
//		returns true which makes the call macro simulate a failure, i.e.
//		throw an exception.
//---------------------------------------------------------------------------
#ifndef GPOS_CFSimulator_H
#define GPOS_CFSimulator_H

#include "gpos/types.h"

#if GPOS_FPSIMULATOR

#include "gpos/common/CBitVector.h"
#include "gpos/common/CList.h"
#include "gpos/common/CStackDescriptor.h"
#include "gpos/common/CSyncHashtable.h"
#include "gpos/common/CSyncHashtableAccessByKey.h"

// macro to trigger failure simulation; must be macro to get accurate 
// file/line information
#define GPOS_SIMULATE_FAILURE(etrace, exma, exmi)	\
		do { \
			if (ITask::PtskSelf()->FTrace(etrace) && \
				CFSimulator::Pfsim()->FNewStack(exma, exmi)) \
			{ \
				GPOS_RAISE(exma, exmi); \
			} \
		} while(0)

// resolution of hash vector
#define GPOS_FSIM_RESOLUTION 10000

	
namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CFSimulator
	//
	//	@doc:
	//		Failpoint simulator; maintains hashtable of stack hashes
	//
	//---------------------------------------------------------------------------
	class CFSimulator
	{	
	
		private:
		
			//---------------------------------------------------------------------------
			//	@class:
			//		CStackTracker
			//
			//	@doc:
			//		Tracks all stacks for a given exception, i.e. contains one single 
			//		bitvector; access to bitvector is protected by spinlock of hashtable
			//		in CFSimulator
			//
			//---------------------------------------------------------------------------
			class CStackTracker
			{
				public:
				
					//---------------------------------------------------------------------------
					//	@struct:
					//		SStackKey
					//
					//	@doc:
					//		Wrapper around the two parts of an exception identification; provides
					//		equality operator for hashtable
					//
					//---------------------------------------------------------------------------
					struct SStackKey
					{
						// stack trackers are identified by the exceptions they manage
						ULONG m_ulMajor;
						ULONG m_ulMinor;
						
						// ctor
						SStackKey
							(
							ULONG ulMajor,
							ULONG ulMinor
							)
							:
							m_ulMajor(ulMajor),
							m_ulMinor(ulMinor)
							{}
						
						// simple comparison
						BOOL operator ==
							(
							const SStackKey &skey
							)
							const
						{
							return m_ulMajor == skey.m_ulMajor && m_ulMinor == skey.m_ulMinor;
						}

						// equality function -- needed for hashtable
						static
						BOOL FEqual
							(
							const SStackKey &skey,
							const SStackKey &skeyOther
							)
						{
							return skey == skeyOther;
						}
						
						// basic hash function
						static
						ULONG UlHash
							(
							const SStackKey &skey
							)
						{
							return skey.m_ulMajor ^ skey.m_ulMinor;
						}

					}; // struct SStackKey
		
								
					// ctor
        			explicit
					CStackTracker(IMemoryPool *pmp, ULONG cResolution, SStackKey skey);
					
					// exchange/set function
					BOOL FExchangeSet(ULONG ulBit);
					
					// link element for hashtable
					SLink m_link;

					// identifier
					SStackKey m_skey;

					// invalid key
					static
					const SStackKey m_skeyInvalid;

				private:
				
					// no copy ctor
					CStackTracker(const CStackTracker &);

					// bitvector to hold stack hashes
					CBitVector *m_pbv;
										
			}; // class CStackTracker
		

		
			// hidden copy ctor
			CFSimulator(const CFSimulator&);

			// memory pool
			IMemoryPool *m_pmp;
			
			// resolution
			ULONG m_cResolution;
			
			// short hands for stack repository and accessor
			typedef CSyncHashtable<CStackTracker, CStackTracker::SStackKey, 
				CSpinlockOS> CStackTable;

			typedef CSyncHashtableAccessByKey<CStackTracker, CStackTracker::SStackKey,
				CSpinlockOS> CStackTableAccessor;
				
			// stack repository
			CStackTable m_st;

			// insert new tracker 
			void AddTracker(CStackTracker::SStackKey skey);

		public:
		
			// ctor
			CFSimulator(IMemoryPool *pmp, ULONG cResolution);

			// dtor
			~CFSimulator() {}

			// determine if stack is new
			BOOL FNewStack(ULONG ulMajor, ULONG ulMinor);

			// global instance
			static
			CFSimulator *m_pfsim;
			
			// initializer for global f-simulator
			static
			GPOS_RESULT EresInit();
			
#ifdef GPOS_DEBUG
			// destroy simulator
			void Shutdown();
#endif // GPOS_DEBUG
			
			// accessor for global instance
			static
			CFSimulator *Pfsim()
			{
				return m_pfsim;
			}

			// check if simulation is activated
			static
			BOOL FSimulation()
			{
				ITask *ptsk = ITask::PtskSelf();
				return
					ptsk->FTrace(EtraceSimulateOOM) ||
					ptsk->FTrace(EtraceSimulateAbort) ||
					ptsk->FTrace(EtraceSimulateIOError) ||
					ptsk->FTrace(EtraceSimulateNetError);
			}

	}; // class CFSimulator
}

#else

#define GPOS_SIMULATE_FAILURE(x,y)	;

#endif // !GPOS_FPSIMULATOR

#endif // !GPOS_CFSimulator_H

// EOF

