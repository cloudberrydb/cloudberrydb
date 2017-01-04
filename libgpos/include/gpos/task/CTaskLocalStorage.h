//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CTaskLocalStorage.h
//
//	@doc:
//		Task-local storage facility; implements TLS to store an instance
//		of a subclass of CTaskLocalStorageObject by an enum index; 
//		no restrictions as to where the actual data is allocated, 


//		e.g. Task's memory pool, global memory etc.
//---------------------------------------------------------------------------
#ifndef GPOS_CTaskLocalStorage_H
#define GPOS_CTaskLocalStorage_H

#include "gpos/base.h"
#include "gpos/common/CSyncHashtable.h"

namespace gpos
{

	// fwd declaration
	class CTaskLocalStorageObject;

	//---------------------------------------------------------------------------
	//	@class:
	//		CTaskLocalStorage
	//
	//	@doc:
	//		TLS implementation; single instance of this class per task; initialized
	//		and destroyed during task setup/tear down
	//
	//---------------------------------------------------------------------------
	class CTaskLocalStorage
	{			
		public:

			enum Etlsidx
			{
				EtlsidxTest,		// unittest slot
				EtlsidxOptCtxt,		// optimizer context
				EtlsidxInvalid,		// used only for hashtable iteration
				
				EtlsidxSentinel
			};

			// ctor
			CTaskLocalStorage() {}
			
			// dtor
			~CTaskLocalStorage();

			// reset
			void Reset(IMemoryPool *pmp);
			
			// accessors
			void Store(CTaskLocalStorageObject *);
			CTaskLocalStorageObject *Ptlsobj(const Etlsidx);
			
			// delete object
			void Remove(CTaskLocalStorageObject *);			

			// equality function -- used for hashtable
			static
			BOOL FEqual
				(
				const CTaskLocalStorage::Etlsidx &etlsidx,
				const CTaskLocalStorage::Etlsidx &etlsidxOther
				)
			{
				return etlsidx == etlsidxOther;
			}

			// hash function
			static
			ULONG UlHashIdx
				(
				const CTaskLocalStorage::Etlsidx &etlsidx
				)
			{
				// keys are unique
				return static_cast<ULONG>(etlsidx);
			}

			// invalid Etlsidx
			static
			const Etlsidx m_etlsidxInvalid;

		private:
		
			// hash table
			CSyncHashtable
				<CTaskLocalStorageObject, 
				Etlsidx,
				CSpinlockOS> m_sht;

			// private copy ctor
			CTaskLocalStorage(const CTaskLocalStorage &);
			
	}; // class CTaskLocalStorage
}

#endif // !GPOS_CTaskLocalStorage_H

// EOF

